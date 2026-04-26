"""End-to-end orchestration: upload bytes → recon + summary + exceptions + Excel out."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field, replace
from datetime import datetime
from pathlib import Path

import pandas as pd

from threshold.config import Mapping
from threshold.exceptions import Exception_, annotate, to_dataframe as exceptions_to_dataframe
from threshold.ingest import SourceData, load_source
from threshold.output import workbook_bytes, write_workbook
from threshold.recon import ReconResult, reconcile
from threshold.summary import SummaryView, build_summary


@dataclass
class Correction:
    """An adjustment applied to a single Internal cell before recon runs.

    The original_amount is the raw source value; corrected_amount is what feeds the recon.
    Corrections are documented (with a comment) and surface in the Corrections sheet of the
    output workbook plus a reference on the affected recon row's Comments column.
    """
    transaction_category: str
    reporting_category: str
    original_amount: float
    corrected_amount: float
    kind: str           # "sign_flip" | "manual"
    comment: str
    applied_at: str = field(default_factory=lambda: datetime.utcnow().isoformat(timespec="seconds") + "Z")


def make_sign_flip(transaction_category: str, reporting_category: str, current_amount: float, comment: str) -> Correction:
    return Correction(
        transaction_category=transaction_category,
        reporting_category=reporting_category,
        original_amount=float(current_amount),
        corrected_amount=-float(current_amount),
        kind="sign_flip",
        comment=comment,
    )


def apply_corrections(internal_df: pd.DataFrame, corrections: list[Correction]) -> pd.DataFrame:
    """Return a copy of internal_df with corrections applied (latest correction per cell wins)."""
    if not corrections:
        return internal_df
    df = internal_df.copy()
    for c in corrections:
        mask = (
            (df["transaction_category"] == c.transaction_category)
            & (df["reporting_category"] == c.reporting_category)
        )
        if mask.any():
            df.loc[mask, "amount"] = c.corrected_amount
    return df


def corrections_to_dicts(corrections: list[Correction]) -> list[dict]:
    return [asdict(c) for c in corrections]


def corrections_from_dicts(data: list[dict] | None) -> list[Correction]:
    if not data:
        return []
    return [Correction(**d) for d in data]


@dataclass
class PipelineResult:
    source: SourceData
    recon: ReconResult
    rows: pd.DataFrame
    exceptions: list[Exception_]
    summary: SummaryView
    corrections: list[Correction] = field(default_factory=list)
    output_path: Path | None = None
    exceptions_csv_path: Path | None = None


def run(
    stripe_source,
    internal_source,
    mapping: Mapping,
    stripe_filename: str | None = None,
    internal_filename: str | None = None,
    corrections: list[Correction] | None = None,
) -> PipelineResult:
    """Run the full pipeline. Each source can be bytes, a path, or a file-like object.

    Filenames are used to detect xlsx vs csv (defaults to xlsx when omitted). Pass
    the same combined xlsx for both sources to use the legacy single-file layout.

    `corrections` is an optional list of cell-level adjustments applied to the Internal
    data BEFORE recon runs (e.g. sign flips on source-data anomalies). Each correction
    carries its own comment and surfaces in the output workbook's Corrections sheet.
    """
    corrections = corrections or []
    src = load_source(
        stripe_source, internal_source, mapping,
        stripe_filename=stripe_filename, internal_filename=internal_filename,
    )
    if corrections:
        src.internal = apply_corrections(src.internal, corrections)
    res = reconcile(src.stripe, src.internal, mapping)
    rows, excs = annotate(res.rows, mapping, src.stripe, res.unmapped_stripe_rcs, res.unmapped_internal_rcs)
    # Annotate the affected recon rows so the user sees the correction reference inline.
    if corrections:
        rows = _stamp_correction_refs(rows, mapping, corrections)
    sv = build_summary(rows, mapping)
    return PipelineResult(source=src, recon=res, rows=rows, exceptions=excs, summary=sv, corrections=corrections)


def _stamp_correction_refs(rows: pd.DataFrame, mapping: Mapping, corrections: list[Correction]) -> pd.DataFrame:
    """Append a 'Correction applied: …' note to the Comments column of each affected row.

    A correction at (cat, rc) affects whichever recon row owns that rc — for subscription_tax
    (sub-categories), the rc is rolled into the parent rc.
    """
    rows = rows.copy()
    for c in corrections:
        # subscription_tax sub-categories (silver/monthly/etc.) collapse into parent rc.
        rule = mapping.rc_rules.get(c.reporting_category)
        target_rc = (
            c.transaction_category if rule is None and c.transaction_category in mapping.rc_rules
            else c.reporting_category
        )
        if target_rc not in mapping.rc_rules:
            continue
        ref = mapping.rc_rules[target_rc].ref
        mask = rows["Ref"] == ref
        if not mask.any():
            continue
        note = (
            f"Correction applied to {c.transaction_category}/{c.reporting_category}: "
            f"{c.original_amount:,.2f} → {c.corrected_amount:,.2f} ({c.kind}). {c.comment}"
        )
        existing = rows.loc[mask, "Comments"].iloc[0] or ""
        rows.loc[mask, "Comments"] = (existing + "\n" if existing else "") + note
    return rows


def write_outputs(
    result: PipelineResult, mapping: Mapping, period: str, version: int, out_dir: Path
) -> tuple[Path, Path]:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    base = f"recon_{period}_v{version}"
    xlsx_path = out_dir / f"{base}.xlsx"
    csv_path = out_dir / f"{base}_exceptions.csv"
    write_workbook(xlsx_path, result.rows, mapping, result.summary, result.exceptions, period)
    exceptions_to_dataframe(result.exceptions).to_csv(csv_path, index=False)
    result.output_path = xlsx_path
    result.exceptions_csv_path = csv_path
    return xlsx_path, csv_path


def apply_overrides(rows: pd.DataFrame, overrides: dict) -> pd.DataFrame:
    """Apply user overrides {ref: {exception_type, comment}} onto a recon rows DataFrame."""
    rows = rows.copy()
    for ref_key, ov in overrides.items():
        ref = float(ref_key)
        mask = rows["Ref"] == ref
        if "exception_type" in ov:
            rows.loc[mask, "Exception Type"] = ov["exception_type"]
        if "comment" in ov:
            rows.loc[mask, "Comments"] = ov["comment"]
    return rows


def apply_overrides_to_exceptions(excs: list[Exception_], overrides: dict) -> list[Exception_]:
    """Return a new exceptions list with user overrides folded into suggested_tag/comment."""
    out = []
    for e in excs:
        ref_key = str(e.ref) if e.ref is not None and e.ref >= 0 else None
        ov = overrides.get(ref_key) if ref_key else None
        if ov:
            out.append(replace(
                e,
                suggested_tag=ov.get("exception_type", e.suggested_tag),
                comment=ov.get("comment", e.comment),
            ))
        else:
            out.append(e)
    return out


def materialise(result: PipelineResult, mapping: Mapping, overrides: dict) -> tuple[pd.DataFrame, list[Exception_], SummaryView]:
    """Apply current overrides to rows + exceptions and rebuild the summary.

    The summary's netting view depends on each row's Exception Type, so it must be
    rebuilt from the override-applied rows for downstream outputs to be consistent.
    """
    rows = apply_overrides(result.rows, overrides)
    excs = apply_overrides_to_exceptions(result.exceptions, overrides)
    summary = build_summary(rows, mapping)
    return rows, excs, summary


def export_workbook_bytes(result: PipelineResult, mapping: Mapping, overrides: dict, period: str) -> bytes:
    rows, excs, summary = materialise(result, mapping, overrides)
    return workbook_bytes(rows, mapping, summary, excs, period, corrections=result.corrections)


def export_exceptions_csv(result: PipelineResult, overrides: dict) -> str:
    excs = apply_overrides_to_exceptions(result.exceptions, overrides)
    return exceptions_to_dataframe(excs).to_csv(index=False)


def compute_triage(rows: pd.DataFrame, mapping: Mapping) -> dict:
    """Headline triage: clean / discrepant counts and $, plus a per-Type breakdown."""
    tied = rows[rows["Tie?"] == "✓"]
    disc = rows[rows["Tie?"] == "✗"]

    by_type = (
        rows.assign(is_tied=lambda df: df["Tie?"] == "✓")
        .groupby("Type", sort=False)
        .agg(
            rows=("Ref", "count"),
            tied_rows=("is_tied", "sum"),
            internal_total=("Internal Total", "sum"),
            stripe_net=("Stripe NET", "sum"),
            net_variance=("Variance", "sum"),
        )
        .reset_index()
    )
    by_type["discrepant_rows"] = by_type["rows"] - by_type["tied_rows"]
    by_type = by_type[
        ["Type", "rows", "tied_rows", "discrepant_rows", "internal_total", "stripe_net", "net_variance"]
    ]
    return {
        "tied_count": int(len(tied)),
        "tied_internal_total": float(tied["Internal Total"].abs().sum()),
        "discrepant_count": int(len(disc)),
        "discrepant_residual": float(disc["Variance"].sum()),
        "discrepant_residual_abs": float(disc["Variance"].abs().sum()),
        "by_type": by_type,
    }


def compute_preflight(result: PipelineResult, mapping: Mapping) -> dict:
    """Hard-fail vs warn checks that should be resolved before relying on the recon."""
    tol = mapping.tie_tolerance
    cat_delta = (result.rows[mapping.cat_columns].sum() - result.recon.cat_validation).abs().max()
    coverage_ok = bool(cat_delta < tol)
    return {
        "coverage_ok": coverage_ok,
        "coverage_delta_max": float(cat_delta),
        "unmapped_stripe": list(result.recon.unmapped_stripe_rcs),
        "unmapped_internal": list(result.recon.unmapped_internal_rcs),
        "has_blocking_issues": not coverage_ok,
        "has_warnings": bool(result.recon.unmapped_stripe_rcs or result.recon.unmapped_internal_rcs),
    }


def compute_exception_groups(result: PipelineResult, mapping: Mapping, overrides: dict | None = None) -> dict:
    """Group non-tied rows into composite / sign-anomaly / reserve / standalone buckets.

    Each group carries the member rows + the live net variance, so the Exceptions UI
    can show users which refs are linked and what their combined residual is.
    """
    rows = apply_overrides(result.rows, overrides or {})
    tol = mapping.tie_tolerance

    composites = []
    grouped_refs: set[float] = set()

    # Composite groups (parent + Internal split components)
    for parent_rc, parent_rule in mapping.rc_rules.items():
        if not parent_rule.composite_components_internal:
            continue
        member_rcs = [parent_rc] + parent_rule.composite_components_internal
        member_rows = rows[rows["reporting_category"].isin(member_rcs)].copy()
        if member_rows.empty:
            continue
        composites.append({
            "kind": "composite",
            "label": f"Composite — {parent_rc}",
            "description": (
                f"Stripe reports the composite NET in '{parent_rc}'; Internal splits it across "
                f"{' + '.join(parent_rule.composite_components_internal)}. "
                f"Members should net to $0 across the bridge."
            ),
            "rows": member_rows,
            "net": float(member_rows["Variance"].sum()),
            "should_net_to_zero": True,
        })
        grouped_refs |= set(member_rows["Ref"])

    # Reserve mechanic pair
    reserves = [r for r in mapping.rc_rules.values() if r.type == "Reserve mechanics"]
    reserve_group = None
    if len(reserves) >= 2:
        member_rows = rows[rows["reporting_category"].isin([r.rc for r in reserves])].copy()
        if not member_rows.empty:
            reserve_group = {
                "kind": "reserve",
                "label": "Reserve mechanic pair",
                "description": "Stripe balance-sheet hold + release; should net to $0 across the pair.",
                "rows": member_rows,
                "net": float(member_rows["Variance"].sum()),
                "should_net_to_zero": True,
            }
            grouped_refs |= set(member_rows["Ref"])

    # Sign anomaly pair (detected dynamically — refs whose Exception Type contains "SIGN ANOMALY")
    anomaly_mask = rows["Exception Type"].fillna("").str.contains("SIGN ANOMALY")
    anomaly_rows = rows[anomaly_mask].copy()
    sign_anomaly_group = None
    if not anomaly_rows.empty:
        sign_anomaly_group = {
            "kind": "sign_anomaly",
            "label": "Sign anomaly pair",
            "description": (
                "Cell-level sign error in Internal source data; mirror appears as structural variance "
                "on the corresponding gross row. Net is the actual residual the SOURCE is wrong by — "
                "it does NOT net to zero until the Internal sign is corrected."
            ),
            "rows": anomaly_rows,
            "net": float(anomaly_rows["Variance"].sum()),
            "should_net_to_zero": False,
        }
        grouped_refs |= set(anomaly_rows["Ref"])

    # Standalone: any non-tied row not already in a group
    standalone_rows = rows[(rows["Tie?"] == "✗") & (~rows["Ref"].isin(grouped_refs))].copy()

    return {
        "composites": composites,
        "reserve": reserve_group,
        "sign_anomaly": sign_anomaly_group,
        "standalone": standalone_rows,
    }


def rewrite_persisted_outputs(
    result: PipelineResult, mapping: Mapping, overrides: dict, period: str,
    xlsx_path: Path | str, csv_path: Path | str,
) -> None:
    """Re-write the persisted xlsx + CSV with current overrides applied.

    Called after the user saves an exception override so the History tab and any
    subsequent download stay in sync with what's shown on screen.
    """
    rows, excs, summary = materialise(result, mapping, overrides)
    write_workbook(xlsx_path, rows, mapping, summary, excs, period, corrections=result.corrections)
    exceptions_to_dataframe(excs).to_csv(csv_path, index=False)
