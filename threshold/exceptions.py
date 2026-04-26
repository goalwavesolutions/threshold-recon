"""Exception detection: classifies each non-tied row with a recommended tag + comment."""
from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from threshold.config import Mapping


@dataclass
class Exception_:
    ref: float
    rc: str
    src: str
    variance: float
    suggested_tag: str
    comment: str
    cause: str        # short machine-readable cause (for the CSV report)
    auto_actionable: bool = False  # True if a JE proposal could fix it
    anomaly_cells: list[tuple[str, float]] = field(default_factory=list)  # [(cat, value), ...]


SIGN_ANOMALY = "SIGN ANOMALY"
COMPOSITE_INTERNAL_SPLIT = "Bundled in Stripe ref {parent_ref} (composite)"
COMPOSITE_STRIPE_PARENT = "Split in Internal across refs {refs}"
NOT_IN_INTERNAL = "Not in Internal"
NOT_IN_STRIPE = "Not in Stripe"
RESERVE_PAIR = "Reserve mechanic (nets off with ref {pair_ref})"
UNMAPPED_STRIPE = "UNMAPPED — Stripe rc has no mapping rule"
UNMAPPED_INTERNAL = "UNMAPPED — Internal rc has no mapping rule"


def _detect_cell_sign_anomalies(
    rows: pd.DataFrame, mapping: Mapping
) -> dict[float, list[tuple[str, float]]]:
    """For each Fee-type row, find per-cat cells whose sign opposes the row's majority.

    Returns {ref: [(cat, anomalous_value), ...]}.
    """
    result: dict[float, list[tuple[str, float]]] = {}
    for _, row in rows.iterrows():
        rule = mapping.rc_rules[row["reporting_category"]]
        if rule.type != "Fees":
            continue
        cells = [(cat, float(row[cat])) for cat in mapping.cat_columns if abs(row[cat]) > mapping.tie_tolerance]
        if len(cells) < 2:
            continue
        pos = [c for c in cells if c[1] > 0]
        neg = [c for c in cells if c[1] < 0]
        # The minority sign is the anomaly (e.g. one positive among many negatives).
        if len(pos) and len(neg):
            minority = pos if len(pos) < len(neg) else neg
            result[row["Ref"]] = minority
    return result


def _detect_structural_fee_mirror(
    rows: pd.DataFrame, stripe: pd.DataFrame, mapping: Mapping
) -> dict[float, str]:
    """For non-fee B-flag rows whose variance ≈ -Stripe[rc].fee, return {ref: rc}.

    These rows reflect Internal recording the rc as gross-only — the variance is
    exactly the Stripe per-rc fee, so they conceptually mirror any sign-anomaly
    on the corresponding Fee row.
    """
    fees = stripe.set_index("reporting_category")["fee"]
    result: dict[float, str] = {}
    for _, row in rows.iterrows():
        rc = row["reporting_category"]
        rule = mapping.rc_rules[rc]
        if rule.type == "Fees" or rule.src != "B":
            continue
        var = float(row["Variance"])
        if abs(var) < mapping.tie_tolerance:
            continue
        stripe_fee = float(fees.get(rc, 0.0))
        if abs(stripe_fee) < mapping.tie_tolerance:
            continue
        if abs(var + stripe_fee) < mapping.tie_tolerance:
            result[row["Ref"]] = rc
    return result


def _ref_for_rc(mapping: Mapping, rc: str) -> float | None:
    rule = mapping.rc_rules.get(rc)
    return rule.ref if rule else None


def annotate(rows: pd.DataFrame, mapping: Mapping, stripe: pd.DataFrame,
             unmapped_stripe: list[str], unmapped_internal: list[str]) -> tuple[pd.DataFrame, list[Exception_]]:
    """Return (rows_with_exception_columns_filled, list_of_Exception_records).

    Mutates a copy of `rows`; original is untouched.
    """
    rows = rows.copy()
    tol = mapping.tie_tolerance
    cell_anoms = _detect_cell_sign_anomalies(rows, mapping)
    structural_mirrors = _detect_structural_fee_mirror(rows, stripe, mapping)
    sign_anomaly_refs = set(cell_anoms.keys()) | set(structural_mirrors.keys())
    excs: list[Exception_] = []

    for idx, row in rows.iterrows():
        rc = row["reporting_category"]
        ref = row["Ref"]
        src = row["Src"]
        var = float(row["Variance"])
        rule = mapping.rc_rules[rc]

        if abs(var) < tol:
            continue  # tied, no exception

        # 1. Sign anomaly — either direct cell-level (Fee row) or structural mirror (gross row)
        if ref in cell_anoms:
            cells = cell_anoms[ref]
            cell_str = ", ".join(f"{c}=${v:,.2f}" for c, v in cells)
            mirror_refs = [str(r) for r in structural_mirrors.keys()]
            mirror_clue = f" (mirror of ref {mirror_refs[0]})" if mirror_refs else ""
            tag = f"{SIGN_ANOMALY}{mirror_clue}"
            comment = (
                f"Cell-level sign anomaly: {cell_str} has sign opposite to row's other cats. "
                f"Likely a sign error in Internal source. When corrected, the row should tie to Stripe NET."
            )
            excs.append(Exception_(ref, rc, src, var, tag, comment, "sign_anomaly_cell",
                                   auto_actionable=True, anomaly_cells=cells))
        elif ref in structural_mirrors:
            cell_refs = [str(r) for r in cell_anoms.keys()]
            mirror_clue = f" (mirror of ref {cell_refs[0]})" if cell_refs else ""
            tag = f"{SIGN_ANOMALY}{mirror_clue}"
            comment = (
                f"Variance = -Stripe per-rc fee for this rc. Internal records this rc as gross-only — "
                f"the fee is netted into Stripe NET (col F = gross + fee). Mirror of the corresponding "
                f"sign anomaly on the Fee row."
            )
            excs.append(Exception_(ref, rc, src, var, tag, comment, "sign_anomaly_structural"))

        # 2. Composite component (Internal split) — src=I with composite_parent_stripe set
        elif rule.composite_parent_stripe:
            parent = rule.composite_parent_stripe
            parent_ref = _ref_for_rc(mapping, parent)
            tag = COMPOSITE_INTERNAL_SPLIT.format(parent_ref=parent_ref)
            siblings = [
                f"{r.ref} {r.rc}" for r in mapping.rc_rules.values()
                if r.composite_parent_stripe == parent and r.rc != rc
            ]
            sib_str = " + ".join(siblings) if siblings else "(none)"
            comment = (
                f"Internal-only split. Component of '{parent}' composite. "
                f"Nets off with ref {parent_ref} (Stripe composite) along with sibling(s): {sib_str}."
            )
            excs.append(Exception_(ref, rc, src, var, tag, comment, "composite_internal_split"))

        # 3. Composite parent (Stripe) — src=S with composite_components_internal set
        elif rule.composite_components_internal:
            comp_refs = [str(_ref_for_rc(mapping, c)) for c in rule.composite_components_internal]
            tag = COMPOSITE_STRIPE_PARENT.format(refs=" + ".join(comp_refs))
            comment = (
                f"Stripe-only composite. Internal splits across "
                f"refs {' + '.join(comp_refs)} ({', '.join(rule.composite_components_internal)}). "
                f"Nets off when summed."
            )
            excs.append(Exception_(ref, rc, src, var, tag, comment, "composite_stripe_parent"))

        # 4. Reserve pair — src=S, type=Reserve mechanics
        elif rule.type == "Reserve mechanics":
            # Find the sibling reserve row
            siblings = [
                r for r in mapping.rc_rules.values()
                if r.type == "Reserve mechanics" and r.rc != rc
            ]
            pair_ref = siblings[0].ref if siblings else None
            tag = RESERVE_PAIR.format(pair_ref=pair_ref) if pair_ref else NOT_IN_INTERNAL
            comment = (
                f"Stripe reserve mechanic (balance-sheet only). "
                f"Nets off with ref {pair_ref}: pair sums to 0."
            )
            excs.append(Exception_(ref, rc, src, var, tag, comment, "reserve_pair"))

        # 5. Internal-only true reconciling (e.g. subscription_tax)
        elif rule.internal_only or (src == "I" and abs(row["Stripe NET"]) < tol):
            tag = NOT_IN_STRIPE
            comment = (
                f"Internal-only line. No Stripe rc counterpart. "
                f"True reconciling item — likely booked separately (e.g. tax payable)."
            )
            excs.append(Exception_(ref, rc, src, var, tag, comment, "internal_only"))

        # 6. Stripe-only true reconciling (e.g. other_adjustment, revenue_share when Internal=0)
        elif src == "S" or (src == "B" and abs(row["Internal Total"]) < tol):
            tag = NOT_IN_INTERNAL
            comment = (
                f"Stripe-only line. No Internal counterpart. "
                f"True reconciling item — either add to Internal extract or accept as Stripe-only."
            )
            excs.append(Exception_(ref, rc, src, var, tag, comment, "stripe_only"))

        # 7. Catch-all — small unexplained variance
        else:
            tag = "Unexplained variance"
            comment = f"Variance ${var:,.2f} not classified by current rules. Investigate."
            excs.append(Exception_(ref, rc, src, var, tag, comment, "unclassified"))

    # Apply tags + comments back onto rows
    by_ref = {e.ref: e for e in excs}
    for idx, row in rows.iterrows():
        e = by_ref.get(row["Ref"])
        if e:
            rows.at[idx, "Exception Type"] = e.suggested_tag
            rows.at[idx, "Comments"] = e.comment

    # Add unmapped exceptions (no row in the recon, but surface them)
    for rc in unmapped_stripe:
        excs.append(Exception_(
            ref=-1, rc=rc, src="S", variance=0.0,
            suggested_tag=UNMAPPED_STRIPE,
            comment=f"Stripe rc '{rc}' has no mapping rule. Add it to config/mapping.yaml or via the UI mapping editor.",
            cause="unmapped_stripe",
        ))
    for rc in unmapped_internal:
        excs.append(Exception_(
            ref=-1, rc=rc, src="I", variance=0.0,
            suggested_tag=UNMAPPED_INTERNAL,
            comment=f"Internal rc '{rc}' has no mapping rule. Add it to config/mapping.yaml or via the UI mapping editor.",
            cause="unmapped_internal",
        ))

    return rows, excs


def to_dataframe(excs: list[Exception_]) -> pd.DataFrame:
    return pd.DataFrame([
        {
            "Ref": e.ref if e.ref >= 0 else None,
            "rc": e.rc,
            "Src": e.src,
            "Variance": e.variance,
            "Cause": e.cause,
            "Suggested Tag": e.suggested_tag,
            "Comment / Suggested Action": e.comment,
        }
        for e in excs
    ])
