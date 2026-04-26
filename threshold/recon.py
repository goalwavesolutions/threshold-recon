"""Core reconciliation engine — joins Stripe NET against pivoted Internal data."""
from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from threshold.config import Mapping, RcRule


@dataclass
class ReconResult:
    rows: pd.DataFrame              # the Recon - Stripe vs Internal (B) sheet
    cat_validation: pd.Series       # per-cat sum from raw Internal (drives the validation row)
    unmapped_stripe_rcs: list[str]
    unmapped_internal_rcs: list[str]


def _build_internal_pivot(internal: pd.DataFrame, mapping: Mapping) -> pd.DataFrame:
    """Pivot Internal rows into rc × cat. Collapses subcategory rcs into their parent."""
    df = internal.copy()
    # Collapse subcategory rcs (e.g. silver/monthly/diamond → subscription_tax)
    sub_rcs = {
        rule.rc: cat
        for rule in mapping.rc_rules.values()
        if rule.values_are_subcategories
        for cat in [rule.rc]
    }
    if sub_rcs:
        # For each rule with values_are_subcategories, the matching transaction_category
        # uses the same name; relabel the rc to the parent rc name.
        for parent_rc in sub_rcs:
            mask = df["transaction_category"] == parent_rc
            df.loc[mask, "reporting_category"] = parent_rc

    pivot = (
        df.groupby(["reporting_category", "transaction_category"])["amount"]
        .sum()
        .unstack(fill_value=0.0)
    )
    # Ensure all configured cat columns exist.
    for cat in mapping.cat_columns:
        if cat not in pivot.columns:
            pivot[cat] = 0.0
    pivot = pivot[mapping.cat_columns]
    return pivot


def _stripe_lookup(stripe: pd.DataFrame) -> tuple[pd.Series, pd.Series, pd.Series]:
    """Return (gross, fee, net) Series indexed by reporting_category."""
    s = stripe.set_index("reporting_category")
    return s["gross"], s["fee"], s["net"]


def _value_for_rc(
    rc: str,
    rule: RcRule,
    pivot: pd.DataFrame,
    stripe_gross: pd.Series,
    stripe_fee: pd.Series,
    stripe_net: pd.Series,
    mapping: Mapping,
) -> tuple[dict[str, float], float, float]:
    """Return (cat_values dict, internal_total, stripe_net) for one rc row."""
    # Per-cat Internal values
    if rc in pivot.index:
        cat_values = {cat: float(pivot.loc[rc].get(cat, 0.0)) for cat in mapping.cat_columns}
    else:
        cat_values = {cat: 0.0 for cat in mapping.cat_columns}
    internal_total = sum(cat_values.values())

    # Stripe NET to compare to
    if rule.internal_only or rule.composite_parent_stripe:
        # Internal-only rcs (e.g. subscription_tax) and composite component rcs
        # (e.g. dispute_fee → bundled in Stripe `dispute`) have no direct Stripe NET.
        stripe_value = 0.0
    else:
        stripe_value = float(stripe_net.get(rc, 0.0))

    return cat_values, internal_total, stripe_value


def reconcile(stripe: pd.DataFrame, internal: pd.DataFrame, mapping: Mapping) -> ReconResult:
    pivot = _build_internal_pivot(internal, mapping)
    stripe_gross, stripe_fee, stripe_net = _stripe_lookup(stripe)

    rows = []
    for rc, rule in sorted(mapping.rc_rules.items(), key=lambda kv: kv[1].ref):
        cat_values, internal_total, stripe_value = _value_for_rc(
            rc, rule, pivot, stripe_gross, stripe_fee, stripe_net, mapping
        )
        variance = internal_total - stripe_value
        tied = abs(variance) < mapping.tie_tolerance
        row = {
            "Type": rule.type,
            "reporting_category": rc,
            "Src": rule.src,
            "Ref": rule.ref,
            "Description": rule.description,
            **cat_values,
            "Internal Total": internal_total,
            "Stripe NET": stripe_value,
            "Variance": variance,
            "Tie?": "✓" if tied else "✗",
            "Exception Type": "",
            "Comments": "",
        }
        rows.append(row)

    rows_df = pd.DataFrame(rows)

    # Per-cat validation: sum of ALL raw Internal amounts (post-rollup-exclusion)
    # grouped by transaction_category. The recon Internal Total per cat must equal this.
    cat_validation = internal.groupby("transaction_category")["amount"].sum()
    cat_validation = cat_validation.reindex(mapping.cat_columns, fill_value=0.0)

    # Unmapped detection
    mapped_rcs = set(mapping.rc_rules.keys())
    stripe_rcs = set(stripe["reporting_category"].astype(str))
    # Internal rc set after subcategory collapse
    pivot_rcs = set(pivot.index.astype(str))
    unmapped_stripe = sorted(stripe_rcs - mapped_rcs)
    unmapped_internal = sorted(pivot_rcs - mapped_rcs)

    return ReconResult(
        rows=rows_df,
        cat_validation=cat_validation,
        unmapped_stripe_rcs=unmapped_stripe,
        unmapped_internal_rcs=unmapped_internal,
    )
