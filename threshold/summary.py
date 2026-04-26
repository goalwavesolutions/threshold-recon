"""Build the Recon Summary view (netting bridges + B/S/I summary + signed netting view)."""
from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from threshold.config import Mapping


@dataclass
class SummaryView:
    bsi_summary: pd.DataFrame              # Source/Internal Total/Stripe NET/Variance/Notes
    composite_bridges: list[pd.DataFrame]  # one per composite (5.1, 5.2, …)
    reserve_bridge: pd.DataFrame | None    # 7.1+7.2 = 0
    netting_view: pd.DataFrame             # signed Internal − Stripe lines + residual
    grand_residual: float                  # cross-check value


def _bsi_summary(rows: pd.DataFrame, mapping: Mapping) -> pd.DataFrame:
    out = []
    for src, label in [
        ("B", "Both Stripe & Internal"),
        ("S", "Stripe only — composites + true Stripe-only"),
        ("I", "Internal only — splits + true Internal-only"),
    ]:
        sub = rows[rows["Src"] == src]
        refs = ", ".join(str(r) for r in sub["Ref"].tolist())
        out.append({
            "Source": src,
            "Description": f"{label} (refs {refs})",
            "Internal Total": sub["Internal Total"].sum(),
            "Stripe NET Total": sub["Stripe NET"].sum(),
            "Net Variance": sub["Variance"].sum(),
        })
    out.append({
        "Source": "TOTAL",
        "Description": "",
        "Internal Total": rows["Internal Total"].sum(),
        "Stripe NET Total": rows["Stripe NET"].sum(),
        "Net Variance": rows["Variance"].sum(),
    })
    return pd.DataFrame(out)


def _composite_bridges(rows: pd.DataFrame, mapping: Mapping) -> list[pd.DataFrame]:
    bridges = []
    for parent_rc, parent_rule in mapping.rc_rules.items():
        if not parent_rule.composite_components_internal:
            continue
        comp_refs = parent_rule.composite_components_internal
        bridge_rows = []
        comp_sum = 0.0
        for c in comp_refs:
            c_rule = mapping.rc_rules[c]
            c_row = rows[rows["reporting_category"] == c].iloc[0]
            bridge_rows.append({
                "Ref": c_rule.ref, "rc": c, "Component": "Internal split",
                "Amount": c_row["Internal Total"],
            })
            comp_sum += c_row["Internal Total"]
        bridge_rows.append({"Ref": "Sum", "rc": "", "Component": " + ".join(str(mapping.rc_rules[c].ref) for c in comp_refs) + " (Internal)",
                             "Amount": comp_sum})
        parent_row = rows[rows["reporting_category"] == parent_rc].iloc[0]
        bridge_rows.append({"Ref": parent_rule.ref, "rc": parent_rc, "Component": "Stripe NET (composite)",
                             "Amount": parent_row["Stripe NET"]})
        bridge_rows.append({"Ref": "Δ", "rc": "", "Component": "Sum − Stripe NET",
                             "Amount": comp_sum - parent_row["Stripe NET"]})
        bridges.append(pd.DataFrame(bridge_rows))
    return bridges


def _reserve_bridge(rows: pd.DataFrame, mapping: Mapping) -> pd.DataFrame | None:
    reserves = [r for r in mapping.rc_rules.values() if r.type == "Reserve mechanics"]
    if len(reserves) < 2:
        return None
    out = []
    total = 0.0
    for r in reserves:
        row = rows[rows["reporting_category"] == r.rc].iloc[0]
        out.append({"Ref": r.ref, "rc": r.rc, "Component": "Stripe", "Amount": row["Stripe NET"]})
        total += row["Stripe NET"]
    out.append({"Ref": "Sum", "rc": "", "Component": " + ".join(str(r.ref) for r in reserves), "Amount": total})
    return pd.DataFrame(out)


def _netting_view(rows: pd.DataFrame, mapping: Mapping, exception_types: dict[float, str]) -> pd.DataFrame:
    """Build the signed Internal − Stripe view. Sum must equal grand residual."""
    tol = mapping.tie_tolerance
    items = []

    # Bucket 1: tied B-rows (no exception, variance ≈ 0). Show their net contribution.
    b_rows = rows[rows["Src"] == "B"].copy()
    sign_anomaly_refs = [r for r, t in exception_types.items() if t.startswith("SIGN ANOMALY")]
    not_in_int_refs = [r for r, t in exception_types.items() if t == "Not in Internal"]

    tied_b = b_rows[b_rows["Variance"].abs() < tol]
    if len(tied_b):
        items.append({
            "Item": "Tied B-flag rows",
            "Refs": ", ".join(str(r) for r in tied_b["Ref"].tolist()),
            "Sign": "+",
            "Internal − Stripe": tied_b["Variance"].sum(),
            "Notes": "All within tolerance.",
        })

    # Bucket 2: each sign anomaly row, individually
    for ref in sign_anomaly_refs:
        r = rows[rows["Ref"] == ref].iloc[0]
        items.append({
            "Item": f"SIGN ANOMALY — {r['reporting_category']}",
            "Refs": ref,
            "Sign": "+",
            "Internal − Stripe": float(r["Variance"]),
            "Notes": exception_types[ref],
        })

    # Bucket 3: composite netting (parent + components combined → 0 by construction)
    for parent_rc, parent_rule in mapping.rc_rules.items():
        if not parent_rule.composite_components_internal:
            continue
        comp_refs = parent_rule.composite_components_internal
        sub = rows[rows["reporting_category"].isin([parent_rc] + comp_refs)]
        net_var = sub["Variance"].sum()
        items.append({
            "Item": f"Composite netting — {parent_rc}",
            "Refs": " + ".join(str(mapping.rc_rules[c].ref) for c in comp_refs) + f" vs {parent_rule.ref}",
            "Sign": "+",
            "Internal − Stripe": net_var,
            "Notes": "Should be 0 — Internal splits sum to Stripe composite NET.",
        })

    # Bucket 4: reserve mechanic netting
    reserves = [r for r in mapping.rc_rules.values() if r.type == "Reserve mechanics"]
    if reserves:
        sub = rows[rows["reporting_category"].isin([r.rc for r in reserves])]
        items.append({
            "Item": "Reserve mechanic netting",
            "Refs": " + ".join(str(r.ref) for r in reserves),
            "Sign": "+",
            "Internal − Stripe": sub["Variance"].sum(),
            "Notes": "Reserves net to 0 across the pair.",
        })

    # Bucket 5: each Stripe-only true reconciling item
    for ref in not_in_int_refs:
        r = rows[rows["Ref"] == ref].iloc[0]
        items.append({
            "Item": f"Stripe-only — {r['reporting_category']}",
            "Refs": ref,
            "Sign": "+",
            "Internal − Stripe": float(r["Variance"]),
            "Notes": "True Stripe-only line; not in Internal extract.",
        })

    # Bucket 6: each Internal-only true reconciling item
    for ref, t in exception_types.items():
        if t == "Not in Stripe":
            r = rows[rows["Ref"] == ref].iloc[0]
            items.append({
                "Item": f"Internal-only — {r['reporting_category']}",
                "Refs": ref,
                "Sign": "+",
                "Internal − Stripe": float(r["Variance"]),
                "Notes": "True Internal-only line; not in Stripe activity.",
            })

    df = pd.DataFrame(items)
    return df


def build_summary(rows: pd.DataFrame, mapping: Mapping) -> SummaryView:
    exception_types = {row["Ref"]: row["Exception Type"] for _, row in rows.iterrows() if row["Exception Type"]}
    bsi = _bsi_summary(rows, mapping)
    composites = _composite_bridges(rows, mapping)
    reserve = _reserve_bridge(rows, mapping)
    netting = _netting_view(rows, mapping, exception_types)
    grand = float(rows["Variance"].sum())
    return SummaryView(
        bsi_summary=bsi,
        composite_bridges=composites,
        reserve_bridge=reserve,
        netting_view=netting,
        grand_residual=grand,
    )
