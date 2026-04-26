"""Threshold Stripe ↔ Internal Reconciliation — Streamlit front end."""
from __future__ import annotations

import io
import json
from pathlib import Path

import pandas as pd
import streamlit as st

from threshold import manifest, pipeline
from threshold.config import Mapping, RcRule, DEFAULT_CONFIG_PATH, clean_optional_str, clean_str_list
from threshold.exceptions import to_dataframe as exceptions_to_dataframe

st.set_page_config(page_title="Threshold Recon", layout="wide")
st.title("Threshold — Stripe ↔ Internal Reconciliation")

OUT_DIR = Path("data/outputs")


# --- Session state -----------------------------------------------------------
if "mapping" not in st.session_state:
    st.session_state.mapping = Mapping.load()
if "current_result" not in st.session_state:
    st.session_state.current_result = None
if "current_reference" not in st.session_state:
    st.session_state.current_reference = None
if "overrides" not in st.session_state:
    st.session_state.overrides = {}
if "corrections" not in st.session_state:
    st.session_state.corrections = []  # list[pipeline.Correction]
# Sequential flow gate: max step the user has unlocked.
#   0 = Upload & Run only
#   1 = + Recon View (set on persist or load)
#   2 = + Exceptions  (set on Next from Recon View)
#   3 = + Summary     (set on Next from Exceptions)
if "flow_step_idx" not in st.session_state:
    st.session_state.flow_step_idx = 0
if "page" not in st.session_state:
    st.session_state.page = "Upload & Run"


FLOW_PAGES = ["Upload & Run", "Recon View", "Exceptions", "Summary"]


def _go(page_name: str, unlock_to: int | None = None) -> None:
    """Switch to a page, optionally raising the flow gate."""
    if unlock_to is not None:
        st.session_state.flow_step_idx = max(st.session_state.flow_step_idx, unlock_to)
    st.session_state.page = page_name
    st.rerun()


def _fmt_money(v):
    if v is None or pd.isna(v):
        return ""
    return f"{v:,.2f}"


def _style_recon(df: pd.DataFrame, mapping: Mapping) -> pd.io.formats.style.Styler:
    money_cols = mapping.cat_columns + ["Internal Total", "Stripe NET", "Variance"]
    styler = df.style.format({c: _fmt_money for c in money_cols})

    def colour_row(row):
        if "Type" in row.index and row["Type"] == "TOTAL":
            return ["font-weight: bold; border-top: 2px solid #444; background-color: #F4F4F8"] * len(row)
        out = [""] * len(row)
        if "Tie?" in row.index:
            i = list(row.index).index("Tie?")
            out[i] = "background-color: #E6F4EA" if row["Tie?"] == "✓" else "background-color: #FCE8E6"
        return out

    return styler.apply(colour_row, axis=1)


# --- Inline mapping resolver -------------------------------------------------

_TYPE_PREFIX = {
    "Gross activity": 1, "Fees": 2, "Cash movements": 3, "Platform earnings": 4,
    "Disputes (composite)": 5, "Adjustments": 6, "Reserve mechanics": 7, "Tax": 8,
}
_TYPE_OPTIONS = list(_TYPE_PREFIX.keys())


def _suggest_next_ref(mapping: Mapping, type_name: str) -> float:
    """Return next available x.N ref under the chosen Type's prefix."""
    prefix = _TYPE_PREFIX.get(type_name, 9)
    in_type = [r.ref for r in mapping.rc_rules.values() if int(r.ref) == prefix]
    next_int = max([int(round((r % 1) * 10)) for r in in_type], default=0) + 1
    return round(prefix + next_int / 10, 1)


def _inline_map_form(rc: str, default_src: str, mapping: Mapping, key_prefix: str) -> tuple[bool, RcRule | None]:
    """Render a one-line mapping form for an unmapped rc. Returns (clicked, rule)."""
    side_label = "Stripe-side" if default_src == "S" else "Internal-side"
    st.markdown(f"**Map `{rc}` ({side_label})**")
    col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
    with col1:
        sel_type = st.selectbox(
            "Type", _TYPE_OPTIONS, key=f"{key_prefix}_type_{rc}",
            help="Group/category for this rc — drives the Type bucket on the recon.",
        )
    next_ref = _suggest_next_ref(mapping, sel_type)
    with col2:
        ref_val = st.number_input(
            "Ref", value=next_ref, step=0.1, format="%.1f", key=f"{key_prefix}_ref_{rc}",
            help="Reference number under this type's prefix (e.g. 1.7 for the 7th Gross activity row). Auto-suggested.",
        )
    with col3:
        src_idx = {"B": 0, "S": 1, "I": 2}.get(default_src, 1)
        src_val = st.selectbox("Src", ["B", "S", "I"], index=src_idx, key=f"{key_prefix}_src_{rc}",
                                help="B = both sides, S = Stripe-only, I = Internal-only.")
    with col4:
        st.write(""); st.write("")  # vertical spacer for button alignment
        clicked = st.button("Save", key=f"{key_prefix}_save_{rc}", type="primary")
    if clicked:
        rule = RcRule(
            rc=rc, type=sel_type, ref=float(ref_val), src=src_val,
            internal_only=(src_val == "I"),
        )
        return True, rule
    return False, None


def _persist_new_rule(rc: str, rule: RcRule, mapping: Mapping) -> Mapping:
    """Append a new rule and persist mapping.yaml. Returns the updated Mapping."""
    new_rules = dict(mapping.rc_rules)
    new_rules[rc] = rule
    new_mapping = Mapping(
        cat_columns=mapping.cat_columns,
        rollup_categories_excluded=mapping.rollup_categories_excluded,
        rc_rules=new_rules,
        synthetic_fee_rcs=mapping.synthetic_fee_rcs,
        tie_tolerance=mapping.tie_tolerance,
    )
    new_mapping.save(DEFAULT_CONFIG_PATH)
    return new_mapping


def _reset_app_state() -> tuple[int, int]:
    """Wipe persisted runs (input + output files + manifest) and clear session state.

    Returns (n_input_files_deleted, n_output_files_deleted). Mapping config is NOT
    reset — use the Mapping Editor's delete UI for that.
    """
    n_in = n_out = 0
    for d, counter_name in ((Path("data/inputs"), "in"), (Path("data/outputs"), "out")):
        if not d.exists():
            continue
        for f in d.iterdir():
            if f.name == ".gitkeep":
                continue
            try:
                f.unlink()
                if counter_name == "in":
                    n_in += 1
                else:
                    n_out += 1
            except OSError:
                pass
    # Reset session state to first-launch defaults
    st.session_state.current_result = None
    st.session_state.current_reference = None
    st.session_state.overrides = {}
    st.session_state.corrections = []
    st.session_state.flow_step_idx = 0
    st.session_state.page = "Upload & Run"
    return n_in, n_out


# --- Sidebar -----------------------------------------------------------------
with st.sidebar:
    st.markdown("### Recon flow")
    flow_idx = st.session_state.flow_step_idx
    current_page = st.session_state.page
    for i, p in enumerate(FLOW_PAGES):
        locked = i > flow_idx
        if locked:
            label = f"🔒  {i+1}. {p}"
        elif p == current_page:
            label = f"▶  {i+1}. {p}"
        elif i < flow_idx:
            label = f"✓  {i+1}. {p}"
        else:
            label = f"○  {i+1}. {p}"
        if st.button(label, key=f"nav_flow_{p}", disabled=locked, use_container_width=True):
            _go(p)

    st.divider()
    st.markdown("### Always available")
    for util in ("Mapping Editor", "History"):
        marker = "▶ " if current_page == util else ""
        if st.button(f"{marker}{util}", key=f"nav_util_{util}", use_container_width=True):
            _go(util)

    st.divider()
    if st.session_state.current_reference:
        st.success(f"Loaded: **{st.session_state.current_reference}**")
    else:
        st.caption("No recon loaded yet.")

# Active page (set by sidebar buttons; defaults to Upload & Run on first load).
page = st.session_state.page


# --- Upload & Run ------------------------------------------------------------
if page == "Upload & Run":
    st.subheader("1. Upload monthly source files")
    st.caption(
        "Drop the **Stripe** and **Internal** workbooks separately. Each reader looks for "
        "its expected sheet (`Data - Stripe` / `Data - Internal`) and falls back to the "
        "first sheet if absent. The period is detected from the Internal amount column header."
    )

    col_left, col_right = st.columns(2)
    with col_left:
        st.markdown("**Stripe activity**")
        stripe_upload = st.file_uploader(
            "Stripe source (xlsx or csv)",
            type=["xlsx", "csv"],
            accept_multiple_files=False,
            key="stripe_upload",
        )
    with col_right:
        st.markdown("**Internal categorisation**")
        internal_upload = st.file_uploader(
            "Internal source (xlsx or csv)",
            type=["xlsx", "csv"],
            accept_multiple_files=False,
            key="internal_upload",
        )

    notes = st.text_input("Optional notes for this run", "")

    if stripe_upload is not None and internal_upload is not None:
        stripe_bytes = stripe_upload.getvalue()
        internal_bytes = internal_upload.getvalue()
        try:
            result = pipeline.run(
                stripe_bytes, internal_bytes, st.session_state.mapping,
                stripe_filename=stripe_upload.name,
                internal_filename=internal_upload.name,
                corrections=st.session_state.corrections,
            )
        except Exception as exc:
            st.error(f"Failed to read source: {exc}")
            st.stop()

        period = result.source.period_label
        version = manifest.next_version(period)
        st.info(f"Detected period: **{period}** — will save as `{period} v{version}`.")

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Stripe rcs", len(result.source.stripe))
        col2.metric("Internal rows (post-rollup)", len(result.source.internal))
        col3.metric("Rollup rows dropped", result.source.dropped_rollup_rows)
        col4.metric(
            "Grand residual (Internal − Stripe)",
            f"${result.summary.grand_residual:,.2f}",
        )

        if result.recon.unmapped_stripe_rcs or result.recon.unmapped_internal_rcs:
            st.warning(
                f"⚠ Unmapped reporting categories detected — "
                f"{len(result.recon.unmapped_stripe_rcs)} Stripe-side, "
                f"{len(result.recon.unmapped_internal_rcs)} Internal-side. "
                f"Map them inline below to capture their activity in the recon, then re-trigger by "
                f"changing any field on the form."
            )
            for rc in result.recon.unmapped_stripe_rcs:
                with st.container(border=True):
                    clicked, rule = _inline_map_form(rc, "S", st.session_state.mapping, key_prefix="upload_unmapped")
                    if clicked:
                        st.session_state.mapping = _persist_new_rule(rc, rule, st.session_state.mapping)
                        st.success(f"Mapped `{rc}` → ref {rule.ref} ({rule.type}). Re-running recon…")
                        st.rerun()
            for rc in result.recon.unmapped_internal_rcs:
                with st.container(border=True):
                    clicked, rule = _inline_map_form(rc, "I", st.session_state.mapping, key_prefix="upload_unmapped")
                    if clicked:
                        st.session_state.mapping = _persist_new_rule(rc, rule, st.session_state.mapping)
                        st.success(f"Mapped `{rc}` → ref {rule.ref} ({rule.type}). Re-running recon…")
                        st.rerun()

        col_a, col_b = st.columns([1, 1])
        with col_a:
            persist_clicked = st.button("💾 Persist this run", type="primary")
        with col_b:
            if st.session_state.current_reference:
                if st.button("Next: Recon View →", type="primary"):
                    _go("Recon View", unlock_to=1)
            else:
                st.caption("Persist to unlock the next step.")

        if persist_clicked:
            xlsx_path, csv_path = pipeline.write_outputs(
                result, st.session_state.mapping, period, version, OUT_DIR
            )
            entry = manifest.add_entry(
                period=period,
                stripe_filename=stripe_upload.name,
                stripe_bytes=stripe_bytes,
                internal_filename=internal_upload.name,
                internal_bytes=internal_bytes,
                output_path=str(xlsx_path),
                exceptions_csv_path=str(csv_path),
                notes=notes,
                corrections=pipeline.corrections_to_dicts(st.session_state.corrections),
            )
            st.session_state.current_result = result
            st.session_state.current_reference = entry.reference
            st.session_state.overrides = {}
            st.session_state.flow_step_idx = max(st.session_state.flow_step_idx, 1)
            st.success(f"Saved as **{entry.reference}**. Step 2 (Recon View) unlocked.")
            st.rerun()

elif page == "Recon View":
    res = st.session_state.current_result
    if res is None:
        st.info("Upload a source file first.")
    else:
        rows = pipeline.apply_overrides(res.rows, st.session_state.overrides)
        mapping = st.session_state.mapping
        st.subheader(f"Recon — {st.session_state.current_reference}")

        # === PRE-FLIGHT ===
        pre = pipeline.compute_preflight(res, mapping)
        st.markdown("### Pre-flight")

        def _resolve_unmapped(rc: str, side: str):
            """Render inline form + handle save / re-run for one unmapped rc."""
            with st.container(border=True):
                clicked, rule = _inline_map_form(rc, side, mapping, key_prefix="recon_unmapped")
                if not clicked:
                    return
                new_mapping = _persist_new_rule(rc, rule, mapping)
                st.session_state.mapping = new_mapping
                # Re-run pipeline against the persisted source files.
                entry = manifest.get_entry(st.session_state.current_reference)
                if entry and Path(entry.stored_stripe_path).exists() and Path(entry.stored_internal_path).exists():
                    with open(entry.stored_stripe_path, "rb") as sf, open(entry.stored_internal_path, "rb") as inf:
                        new_result = pipeline.run(
                            sf.read(), inf.read(), new_mapping,
                            stripe_filename=entry.stripe_filename,
                            internal_filename=entry.internal_filename,
                            corrections=st.session_state.corrections,
                        )
                    st.session_state.current_result = new_result
                    if entry.output_path and entry.exceptions_csv_path:
                        pipeline.rewrite_persisted_outputs(
                            new_result, new_mapping, st.session_state.overrides,
                            new_result.source.period_label,
                            entry.output_path, entry.exceptions_csv_path,
                        )
                st.success(f"Mapped `{rc}` → ref {rule.ref} ({rule.type}). Recon re-run.")
                st.rerun()

        if pre["has_blocking_issues"]:
            st.error(
                f"✗ **Source coverage failed** — recon's per-category sum differs from raw Internal "
                f"by ${pre['coverage_delta_max']:,.2f}. Internal source data is being dropped — "
                f"most often because an Internal rc isn't mapped. Resolve below to re-run."
            )

        if pre["unmapped_stripe"] or pre["unmapped_internal"]:
            cols = st.columns(2)
            with cols[0]:
                if pre["unmapped_stripe"]:
                    st.warning(f"⚠ {len(pre['unmapped_stripe'])} unmapped Stripe rc(s)")
                    st.caption(", ".join(pre["unmapped_stripe"]))
                else:
                    st.success("✓ All Stripe rcs mapped")
            with cols[1]:
                if pre["unmapped_internal"]:
                    st.warning(f"⚠ {len(pre['unmapped_internal'])} unmapped Internal rc(s)")
                    st.caption(", ".join(pre["unmapped_internal"]))
                else:
                    st.success("✓ All Internal rcs mapped")

            st.markdown("##### Resolve unmapped rcs")
            for rc in pre["unmapped_stripe"]:
                _resolve_unmapped(rc, "S")
            for rc in pre["unmapped_internal"]:
                _resolve_unmapped(rc, "I")
        elif not pre["has_blocking_issues"]:
            st.success("✓ All rcs mapped · ✓ Source coverage ties · Recon is safe to triage.")

        # === TRIAGE ===
        st.markdown("### Triage")
        # Recompute triage against override-applied rows so reclassifications flow through.
        tri = pipeline.compute_triage(rows, mapping)
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("✓ Clean rows", tri["tied_count"])
        col2.metric("✗ Discrepant rows", tri["discrepant_count"])
        col3.metric("Residual ($)", f"${tri['discrepant_residual']:,.2f}",
                    help="Sum of variances on discrepant rows. Net of bridges and true reconciling items.")
        col4.metric("Total Internal Total", f"${rows['Internal Total'].sum():,.2f}")

        st.markdown("##### Breakdown by Type")
        bt = tri["by_type"].rename(columns={
            "rows": "# rows", "tied_rows": "# clean", "discrepant_rows": "# discrepant",
            "internal_total": "Internal Total", "stripe_net": "Stripe NET", "net_variance": "Variance",
        })
        st.dataframe(
            bt.style.format({
                "Internal Total": _fmt_money, "Stripe NET": _fmt_money, "Variance": _fmt_money,
            }),
            hide_index=True, use_container_width=True,
        )
        st.caption("Click into the **Exceptions** tab to work through the discrepant rows one group at a time.")

        # === DETAIL (collapsed) ===
        with st.expander("Per-category source-coverage validation", expanded=False):
            st.caption(
                "Coverage check: recon's per-category sum vs raw Internal sum (post-rollup). "
                "Δ = 0 confirms no Internal source rows were dropped. Independent of the Stripe-vs-Internal residual."
            )
            recon_cat = rows[mapping.cat_columns].sum()
            raw_cat = res.recon.cat_validation
            delta = recon_cat - raw_cat
            val_df = pd.DataFrame({
                "Category": list(recon_cat.index) + ["TOTAL"],
                "Recon sum": list(recon_cat.values) + [recon_cat.sum()],
                "Raw Internal sum": list(raw_cat.values) + [raw_cat.sum()],
                "Δ": list(delta.values) + [delta.sum()],
            })

            def _bold_total(row):
                return ["font-weight: bold; border-top: 2px solid #444"] * len(row) if row["Category"] == "TOTAL" else [""] * len(row)

            st.dataframe(
                val_df.style
                    .format({"Recon sum": _fmt_money, "Raw Internal sum": _fmt_money, "Δ": _fmt_money})
                    .apply(_bold_total, axis=1),
                hide_index=True, use_container_width=True,
            )

        with st.expander("Full recon detail (all rcs × cats)", expanded=False):
            display_cols = (
                ["Type", "reporting_category", "Src", "Ref"]
                + mapping.cat_columns
                + ["Internal Total", "Stripe NET", "Variance", "Tie?", "Exception Type"]
            )
            recon_display = rows[display_cols].copy()
            numeric_cols = mapping.cat_columns + ["Internal Total", "Stripe NET", "Variance"]
            total_row = {c: "" for c in display_cols}
            total_row["Type"] = "TOTAL"
            for c in numeric_cols:
                total_row[c] = recon_display[c].sum()
            total_row["Tie?"] = "✓" if abs(total_row["Variance"]) < mapping.tie_tolerance else "✗"
            recon_display = pd.concat([recon_display, pd.DataFrame([total_row])], ignore_index=True)
            st.dataframe(
                _style_recon(recon_display, mapping),
                hide_index=True, use_container_width=True, height=720,
            )

        # === NEXT ===
        st.divider()
        col_nav1, col_nav2 = st.columns([1, 1])
        with col_nav1:
            if st.button("← Back: Upload & Run"):
                _go("Upload & Run")
        with col_nav2:
            if st.button("Next: Exceptions →", type="primary"):
                _go("Exceptions", unlock_to=2)

elif page == "Exceptions":
    res = st.session_state.current_result
    if res is None:
        st.info("Upload a source file first.")
    else:
        st.subheader(f"Exceptions — {st.session_state.current_reference}")
        st.caption(
            "Discrepant rows are grouped by relationship — composite bridges, sign-anomaly pairs, "
            "reserve pairs, and standalone reconciling items — so you can see the full netting context "
            "before classifying. Each group's net is the live $ residual contributed by that group."
        )
        mapping = st.session_state.mapping

        EXC_OPTIONS_BASE = [
            "Bundled in Stripe (composite)",
            "Split in Internal across refs",
            "SIGN ANOMALY",
            "Not in Internal",
            "Not in Stripe",
            "Reserve mechanic",
            "Unexplained variance — investigate",
        ]

        def _persist_overrides():
            if not st.session_state.current_reference:
                return
            entry = manifest.update_entry(
                st.session_state.current_reference,
                overrides=st.session_state.overrides,
            )
            if entry and entry.output_path and entry.exceptions_csv_path:
                pipeline.rewrite_persisted_outputs(
                    res, mapping, st.session_state.overrides,
                    res.source.period_label, entry.output_path, entry.exceptions_csv_path,
                )

        def _ref_review_block(r, container):
            """Render the per-ref classify form inside the given container."""
            ref = r["Ref"]
            ref_key = str(ref)
            container.markdown(
                f"**Ref {ref}** — `{r['reporting_category']}` ({r['Src']})  ·  "
                f"Internal ${r['Internal Total']:,.2f}  ·  Stripe ${r['Stripe NET']:,.2f}  ·  "
                f"**Variance ${r['Variance']:,.2f}**"
            )
            col_left, col_right = container.columns([1, 2])
            with col_left:
                container.caption(f"Suggested: {r['Exception Type']}")
                options = [r["Exception Type"]] + [o for o in EXC_OPTIONS_BASE if o != r["Exception Type"]] + ["Custom…"]
                selected = col_left.selectbox("Exception Type", options, key=f"sel_{ref_key}")
                if selected == "Custom…":
                    selected = col_left.text_input("Custom Exception Type", key=f"custom_{ref_key}")
            with col_right:
                user_comment = col_right.text_area(
                    "Comment / suggested action",
                    value=r.get("Comments", ""),
                    key=f"com_{ref_key}", height=100,
                )
            if container.button("Save override", key=f"save_{ref_key}"):
                st.session_state.overrides[ref_key] = {
                    "exception_type": selected, "comment": user_comment,
                }
                _persist_overrides()
                container.success(f"Override saved for ref {ref}.")

        def _render_group(group, idx):
            net = group["net"]
            target_zero = group.get("should_net_to_zero", False)
            net_indicator = "✓ nets to zero" if target_zero and abs(net) < mapping.tie_tolerance else ""
            header = (
                f"Group {idx}: {group['label']}  ·  refs {sorted(group['rows']['Ref'].tolist())}  ·  "
                f"net ${net:,.2f}  {net_indicator}"
            )
            with st.expander(header, expanded=True):
                st.caption(group["description"])
                # Member preview table
                preview = group["rows"][[
                    "Ref", "reporting_category", "Src",
                    "Internal Total", "Stripe NET", "Variance", "Exception Type",
                ]].copy()
                st.dataframe(
                    preview.style.format({
                        "Internal Total": _fmt_money, "Stripe NET": _fmt_money, "Variance": _fmt_money,
                    }),
                    hide_index=True, use_container_width=True,
                )

                # SIGN ANOMALY → offer cell-level sign-flip corrections
                if group["kind"] == "sign_anomaly":
                    _render_sign_flip_actions(group, idx)

                # Group-level "accept all suggestions" button
                refs_in_group = group["rows"]["Ref"].tolist()
                if st.button(f"Accept all suggested tags for group {idx}", key=f"accept_grp_{idx}"):
                    for _, r in group["rows"].iterrows():
                        st.session_state.overrides[str(r["Ref"])] = {
                            "exception_type": r["Exception Type"],
                            "comment": r.get("Comments", ""),
                        }
                    _persist_overrides()
                    st.success(f"Accepted suggested tags for refs {refs_in_group}.")
                # Per-ref reclassify
                st.markdown("###### Reclassify individual refs")
                for _, r in group["rows"].iterrows():
                    with st.container(border=True):
                        _ref_review_block(r, st)

        def _rerun_pipeline_with_corrections():
            """Re-run pipeline + rewrite persisted outputs with the latest corrections."""
            entry = manifest.get_entry(st.session_state.current_reference)
            if not entry or not Path(entry.stored_stripe_path).exists():
                return
            with open(entry.stored_stripe_path, "rb") as sf, open(entry.stored_internal_path, "rb") as inf:
                new_result = pipeline.run(
                    sf.read(), inf.read(), mapping,
                    stripe_filename=entry.stripe_filename,
                    internal_filename=entry.internal_filename,
                    corrections=st.session_state.corrections,
                )
            st.session_state.current_result = new_result
            manifest.update_entry(
                st.session_state.current_reference,
                corrections=pipeline.corrections_to_dicts(st.session_state.corrections),
            )
            entry = manifest.get_entry(st.session_state.current_reference)
            if entry and entry.output_path and entry.exceptions_csv_path:
                pipeline.rewrite_persisted_outputs(
                    new_result, mapping, st.session_state.overrides,
                    new_result.source.period_label,
                    entry.output_path, entry.exceptions_csv_path,
                )

        def _render_sign_flip_actions(group, idx):
            """Surface cell-level anomalies in this SIGN ANOMALY group as flip-sign actions."""
            anomaly_cells_by_ref: dict[float, list[tuple[str, float]]] = {
                e.ref: e.anomaly_cells for e in res.exceptions
                if e.cause == "sign_anomaly_cell" and e.anomaly_cells
            }
            already_flipped = {(c.transaction_category, c.reporting_category) for c in st.session_state.corrections}

            actionable_cells: list[tuple[float, str, str, float]] = []
            for _, r in group["rows"].iterrows():
                cells = anomaly_cells_by_ref.get(r["Ref"], [])
                for cat, value in cells:
                    actionable_cells.append((r["Ref"], cat, r["reporting_category"], value))

            if not actionable_cells:
                return

            st.markdown("###### Apply sign-flip correction")
            st.caption(
                "Each row below is a per-cell sign anomaly detected in the Internal source. "
                "Flipping records the correction with your comment; the recon re-runs immediately, "
                "and the correction shows on the Corrections sheet of the final workbook."
            )
            for ref, cat, rc, value in actionable_cells:
                key_id = f"flip_{idx}_{ref}_{cat}_{rc}".replace(".", "_")
                already = (cat, rc) in already_flipped
                with st.container(border=True):
                    cols = st.columns([3, 3, 1])
                    cols[0].markdown(f"**{cat} / {rc}**  ·  current value: **${value:,.2f}**")
                    if already:
                        cols[0].success("✓ Sign flip already applied — see Corrections sheet.")
                        if cols[2].button("Remove correction", key=f"{key_id}_remove"):
                            st.session_state.corrections = [
                                c for c in st.session_state.corrections
                                if (c.transaction_category, c.reporting_category) != (cat, rc)
                            ]
                            _rerun_pipeline_with_corrections()
                            st.rerun()
                        continue
                    default_comment = (
                        f"Sign error in {cat}/{rc} source data — flipped to align with Stripe convention."
                    )
                    user_comment = cols[1].text_area(
                        "Comment (persists to Corrections sheet)",
                        value=default_comment, key=f"{key_id}_comment", height=80,
                    )
                    cols[2].caption(f"will become **${-value:,.2f}**")
                    if cols[2].button("Flip sign", key=f"{key_id}_apply", type="primary"):
                        st.session_state.corrections.append(
                            pipeline.make_sign_flip(cat, rc, value, user_comment)
                        )
                        _rerun_pipeline_with_corrections()
                        st.success(f"Flipped {cat}/{rc} from ${value:,.2f} to ${-value:,.2f}.")
                        st.rerun()

        # Build groups against override-applied rows so accepting reclassifications updates the view.
        groups = pipeline.compute_exception_groups(res, mapping, st.session_state.overrides)

        idx = 0
        for g in groups["composites"]:
            idx += 1
            _render_group(g, idx)
        if groups["sign_anomaly"]:
            idx += 1
            _render_group(groups["sign_anomaly"], idx)
        if groups["reserve"]:
            idx += 1
            _render_group(groups["reserve"], idx)

        # Standalone exceptions
        if not groups["standalone"].empty:
            st.markdown("### Standalone exceptions")
            st.caption("Discrepant rows that aren't part of a known bridge or pair — true reconciling items or unexplained variances.")
            for _, r in groups["standalone"].iterrows():
                ref = r["Ref"]
                with st.expander(
                    f"Ref {ref} — {r['reporting_category']} ({r['Src']})  ·  Variance ${r['Variance']:,.2f}  ·  Suggested: {r['Exception Type']}",
                    expanded=False,
                ):
                    _ref_review_block(r, st)

        st.divider()
        st.markdown("##### All exceptions (with overrides applied)")
        live_excs = pipeline.apply_overrides_to_exceptions(res.exceptions, st.session_state.overrides)
        excs_df = exceptions_to_dataframe(live_excs)
        st.dataframe(excs_df, hide_index=True, use_container_width=True)
        st.download_button(
            "📥 Download exceptions CSV",
            data=excs_df.to_csv(index=False).encode("utf-8"),
            file_name=f"exceptions_{st.session_state.current_reference}.csv",
            mime="text/csv",
        )

        st.divider()
        col_nav1, col_nav2 = st.columns([1, 1])
        with col_nav1:
            if st.button("← Back: Recon View"):
                _go("Recon View")
        with col_nav2:
            if st.button("Next: Summary →", type="primary"):
                _go("Summary", unlock_to=3)

elif page == "Summary":
    res = st.session_state.current_result
    if res is None:
        st.info("Upload a source file first.")
    else:
        # Rebuild the summary against override-applied rows so the netting view
        # buckets reflect any user re-classifications.
        _, _, sv = pipeline.materialise(res, st.session_state.mapping, st.session_state.overrides)
        st.subheader(f"Recon Summary — {st.session_state.current_reference}")

        st.markdown("##### High-level summary by source flag")
        st.dataframe(
            sv.bsi_summary.style.format({
                "Internal Total": _fmt_money,
                "Stripe NET Total": _fmt_money,
                "Net Variance": _fmt_money,
            }),
            hide_index=True, use_container_width=True,
        )

        st.markdown("##### Composite bridges")
        for bridge in sv.composite_bridges:
            st.dataframe(
                bridge.style.format({"Amount": _fmt_money}),
                hide_index=True, use_container_width=True,
            )

        if sv.reserve_bridge is not None:
            st.markdown("##### Reserve mechanic pair")
            st.dataframe(
                sv.reserve_bridge.style.format({"Amount": _fmt_money}),
                hide_index=True, use_container_width=True,
            )

        st.markdown("##### Netting view (Internal − Stripe NET, signed)")
        st.dataframe(
            sv.netting_view.style.format({"Internal − Stripe": _fmt_money}),
            hide_index=True, use_container_width=True,
        )
        col1, col2, col3 = st.columns(3)
        sum_check = sv.netting_view["Internal − Stripe"].sum()
        col1.metric("Netting view SUM", f"${sum_check:,.2f}")
        col2.metric("Grand residual", f"${sv.grand_residual:,.2f}")
        col3.metric("Δ (cross-check)", f"${sum_check - sv.grand_residual:,.2f}")

        # === CORRECTIONS APPLIED ===
        if st.session_state.corrections:
            st.markdown("##### Source-data corrections applied")
            st.caption(
                "These are cell-level adjustments to the Internal source (e.g. sign flips on known "
                "source bugs) that were applied before reconciliation ran. Each is recorded on the "
                "Corrections sheet of the downloaded workbook."
            )
            corr_df = pd.DataFrame([
                {
                    "Cat": c.transaction_category,
                    "Rc": c.reporting_category,
                    "Kind": c.kind,
                    "Original": c.original_amount,
                    "Corrected": c.corrected_amount,
                    "Δ": c.corrected_amount - c.original_amount,
                    "Comment": c.comment,
                    "Applied at": c.applied_at,
                }
                for c in st.session_state.corrections
            ])
            st.dataframe(
                corr_df.style.format({
                    "Original": _fmt_money, "Corrected": _fmt_money, "Δ": _fmt_money,
                }),
                hide_index=True, use_container_width=True,
            )

        # === FINAL DOWNLOAD ===
        st.divider()
        st.markdown("### Final artifact")
        st.caption(
            "Download the reconciled workbook for documentation and journal-entry preparation. "
            "The xlsx includes the recon detail, summary, composite bridges, netting view, and "
            "the exceptions sheet — all with current overrides applied."
        )
        ref = st.session_state.current_reference
        entry = manifest.get_entry(ref) if ref else None
        if entry:
            xlsx_bytes = pipeline.export_workbook_bytes(
                res, st.session_state.mapping, st.session_state.overrides, res.source.period_label,
            )
            filename = Path(entry.output_path).name if entry.output_path else f"recon_{ref.replace(' ', '_')}.xlsx"
            st.download_button(
                "📥 Download final reconciliation workbook",
                data=xlsx_bytes,
                file_name=filename,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary",
            )

        st.divider()
        if st.button("← Back: Exceptions"):
            _go("Exceptions")

elif page == "Mapping Editor":
    st.subheader("Mapping editor")
    st.caption(
        "Edit categorisation rules below. Changes save to `config/mapping.yaml` and apply to "
        "the next recon run. Use this to map any unmapped Stripe or Internal rcs."
    )
    m = st.session_state.mapping

    st.markdown("##### Reporting category rules")
    rules_df = pd.DataFrame([
        {
            "rc": rc, "type": r.type, "ref": r.ref, "src": r.src,
            "composite_parent_stripe": r.composite_parent_stripe or "",
            "composite_components_internal": ",".join(r.composite_components_internal),
            "internal_only": r.internal_only,
            "values_are_subcategories": r.values_are_subcategories,
            "description": r.description,
        }
        for rc, r in sorted(m.rc_rules.items(), key=lambda kv: kv[1].ref)
    ])
    edited = st.data_editor(rules_df, hide_index=True, num_rows="dynamic", use_container_width=True)
    st.caption(
        "To **add** a rule, scroll to the empty row at the bottom and fill in the cells. "
        "To **edit**, change cell values in place. To **delete**, use the panel below "
        "(deletion via the row selector in the table doesn't always persist on save)."
    )

    # --- Delete a rule -------------------------------------------------------
    st.markdown("##### Delete a rule")
    deletable_rcs = sorted(m.rc_rules.keys())
    col_del_left, col_del_right = st.columns([3, 1])
    with col_del_left:
        to_delete = st.multiselect(
            "Select rcs to remove from the mapping",
            options=deletable_rcs,
            key="rules_to_delete",
            help="Removes the rule from config/mapping.yaml. The rc will then surface as 'unmapped' on the next recon if it appears in either source.",
        )
    with col_del_right:
        st.write("")  # vertical alignment spacer
        st.write("")
        delete_clicked = st.button("🗑 Delete selected", disabled=not to_delete)

    if delete_clicked and to_delete:
        # Also clear any composite refs that pointed at the deleted rcs (defence-in-depth).
        deleted = set(to_delete)
        kept_rules = {rc: r for rc, r in m.rc_rules.items() if rc not in deleted}
        for r in kept_rules.values():
            if r.composite_parent_stripe in deleted:
                r.composite_parent_stripe = None
            if r.composite_components_internal:
                r.composite_components_internal = [
                    c for c in r.composite_components_internal if c not in deleted
                ]
        new_mapping = Mapping(
            cat_columns=m.cat_columns,
            rollup_categories_excluded=m.rollup_categories_excluded,
            rc_rules=kept_rules,
            synthetic_fee_rcs=m.synthetic_fee_rcs,
            tie_tolerance=m.tie_tolerance,
        )
        new_mapping.save(DEFAULT_CONFIG_PATH)
        st.session_state.mapping = new_mapping
        st.session_state.rules_to_delete = []
        st.success(f"Deleted {len(deleted)} rule(s): {', '.join(sorted(deleted))}. {len(kept_rules)} remain.")
        st.rerun()

    # --- Cat columns + rollups ----------------------------------------------
    st.markdown("##### Cat columns (display order)")
    cat_text = st.text_area("One cat per line", value="\n".join(m.cat_columns), height=200)

    st.markdown("##### Rollup categories to exclude")
    rollup_text = st.text_area("One per line", value="\n".join(m.rollup_categories_excluded), height=120)

    if st.button("💾 Save mapping changes", type="primary"):
        new_rules = {}
        for _, r in edited.iterrows():
            rc_name = clean_optional_str(r["rc"])
            if not rc_name:
                continue
            new_rules[rc_name] = RcRule(
                rc=rc_name,
                type=clean_optional_str(r["type"]) or "",
                ref=float(r["ref"]),
                src=clean_optional_str(r["src"]) or "",
                composite_parent_stripe=clean_optional_str(r["composite_parent_stripe"]),
                composite_components_internal=clean_str_list(r["composite_components_internal"]),
                internal_only=bool(r["internal_only"]),
                values_are_subcategories=bool(r["values_are_subcategories"]),
                description=clean_optional_str(r["description"]) or "",
            )
        new_mapping = Mapping(
            cat_columns=[c.strip() for c in cat_text.splitlines() if c.strip()],
            rollup_categories_excluded=[c.strip() for c in rollup_text.splitlines() if c.strip()],
            rc_rules=new_rules,
            synthetic_fee_rcs=m.synthetic_fee_rcs,
            tie_tolerance=m.tie_tolerance,
        )
        new_mapping.save(DEFAULT_CONFIG_PATH)
        st.session_state.mapping = new_mapping
        st.success(f"Saved {len(new_rules)} rules to {DEFAULT_CONFIG_PATH}.")

elif page == "History":
    st.subheader("Run history")

    # --- Reset / rebaseline ---------------------------------------------------
    with st.expander("⚠ Reset — clear all history (rebaseline for a clean demo)", expanded=False):
        st.caption(
            "Deletes every persisted run (input files, output xlsx + CSV, manifest) and "
            "clears the current session state. Mapping config is NOT reset — use the "
            "Mapping Editor's delete panel for that. Use this before a clean demo or "
            "to hand off a fresh state to another user."
        )
        confirm = st.text_input("Type **RESET** to confirm (case-sensitive)", key="reset_confirm")
        if st.button("🗑 Clear all history", type="primary", disabled=(confirm != "RESET")):
            n_in, n_out = _reset_app_state()
            st.success(f"Cleared {n_in} input file(s) and {n_out} output file(s). Session reset.")
            st.rerun()

    st.divider()
    entries = manifest.list_entries()
    if not entries:
        st.info("No runs yet. Upload a workbook from the Upload page.")
    else:
        for e in sorted(entries, key=lambda x: (x.period, x.version), reverse=True):
            with st.container(border=True):
                col1, col2, col3 = st.columns([2, 4, 2])
                col1.markdown(f"**{e.reference}**")
                col1.caption(e.uploaded_at)
                col2.write(f"Stripe: `{e.stripe_filename}`")
                col2.write(f"Internal: `{e.internal_filename}`")
                if e.notes:
                    col2.caption(f"Notes: {e.notes}")
                if e.output_path and Path(e.output_path).exists():
                    with open(e.output_path, "rb") as f:
                        col3.download_button(
                            "📥 xlsx",
                            data=f.read(),
                            file_name=Path(e.output_path).name,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key=f"dl_{e.reference}",
                        )
                if e.exceptions_csv_path and Path(e.exceptions_csv_path).exists():
                    with open(e.exceptions_csv_path) as f:
                        col3.download_button(
                            "📥 exceptions CSV",
                            data=f.read(),
                            file_name=Path(e.exceptions_csv_path).name,
                            mime="text/csv",
                            key=f"dlcsv_{e.reference}",
                        )
                if st.button(f"Load {e.reference}", key=f"load_{e.reference}"):
                    loaded_corrections = pipeline.corrections_from_dicts(getattr(e, "corrections", []) or [])
                    with open(e.stored_stripe_path, "rb") as sf, open(e.stored_internal_path, "rb") as inf:
                        result = pipeline.run(
                            sf.read(), inf.read(), st.session_state.mapping,
                            stripe_filename=e.stripe_filename,
                            internal_filename=e.internal_filename,
                            corrections=loaded_corrections,
                        )
                    st.session_state.current_result = result
                    st.session_state.current_reference = e.reference
                    st.session_state.overrides = e.overrides or {}
                    st.session_state.corrections = loaded_corrections
                    # Loading a saved entry unlocks the full flow — the user has previously
                    # walked through it for this dataset.
                    _go("Recon View", unlock_to=3)
