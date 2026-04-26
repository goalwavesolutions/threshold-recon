# Threshold — Stripe ↔ Internal Reconciliation

A controller-grade monthly reconciliation tool that compares Stripe payment activity
(by `reporting_category`) against Threshold's own internal categorisation, automatically
flags exceptions, and produces a downloadable Excel package suitable for documentation
and journal-entry preparation.

Replaces a two-day manual Excel process with a repeatable pipeline that takes seconds
per month and surfaces every reconciling item with a recommended classification.

## Quickstart

```bash
python3 -m pip install -r requirements.txt
python3 -m streamlit run app.py
```

In the **Upload & Run** tab, drop the **Stripe** file in the left uploader and the
**Internal** file in the right uploader. Both **xlsx and csv** are accepted. For xlsx
the named sheet (`Data - Stripe` / `Data - Internal`) is preferred but the first
sheet is used as a fallback. For both formats the header row is auto-detected by
scanning for expected token names, so files with title rows above the header (the
original Excel layout) and bare CSV exports both work. The app detects the period,
runs the recon, persists both inputs as `<YYYY-MM> v<N>`, and shows the recon view,
exceptions, and summary.

For testing or replay convenience you can also pass the same combined workbook for
both arguments at the API level — `pipeline.run(combined_bytes, combined_bytes, mapping)`
— and each reader will pick its own sheet.

## What the pipeline does

```
   source.xlsx                                 versioned outputs
       │                                       (data/outputs/)
       ▼                                              ▲
  ingest.py    ──►  recon.py    ──►  exceptions.py    │
  (drop rollups)    (pivot, join)    (classify)       │
                                          │           │
                                          ▼           │
                                     summary.py  ──► output.py
                                     (B/S/I, netting, bridges)
```

**Key business logic encoded** (see `config/mapping.yaml`):
- Composite mappings: Stripe `dispute` (5.1) = Internal `dispute_gross` (1.4) + `dispute_fee` (2.2); Stripe `dispute_reversal` (5.2) = Internal `dispute_reversal_gross` (1.5) + `dispute_reversal_fee` (2.3).
- Per-rc Stripe fees (col E of `charge`/`fee` rows) are absorbed into the parent rc's NET — they appear as variance on the gross row, not as separate rows.
- `subscription_tax` rc values in Internal are tier/cycle names (`silver`, `monthly`, etc.); the engine collapses them into a single `subscription_tax` row.
- Stripe always records fees as negative; Internal sometimes records them positive (sign anomaly). The engine flags these via the `Exception Type` column, never auto-corrects.

## Reconciliation outputs

Each persisted run writes to `data/outputs/recon_<period>_v<N>.xlsx` with three sheets:

1. **Recon - Stripe vs Internal** — one row per `reporting_category`, grouped by Type, with per-cat Internal columns + Internal Total, Stripe NET, Variance, Tie?, Exception Type, Comments.
2. **Recon Summary** — B/S/I roll-up, composite bridges (e.g. 1.4 + 2.2 vs 5.1), reserve mechanic pair, and the signed netting view (`Internal − Stripe NET`) whose SUM equals the grand residual exactly.
3. **Exceptions** — every non-tied row with cause, suggested tag, and suggested action.

A separate `recon_<period>_v<N>_exceptions.csv` is also written for downstream tooling.

## Exception types

Auto-classified by `threshold/exceptions.py`:

| Tag | When |
|---|---|
| `SIGN ANOMALY (mirror of ref X.X)` | Cell-level sign anomaly in a Fee row, or a non-fee row whose variance equals −Stripe[rc].fee |
| `Bundled in Stripe ref X.X (composite)` | Internal-only split of a Stripe composite (e.g. dispute_fee → dispute) |
| `Split in Internal across refs X.X + Y.Y` | Stripe composite parent (e.g. dispute → dispute_gross + dispute_fee) |
| `Reserve mechanic (nets off with ref X.X)` | Stripe reserve hold/release pair |
| `Not in Internal` | Stripe-only line, no Internal counterpart |
| `Not in Stripe` | Internal-only line (e.g. subscription_tax) |
| `UNMAPPED — …` | rc has no mapping rule — surface for the user to map and re-run |

The Streamlit **Exceptions** tab lets the user agree with the suggested tag or override
it with a custom one. Overrides persist on the manifest entry and flow into the Excel output.

## Inputs (assumed shape)

The two source files are uploaded separately. Each reader looks for the named sheet
first, then falls back to the workbook's first sheet.

**Stripe file** — sheet `Data - Stripe` (or first sheet):
- Header row 3, with columns `reporting_category`, `currency`, `count`, `gross`, `fee`, `net`.
- One row per Stripe `reporting_category`. A `total` footer row is auto-dropped.

**Internal file** — sheet `Data - Internal` (or first sheet):
- Header row 1, with columns `transaction_category`, `reporting_category`, concat key, amount.
- The amount column header is the period date (auto-detected as `YYYY-MM`).
- Rollup rows under `total_subscription`, `subscription_cycle_monthly`, and `subscription_cycle_yearly` are auto-excluded to avoid double-counting.

## Mapping config

`config/mapping.yaml` is the source of truth for:
- Which rcs exist and how they're grouped (Type, Ref, Src=B/S/I).
- Composite parent ↔ component relationships.
- Internal-only categories (e.g. `subscription_tax`).
- Cat column display order.
- Rollup categories to drop from Internal.
- The tie tolerance (default $1.00).

Edit it directly, or use the **Mapping Editor** tab in the app (data-editor view, saves
back to the YAML on click). New rcs discovered at runtime show up as `UNMAPPED` exceptions
and can be mapped in-place from the editor.

## Resetting / rebaselining

To clear all persisted runs and start fresh — useful before a clean demo or when
handing off to a new user:

- **In-app**: open the **History** tab → expand the "⚠ Reset" panel → type `RESET` → click "Clear all history".
- **CLI**: `python3 scripts/reset.py` from the project root.

Both wipe `data/inputs/`, `data/outputs/`, and `manifest.json`. Mapping config is
NOT reset — use the **Mapping Editor**'s delete panel for that.

## Versioning / history

Every upload is stored under `data/inputs/<period>_v<N>_stripe_<filename>` and
`data/inputs/<period>_v<N>_internal_<filename>` with metadata recorded in
`data/inputs/manifest.json` (period, version, reference, both paths, notes, user
overrides). The **History** tab lists every prior run; clicking **Load** re-runs the
pipeline against the stored input bytes (useful for replaying after a mapping change).

The reference label format is `<YYYY-MM> v<N>`, e.g. `2025-12 v1`, `2025-12 v2`.

## Future enhancement: folder-watch ingestion

The brief calls for the optional ability to trigger runs by dropping files into a source
folder per input type. The cleanest path would be a separate `watcher.py` that uses
[`watchdog`](https://pythonhosted.org/watchdog/) to observe `data/inbox_stripe/` and
`data/inbox_internal/`, pairs the most recent file from each, and calls
`pipeline.run` + `manifest.add_entry`. The current scope ships manual upload only;
the pipeline module is structured so the watcher would be a one-file addition.

## Tests

```bash
python3 -m pytest tests/ -v
```

The suite (69 tests) runs against three sources:

**Gold fixture** — `fixtures/threshold_recon_input.xlsx`:
- Period auto-detection (`2025-12`).
- Rollup categories correctly excluded.
- Per-row variances match the gold reference for all 21 rcs.
- Grand residual = `$1,344,570.50`.
- B/S/I summary matches gold.
- Per-cat validation row sums to 0 (recon captures all source data).
- Netting view SUM equals grand residual.
- Each exception tag matches the expected classification.

**CSV round-trip** — `fixtures/data_stripe.csv` + `fixtures/data_internal.csv` (generated from gold):
- CSV path produces identical period, residual, per-row variance, and exception tags as the xlsx path.

**Synthetic scenarios** — `fixtures/scenarios/` (generated by `python3 scripts/build_scenarios.py`):

| Scenario | What it exercises | Expected outcome |
|---|---|---|
| `scenario_1_clean_match` | Stripe per-rc fee on charge zeroed, billing/fee sign anomaly fixed, Stripe-only and Internal-only items removed | Grand residual ≈ 0; only composite-related exceptions (which net via the bridges) |
| `scenario_2_amount_diffs` | Clean baseline + billing/refund +$5,000 and cx_transfer/transfer −$1,500 | Two `Unexplained variance` exceptions with the exact known amounts; residual = +$3,500 |
| `scenario_3_new_stripe_rc` | Clean baseline + a new Stripe rc `beta_program_credit` not in mapping | Surfaces as `unmapped_stripe` exception; recon residual unaffected |
| `scenario_4_new_internal_rc` | Clean baseline + a new Internal rc `loyalty_credit` not in mapping | Surfaces as `unmapped_internal` exception; recon residual unaffected |

Regenerate the scenario fixtures any time you change the mapping or want different
numbers — the build script clones the gold workbook and applies surgical mutations
documented in `scripts/build_scenarios.py`.

## Project layout

```
project_threshold/
├── app.py                        Streamlit entry point
├── config/mapping.yaml           rc → Type/Ref/Src, composites, rollups, tolerance
├── threshold/
│   ├── config.py                 mapping load/save
│   ├── ingest.py                 read xlsx → DataFrames, drop rollups
│   ├── recon.py                  pivot + join + variance
│   ├── exceptions.py             classify each non-tied row
│   ├── summary.py                B/S/I + bridges + signed netting view
│   ├── output.py                 write xlsx workbook
│   ├── manifest.py               versioned monthly run log
│   └── pipeline.py               end-to-end orchestrator
├── data/
│   ├── inputs/                   stored uploads + manifest.json
│   └── outputs/                  generated recon xlsx + exceptions csv
├── fixtures/
│   └── threshold_recon_input.xlsx   gold reference for tests
├── tests/test_recon.py           pytest suite (43 tests)
└── requirements.txt
```

## Assumptions

1. **Source files have stable sheet names**: `Data - Stripe`, `Data - Internal`. Sheet name changes break ingestion (would need a sheet-mapping config).
2. **Single currency per file** (USD assumed). A multi-currency extension would split the recon by `currency`.
3. **Period is identified by the Internal amount column header** (a date). If absent, period defaults to `"unknown"`.
4. **Rollup rows always use the same names** (`total_subscription`, `subscription_cycle_*`). New rollups would need to be added to `rollup_categories_excluded`.
5. **The mapping is the source of truth for what's "expected"**; new rcs in either source surface as `UNMAPPED` exceptions rather than silently being dropped.
6. **Sign anomalies are flagged, never auto-corrected**. A future JE proposal step (stretch goal) could generate correcting entries before posting.

## Stretch goals (not yet implemented)

- Cross-month rc vocabulary diff: detect when an rc appears or disappears between months.
- JE proposal generator: emit a draft journal entry that corrects sign anomalies and books true reconciling items (e.g. credit `4xxx revenue_share`, debit `2300 sales_tax_payable`).
- Folder-watch ingestion (described above).
