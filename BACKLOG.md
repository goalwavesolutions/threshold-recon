# Backlog — known enhancements

Nice-to-haves identified during the build but deferred to keep the initial deliverable
focused. Listed here so they can be discussed in the Part 1 write-up rather than silently
deferred.

---

## 1. Re-key categorisation Type vocabulary to JE / GL account names

**Current:** Reconciliation rows are grouped by economic Type — `Gross activity`, `Fees`,
`Cash movements`, `Platform earnings`, `Disputes (composite)`, `Adjustments`,
`Reserve mechanics`, `Tax`. The names describe the *Stripe activity*.

**Improvement:** Re-key these to match the journal-entry chart of accounts the controller
actually books to — e.g. `Revenue`, `Contra revenue`, `Fees`, `Cash`,
`Other adjustments`, `Tax payable`. This makes the recon directly map to the JE proposal
step downstream (each Type maps to one or more GL accounts in NetSuite), and lets the
reconciled workbook double as the supporting document for the JE without a translation
step.

**Effort:** Low — change the `type` field per rc in [`config/mapping.yaml`](config/mapping.yaml).
No code change required. Editable from the in-app **Mapping Editor** tab.

**Why deferred:** Wanted to confirm the matching engine and bridge logic before naming
the buckets — easier to rename once we know what each bucket actually contains.

---

## 2. Three-state "Tie?" column to distinguish structural variance from true exceptions

**Current:** Two-state — `✓` (tied within tolerance) or `✗` (variance ≠ 0). Any row with
non-zero variance shows `✗`, even if the variance is *structurally* explained — e.g.
`1.4 dispute_gross` shows `✗` because Stripe NET = 0 for that rc, but the row is part of
the dispute composite that nets to 0 across the bridge (`1.4 + 2.2 = 5.1`).

**Improvement:** Add a third state — `−` highlighted yellow (or similar) — for rows that
are part of a known bridge (composite component, reserve pair, sign-anomaly pair) and
don't represent a true reconciling item. The grouping logic already exists in
[`pipeline.compute_exception_groups`](threshold/pipeline.py); the UI just needs to consume
a "bridge member" flag and the colour map needs a third tier.

**Why it matters:** The current view shows ~13 `✗` rows for the gold input, but only
~3 are true reconciling items needing investigation. The triage cards undersell how
clean the recon actually is, and the user sees more red than they should.

**Effort:** Low — extend [`_style_recon`](app.py) to read the bridge-member flag, and
update the triage card "discrepant rows" count to subtract bridge-resolved rows.

---

## 3. End-to-end CSV upload smoke-test through the UI

**Current:** Unit tests ([`tests/test_csv_ingest.py`](tests/test_csv_ingest.py)) confirm the
pipeline produces identical period, residual, per-row variance, and exception tags from
CSV vs xlsx inputs. The Streamlit upload widget accepts `.csv` in both Stripe and Internal
uploaders. But the **full UI flow** (upload → pre-flight → triage → exceptions → summary
→ download) hasn't been walked manually with CSV inputs.

**Improvement:** Run the end-to-end flow with the bundled
[`fixtures/data_stripe.csv`](fixtures/data_stripe.csv) +
[`fixtures/data_internal.csv`](fixtures/data_internal.csv) to confirm:
- Upload widget accepts CSV cleanly
- Period auto-detection works on the CSV-shaped header (Excel exports dates as
  `2025-12-31 00:00:00` strings, which the parser handles, but worth confirming visually)
- Pre-flight, triage, group rendering, sign-flip corrections, and final download all
  behave identically to the xlsx path
- Mixed inputs (e.g. CSV Stripe + xlsx Internal) also work

**Effort:** Manual, ~5 min. If issues surface, likely small fixes in
[`ingest.py`](threshold/ingest.py) header detection or the Streamlit uploader's
content-type handling.

**Why deferred:** Caught by automated test coverage, so functionally low-risk; just needs
a human eye on the rendered UI states.

---

## 4. Split Connect vs Billing revenue streams

**Current:** All Internal `transaction_categories` are reconciled together into one set
of recon rows. The brief calls out that Threshold runs *two* Stripe products with
different economics — **Stripe Connect** (embedded payments / merchant settlement to
property owners) and **Stripe Billing** (SaaS subscription invoicing) — and that "fee
structure differences between the two products" is one of the drivers of mismatch.

**Improvement:** Tag each `transaction_category` in the mapping with a `revenue_stream`
field (`connect` | `billing`), then surface a per-stream view alongside the unified
recon — Triage cards split into Connect / Billing columns, Type breakdown gains a
stream filter, and the JE preview (item 5 below) emits separate debit/credit lines per
stream. This is a prerequisite to any clean JE: Connect payments hit a different GL
account (merchant clearing / Stripe payable) than Billing subscription revenue, and
mixing them in one entry would make the AR/clearing reconciliation harder downstream.

**Effort:** Medium. New field on the mapping config, ~2 lines per cat to tag, then a
sidecar view in Recon View / Triage / Summary. The recon math doesn't change — only
how we slice the rows for presentation.

**Why deferred:** The matching engine and exception logic are stream-agnostic; the
split is a presentation + JE-preparation concern that's cleaner to layer on once the
core recon is locked.

---

## 5. Auto-generate the JE in a standardised import format

**Current:** The downloaded workbook is the audit/working-paper artifact. The journal
entry itself is still prepared manually by the controller from that artifact.

**Improvement:** Add a "JE Proposal" sheet (or separate CSV) to the output that emits
the period's journal entry in a **NetSuite-importable format** — one row per debit/credit
line with the GL account number, amount, memo, and (for Connect) the per-merchant
subsidiary/department dimension. Driven by:
- The mapping's `Type` field re-keyed to JE/GL accounts (item 1 above)
- The Connect/Billing split (item 4 above)
- The reconciled rows (with overrides + corrections applied)

This closes the loop from raw Stripe activity → reconciled view → posted JE in one
flow, which is the brief's stated end goal ("from raw data ingestion through to a
closed and reconciled ledger").

**Effort:** Medium-high. The mechanical part (writing rows in NetSuite's import schema)
is straightforward; the harder part is encoding the GL-account mapping rules
(which `Type` × `revenue_stream` combination posts to which account, and which sign
convention the GL expects).

**Why deferred:** Requires items 1 and 4 first, plus access to Threshold's actual
NetSuite chart of accounts to confirm the import schema. Worth scoping during a working
session with the AP/AR team rather than building blind.
