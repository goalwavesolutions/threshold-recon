"""Golden-output tests against the supplied threshold_recon_input.xlsx fixture."""
from __future__ import annotations

from pathlib import Path

import pytest

from threshold import pipeline
from threshold.config import Mapping

FIXTURE = Path(__file__).parent.parent / "fixtures" / "threshold_recon_input.xlsx"

# Per-row gold variances from the supplied recon spec (rounded to 2dp).
GOLD_VARIANCE_BY_REF = {
    1.1:  576336.36,
    1.2:  0.00,
    1.3:  0.00,
    1.4:  -603068.10,
    1.5:  176609.97,
    1.6:  -0.46,
    2.2:  -54135.00,
    2.3:  450.00,
    2.5:  576119.07,
    3.1:  0.00,
    3.2:  0.00,
    3.3:  0.00,
    4.1:  0.00,
    4.2:  0.00,
    4.3:  -440487.33,
    5.1:  657203.10,
    5.2:  -177059.97,
    6.1:  -25920.00,
    7.1:  5280000.00,
    7.2:  -5280000.00,
    8.1:  658522.86,
}

GOLD_BSI_TOTALS = {
    "B": (24568257.17, 23856289.53, 711967.64),
    "S": (0.00, -454223.13, 454223.13),
    "I": (178379.73, 0.00, 178379.73),
}

GOLD_GRAND_RESIDUAL = 1344570.50

# Exception tags we expect from the auto-classifier on this input.
EXPECTED_EXCEPTION_TAGS = {
    1.1: "SIGN ANOMALY (mirror of ref 2.5)",
    1.4: "Bundled in Stripe ref 5.1 (composite)",
    1.5: "Bundled in Stripe ref 5.2 (composite)",
    2.2: "Bundled in Stripe ref 5.1 (composite)",
    2.3: "Bundled in Stripe ref 5.2 (composite)",
    2.5: "SIGN ANOMALY (mirror of ref 1.1)",
    4.3: "Not in Internal",
    5.1: "Split in Internal across refs 1.4 + 2.2",
    5.2: "Split in Internal across refs 1.5 + 2.3",
    6.1: "Not in Internal",
    7.1: "Reserve mechanic (nets off with ref 7.2)",
    7.2: "Reserve mechanic (nets off with ref 7.1)",
    8.1: "Not in Stripe",
}


@pytest.fixture(scope="module")
def result():
    """Use the combined fixture for both inputs — each reader picks its own sheet."""
    mapping = Mapping.load()
    with open(FIXTURE, "rb") as f:
        data = f.read()
    return pipeline.run(data, data, mapping)


def test_period_detected(result):
    assert result.source.period_label == "2025-12"


def test_rollup_categories_dropped(result):
    cats = set(result.source.internal["transaction_category"])
    assert not (cats & {"total_subscription", "subscription_cycle_monthly", "subscription_cycle_yearly"})


@pytest.mark.parametrize("ref,expected_var", list(GOLD_VARIANCE_BY_REF.items()))
def test_variance_matches_gold(result, ref, expected_var):
    row = result.rows[result.rows["Ref"] == ref].iloc[0]
    assert row["Variance"] == pytest.approx(expected_var, abs=1.0), \
        f"ref {ref} ({row['reporting_category']}): got {row['Variance']:.2f}, expected {expected_var:.2f}"


def test_grand_residual_matches_gold(result):
    assert result.summary.grand_residual == pytest.approx(GOLD_GRAND_RESIDUAL, abs=1.0)


@pytest.mark.parametrize("src,expected", list(GOLD_BSI_TOTALS.items()))
def test_bsi_summary_matches_gold(result, src, expected):
    row = result.summary.bsi_summary[result.summary.bsi_summary["Source"] == src].iloc[0]
    assert row["Internal Total"] == pytest.approx(expected[0], abs=1.0)
    assert row["Stripe NET Total"] == pytest.approx(expected[1], abs=1.0)
    assert row["Net Variance"] == pytest.approx(expected[2], abs=1.0)


def test_per_cat_validation_ties(result):
    """Recon's per-cat sum must equal raw Internal's per-cat sum (no source data dropped)."""
    mapping = Mapping.load()
    recon_per_cat = result.rows[mapping.cat_columns].sum()
    raw_per_cat = result.recon.cat_validation
    delta = (recon_per_cat - raw_per_cat).abs().max()
    assert delta < mapping.tie_tolerance, f"Per-cat validation delta = {delta}"


def test_netting_view_sums_to_residual(result):
    sum_check = result.summary.netting_view["Internal − Stripe"].sum()
    assert sum_check == pytest.approx(result.summary.grand_residual, abs=1.0)


@pytest.mark.parametrize("ref,expected_tag", list(EXPECTED_EXCEPTION_TAGS.items()))
def test_exception_tag_matches_expected(result, ref, expected_tag):
    row = result.rows[result.rows["Ref"] == ref].iloc[0]
    assert row["Exception Type"] == expected_tag


def test_no_unmapped_rcs_in_supplied_data(result):
    assert result.recon.unmapped_stripe_rcs == []
    assert result.recon.unmapped_internal_rcs == []
