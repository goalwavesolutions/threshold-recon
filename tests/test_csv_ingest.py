"""Verify the CSV path reproduces the same recon as the xlsx path."""
from __future__ import annotations

from pathlib import Path

import pytest

from threshold import pipeline
from threshold.config import Mapping

FIXTURES = Path(__file__).parent.parent / "fixtures"
STRIPE_CSV = FIXTURES / "data_stripe.csv"
INTERNAL_CSV = FIXTURES / "data_internal.csv"
COMBINED_XLSX = FIXTURES / "threshold_recon_input.xlsx"


@pytest.fixture(scope="module")
def csv_result():
    mapping = Mapping.load()
    with open(STRIPE_CSV, "rb") as sf, open(INTERNAL_CSV, "rb") as inf:
        return pipeline.run(
            sf.read(), inf.read(), mapping,
            stripe_filename=STRIPE_CSV.name,
            internal_filename=INTERNAL_CSV.name,
        )


@pytest.fixture(scope="module")
def xlsx_result():
    mapping = Mapping.load()
    with open(COMBINED_XLSX, "rb") as f:
        data = f.read()
    return pipeline.run(data, data, mapping)


def test_csv_period_matches_xlsx(csv_result, xlsx_result):
    assert csv_result.source.period_label == xlsx_result.source.period_label == "2025-12"


def test_csv_residual_matches_xlsx(csv_result, xlsx_result):
    assert csv_result.summary.grand_residual == pytest.approx(
        xlsx_result.summary.grand_residual, abs=1.0
    )


def test_csv_per_row_variance_matches_xlsx(csv_result, xlsx_result):
    csv_var = csv_result.rows.set_index("Ref")["Variance"]
    xlsx_var = xlsx_result.rows.set_index("Ref")["Variance"]
    assert set(csv_var.index) == set(xlsx_var.index)
    for ref in csv_var.index:
        assert csv_var[ref] == pytest.approx(xlsx_var[ref], abs=1.0)


def test_csv_exception_tags_match_xlsx(csv_result, xlsx_result):
    csv_tags = csv_result.rows.set_index("Ref")["Exception Type"].to_dict()
    xlsx_tags = xlsx_result.rows.set_index("Ref")["Exception Type"].to_dict()
    assert csv_tags == xlsx_tags
