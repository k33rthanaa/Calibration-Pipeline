"""Unit tests for the validation framework."""

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.validation.validators import (
    detect_duplicates,
    detect_outliers,
    format_validation_report,
    run_all_validations,
    validate_deviation_range,
    validate_no_missing_measurements,
    validate_required_columns,
    validate_row_count,
    validate_timestamps,
)


def good_df() -> pd.DataFrame:
    return pd.DataFrame({
        "timestamp":      pd.date_range("2024-01-01", periods=10, freq="min"),
        "position":       [f"POS_{i}" for i in range(10)],
        "actual_value":   [10.0 + i * 0.001 for i in range(10)],
        "expected_value": [10.0] * 10,
        "deviation":      [i * 0.001 for i in range(10)],
        "equipment_id":   ["TOOL_A"] * 10,
        "operator":       ["OP001"] * 10,
    })


class TestRequiredColumns:
    def test_passes_with_all_columns(self):
        result = validate_required_columns(good_df())
        assert result["status"] == "pass"

    def test_fails_when_column_missing(self):
        df = good_df().drop(columns=["deviation"])
        result = validate_required_columns(df)
        assert result["status"] == "fail"
        assert "deviation" in result["details"]


class TestRowCount:
    def test_passes_sufficient_rows(self):
        result = validate_row_count(good_df(), min_rows=5)
        assert result["status"] == "pass"

    def test_fails_too_few_rows(self):
        df = good_df().head(2)
        result = validate_row_count(df, min_rows=5)
        assert result["status"] == "fail"


class TestMissingMeasurements:
    def test_passes_no_nulls(self):
        result = validate_no_missing_measurements(good_df())
        assert result["status"] == "pass"

    def test_warns_when_nulls_present(self):
        df = good_df().copy()
        df.loc[0, "actual_value"] = None
        result = validate_no_missing_measurements(df)
        assert result["status"] == "warn"


class TestTimestamps:
    def test_passes_chronological(self):
        result = validate_timestamps(good_df(), max_gap_hours=24)
        assert result["status"] == "pass"

    def test_warns_large_gap(self):
        df = good_df().copy()
        # Introduce a 48-hour gap
        df.loc[5, "timestamp"] = pd.Timestamp("2024-01-02 12:00:00")
        result = validate_timestamps(df, max_gap_hours=1)
        assert result["status"] == "warn"

    def test_warns_no_timestamp_column(self):
        df = good_df().drop(columns=["timestamp"])
        result = validate_timestamps(df)
        assert result["status"] == "warn"


class TestDuplicates:
    def test_passes_unique_rows(self):
        result = detect_duplicates(good_df())
        assert result["status"] == "pass"

    def test_warns_in_file_duplicates(self):
        df = pd.concat([good_df().head(3), good_df().head(3)], ignore_index=True)
        result = detect_duplicates(df)
        assert result["status"] == "warn"
        assert "duplicate" in result["details"].lower()


class TestOutliers:
    def test_passes_no_outliers(self):
        result = detect_outliers(good_df())
        assert result["status"] == "pass"

    def test_warns_with_outlier(self):
        df = good_df().copy()
        df.loc[0, "deviation"] = 999.0  # extreme outlier
        result = detect_outliers(df)
        assert result["status"] in ("warn", "fail")

    def test_warns_too_few_points(self):
        df = good_df().head(2)
        result = detect_outliers(df)
        assert result["status"] == "warn"


class TestDeviationRange:
    def test_passes_within_threshold(self):
        result = validate_deviation_range(good_df(), alert_threshold=1.0)
        assert result["status"] == "pass"

    def test_warns_breach_threshold(self):
        df = good_df().copy()
        df.loc[0, "deviation"] = 2.0
        result = validate_deviation_range(df, alert_threshold=0.5)
        assert result["status"] == "warn"


class TestRunAllValidations:
    def test_returns_list_of_results(self):
        results = run_all_validations(good_df())
        assert isinstance(results, list)
        assert all("status" in r and "check_type" in r for r in results)

    def test_all_pass_for_good_data(self):
        results = run_all_validations(good_df())
        statuses = {r["status"] for r in results}
        assert "fail" not in statuses


class TestFormatReport:
    def test_returns_string(self):
        results = run_all_validations(good_df())
        report = format_validation_report(results)
        assert isinstance(report, str)
        assert "VALIDATION REPORT" in report
        assert "TOTAL" in report
