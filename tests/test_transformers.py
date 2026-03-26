"""Unit tests for ETL transformers."""

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.etl.transformers import (
    calculate_deviation,
    cast_numeric,
    handle_missing_values,
    normalize_columns,
    normalize_position_ids,
    parse_timestamps,
    transform,
)


def make_df(**kwargs) -> pd.DataFrame:
    base = {
        "timestamp": ["2024-03-01 08:00:00", "2024-03-01 08:01:00"],
        "position":  ["pos_a1", "pos_a2"],
        "actual_value":   [10.002, 9.998],
        "expected_value": [10.000, 10.000],
    }
    base.update(kwargs)
    return pd.DataFrame(base)


class TestNormalizeColumns:
    def test_maps_aliases(self):
        df = pd.DataFrame({"Actual": [1.0], "Nominal": [1.0], "Pos": ["A"]})
        result = normalize_columns(df)
        assert "actual_value" in result.columns
        assert "expected_value" in result.columns
        assert "position" in result.columns

    def test_unknown_columns_preserved(self):
        df = pd.DataFrame({"custom_col": [1]})
        result = normalize_columns(df)
        assert "custom_col" in result.columns


class TestParseTimestamps:
    def test_parses_iso_format(self):
        df = make_df()
        result = parse_timestamps(df)
        assert pd.api.types.is_datetime64_any_dtype(result["timestamp"])

    def test_handles_nat_gracefully(self):
        df = make_df(timestamp=["not-a-date", "2024-01-01"])
        result = parse_timestamps(df)
        assert result["timestamp"].isna().sum() == 1

    def test_missing_column_does_not_raise(self):
        df = pd.DataFrame({"value": [1, 2]})
        result = parse_timestamps(df)  # should not raise
        assert "value" in result.columns


class TestCalculateDeviation:
    def test_calculates_deviation(self):
        df = make_df()
        df = df.drop(columns=["position"])
        df["actual_value"] = [10.002, 9.998]
        df["expected_value"] = [10.0, 10.0]
        result = calculate_deviation(df)
        assert "deviation" in result.columns
        assert abs(result["deviation"].iloc[0] - 0.002) < 1e-9

    def test_does_not_overwrite_existing_deviation(self):
        df = make_df()
        df["deviation"] = [99.9, 99.9]
        result = calculate_deviation(df)
        assert result["deviation"].iloc[0] == 99.9


class TestCastNumeric:
    def test_converts_string_numbers(self):
        df = pd.DataFrame({"actual_value": ["10.5", "9.8"], "expected_value": ["10.0", "10.0"]})
        result = cast_numeric(df)
        assert result["actual_value"].dtype == float

    def test_bad_values_become_nan(self):
        df = pd.DataFrame({"actual_value": ["abc", "10.0"], "expected_value": ["10.0", "10.0"]})
        result = cast_numeric(df)
        assert result["actual_value"].isna().sum() == 1


class TestHandleMissingValues:
    def test_flag_strategy_leaves_nans(self):
        df = pd.DataFrame({"actual_value": [None, 10.0], "expected_value": [10.0, 10.0], "deviation": [None, 0.0]})
        result = handle_missing_values(df, strategy="flag")
        assert result["actual_value"].isna().sum() == 1

    def test_drop_strategy_removes_rows(self):
        df = pd.DataFrame({"actual_value": [None, 10.0], "expected_value": [10.0, 10.0], "deviation": [None, 0.0]})
        result = handle_missing_values(df, strategy="drop")
        assert len(result) == 1

    def test_fill_strategy_fills_value(self):
        df = pd.DataFrame({"actual_value": [None, 10.0], "expected_value": [10.0, 10.0], "deviation": [None, 0.0]})
        result = handle_missing_values(df, strategy="fill", fill_value=0.0)
        assert result["actual_value"].isna().sum() == 0


class TestNormalizePositionIds:
    def test_uppercases_positions(self):
        df = pd.DataFrame({"position": ["pos_a1", "pos_b2"]})
        result = normalize_position_ids(df)
        assert result["position"].iloc[0] == "POS_A1"


class TestTransformIntegration:
    def test_full_transform_pipeline(self):
        df = make_df()
        result = transform(df, source_file="test.csv")
        assert "deviation" in result.columns
        assert "source_file" in result.columns
        assert pd.api.types.is_datetime64_any_dtype(result["timestamp"])
        assert result["position"].str.isupper().all()
