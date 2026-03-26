"""Unit tests for analysis metrics (src/analysis/metrics.py)."""

import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.analysis.metrics import (
    assess_stability,
    calculate_mean_drift,
    calculate_run_variation,
    equipment_comparison,
    generate_summary_report,
    operator_performance,
    time_based_trends,
    worst_performing_positions,
)
from src.database.schema import Base, init_db, get_session
from src.etl.loaders import load_to_database
from src.etl.transformers import transform


# ─── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def session():
    """In-memory SQLite session populated with two calibration runs."""
    engine = init_db(":memory:")
    s = get_session(engine)
    yield s
    s.close()


def _make_df(
    n: int = 10,
    equipment_id: str = "TOOL_A",
    operator: str = "OP001",
    base_ts: datetime = None,
    drift: float = 0.0,
) -> pd.DataFrame:
    """Build a clean calibration DataFrame with optional linear drift."""
    # Default to recent timestamps so lookback-window filters never exclude test data
    base_ts = base_ts or (datetime.utcnow() - timedelta(hours=n))
    timestamps = [base_ts + timedelta(minutes=i) for i in range(n)]
    deviations = [round(0.001 * (i + 1) + drift * i, 6) for i in range(n)]
    return pd.DataFrame({
        "timestamp":      timestamps,
        "position":       [f"POS_{chr(65 + i % 6)}{i // 6 + 1}" for i in range(n)],
        "actual_value":   [10.0 + d for d in deviations],
        "expected_value": [10.0] * n,
        "deviation":      deviations,
        "equipment_id":   [equipment_id] * n,
        "operator":       [operator] * n,
        "source_file":    ["test.csv"] * n,
    })


@pytest.fixture
def loaded_session(session):
    """Session with two runs loaded: TOOL_A/OP001 and TOOL_B/OP002."""
    now = datetime.utcnow()
    df1 = _make_df(10, equipment_id="TOOL_A", operator="OP001",
                   base_ts=now - timedelta(days=2))
    df2 = _make_df(8, equipment_id="TOOL_B", operator="OP002",
                   base_ts=now - timedelta(days=1), drift=0.05)

    for df, fname in [(df1, "run1.csv"), (df2, "run2.csv")]:
        load_to_database(df, [], session, fname, force=True)

    return session


# ─── calculate_mean_drift ─────────────────────────────────────────────────────

class TestCalculateMeanDrift:
    def test_returns_expected_keys(self, loaded_session):
        result = calculate_mean_drift(run_id=1, session=loaded_session)
        for key in ("run_id", "per_position", "drift_slope", "drift_direction"):
            assert key in result

    def test_run_id_matches(self, loaded_session):
        result = calculate_mean_drift(run_id=1, session=loaded_session)
        assert result["run_id"] == 1

    def test_per_position_is_dataframe(self, loaded_session):
        result = calculate_mean_drift(run_id=1, session=loaded_session)
        assert isinstance(result["per_position"], pd.DataFrame)
        assert not result["per_position"].empty

    def test_drift_slope_is_float(self, loaded_session):
        result = calculate_mean_drift(run_id=1, session=loaded_session)
        assert isinstance(result["drift_slope"], float)

    def test_drift_direction_positive_for_increasing_deviations(self, loaded_session):
        # run1 has monotonically increasing deviations → positive slope
        result = calculate_mean_drift(run_id=1, session=loaded_session)
        assert result["drift_direction"] == "positive"

    def test_empty_run_returns_graceful_result(self, session):
        result = calculate_mean_drift(run_id=9999, session=session)
        assert result["drift_slope"] is None
        assert result["drift_direction"] == "unknown"


# ─── calculate_run_variation ──────────────────────────────────────────────────

class TestCalculateRunVariation:
    def test_returns_expected_keys(self, loaded_session):
        result = calculate_run_variation("POS_A1", loaded_session, lookback_days=9999)
        assert "n_runs" in result
        assert "mean" in result
        assert "std" in result
        assert "cv" in result
        assert "range" in result

    def test_unknown_position_returns_zero_runs(self, loaded_session):
        result = calculate_run_variation("POS_ZZZ", loaded_session, lookback_days=9999)
        assert result.get("n_runs", 0) == 0

    def test_std_non_negative(self, loaded_session):
        result = calculate_run_variation("POS_A1", loaded_session, lookback_days=9999)
        if "std" in result:
            assert result["std"] >= 0

    def test_range_non_negative(self, loaded_session):
        result = calculate_run_variation("POS_A1", loaded_session, lookback_days=9999)
        if "range" in result:
            assert result["range"] >= 0

    def test_empty_db_returns_empty(self, session):
        result = calculate_run_variation("POS_A1", session, lookback_days=30)
        assert result == {}


# ─── assess_stability ─────────────────────────────────────────────────────────

class TestAssessStability:
    def test_returns_expected_keys(self, loaded_session):
        result = assess_stability("TOOL_A", loaded_session, lookback_days=9999)
        for key in ("equipment_id", "mean", "ucl", "lcl", "out_of_control", "trend", "alert"):
            assert key in result

    def test_ucl_greater_than_lcl(self, loaded_session):
        result = assess_stability("TOOL_A", loaded_session, lookback_days=9999)
        assert result["ucl"] >= result["lcl"]

    def test_out_of_control_is_non_negative_int(self, loaded_session):
        result = assess_stability("TOOL_A", loaded_session, lookback_days=9999)
        assert isinstance(result["out_of_control"], int)
        assert result["out_of_control"] >= 0

    def test_alert_is_bool(self, loaded_session):
        result = assess_stability("TOOL_A", loaded_session, lookback_days=9999)
        assert isinstance(result["alert"], bool)

    def test_unknown_equipment_returns_minimal_dict(self, loaded_session):
        result = assess_stability("NONEXISTENT", loaded_session, lookback_days=9999)
        assert result == {"equipment_id": "NONEXISTENT", "alert": False}

    def test_large_mean_triggers_alert(self, session):
        """A mean deviation far above alert_threshold should set alert=True."""
        df = _make_df(10, equipment_id="DRIFTED", drift=1.0)
        load_to_database(df, [], session, "drifted.csv", force=True)
        result = assess_stability("DRIFTED", session, lookback_days=9999, alert_threshold=0.1)
        assert result["alert"] is True


# ─── Diagnostic queries ───────────────────────────────────────────────────────

class TestWorstPerformingPositions:
    def test_returns_dataframe(self, loaded_session):
        df = worst_performing_positions(loaded_session)
        assert isinstance(df, pd.DataFrame)

    def test_limited_to_top_n(self, loaded_session):
        df = worst_performing_positions(loaded_session, top_n=3)
        assert len(df) <= 3

    def test_sorted_descending_by_abs_deviation(self, loaded_session):
        df = worst_performing_positions(loaded_session, top_n=10)
        if len(df) > 1:
            assert df["mean_abs_dev"].iloc[0] >= df["mean_abs_dev"].iloc[-1]

    def test_empty_db_returns_empty_df(self, session):
        df = worst_performing_positions(session)
        assert df.empty


class TestEquipmentComparison:
    def test_returns_dataframe_with_equipment_rows(self, loaded_session):
        df = equipment_comparison(loaded_session)
        assert isinstance(df, pd.DataFrame)
        assert "equipment_id" in df.columns
        assert len(df) == 2  # TOOL_A and TOOL_B

    def test_has_required_columns(self, loaded_session):
        df = equipment_comparison(loaded_session)
        for col in ("equipment_id", "mean_dev", "std_dev", "n_measurements"):
            assert col in df.columns

    def test_empty_db_returns_empty(self, session):
        df = equipment_comparison(session)
        assert df.empty


class TestOperatorPerformance:
    def test_returns_dataframe(self, loaded_session):
        df = operator_performance(loaded_session)
        assert isinstance(df, pd.DataFrame)
        assert "operator" in df.columns

    def test_two_operators_present(self, loaded_session):
        df = operator_performance(loaded_session)
        assert len(df) == 2

    def test_empty_db_returns_empty(self, session):
        df = operator_performance(session)
        assert df.empty


class TestTimeBasedTrends:
    def test_daily_aggregation(self, loaded_session):
        df = time_based_trends(loaded_session, freq="D")
        assert isinstance(df, pd.DataFrame)
        assert "mean_dev" in df.columns

    def test_weekly_aggregation(self, loaded_session):
        df = time_based_trends(loaded_session, freq="W")
        assert isinstance(df, pd.DataFrame)

    def test_empty_db_returns_empty(self, session):
        df = time_based_trends(session)
        assert df.empty

    def test_at_least_one_row_per_run_day(self, loaded_session):
        # Two runs on different days → at least 2 daily buckets
        df = time_based_trends(loaded_session, freq="D")
        assert len(df) >= 2


# ─── generate_summary_report ─────────────────────────────────────────────────

class TestGenerateSummaryReport:
    def test_returns_dict_with_expected_keys(self, loaded_session):
        report = generate_summary_report(loaded_session)
        for key in ("worst_positions", "equipment_summary", "operator_summary", "daily_trend"):
            assert key in report

    def test_all_values_are_dataframes(self, loaded_session):
        report = generate_summary_report(loaded_session)
        for key, val in report.items():
            assert isinstance(val, pd.DataFrame), f"Expected DataFrame for key '{key}'"

    def test_empty_db_all_empty_dataframes(self, session):
        report = generate_summary_report(session)
        for key, val in report.items():
            assert val.empty, f"Expected empty DataFrame for key '{key}' on empty DB"
