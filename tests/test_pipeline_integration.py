"""
Integration tests: full end-to-end pipeline using an in-memory SQLite database.
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.database.schema import Base, init_db, get_session
from src.etl.extractors import ExtractorFactory
from src.etl.loaders import check_already_imported, load_to_database
from src.etl.transformers import transform
from src.validation.validators import run_all_validations

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def session():
    """In-memory SQLite session for tests."""
    engine = init_db(":memory:")
    s = get_session(engine)
    yield s
    s.close()


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "timestamp":      pd.date_range("2024-01-01", periods=6, freq="min"),
        "position":       ["POS_A1", "POS_A2", "POS_B1", "POS_B2", "POS_C1", "POS_C2"],
        "actual_value":   [10.002, 9.998, 10.005, 9.997, 10.001, 9.999],
        "expected_value": [10.0] * 6,
        "deviation":      [0.002, -0.002, 0.005, -0.003, 0.001, -0.001],
        "equipment_id":   ["TOOL_X1"] * 6,
        "operator":       ["OP001"] * 6,
        "source_file":    ["test.csv"] * 6,
    })


class TestEndToEnd:
    def test_csv_pipeline(self, session):
        filepath = str(FIXTURES / "sample_calibration.csv")
        df, file_hash = ExtractorFactory.extract(filepath)
        df = transform(df, source_file="sample_calibration.csv")
        results = run_all_validations(df, session=session)
        load_result = load_to_database(df, results, session, "sample_calibration.csv", file_hash)

        assert load_result["status"] == "success"
        assert load_result["rows_loaded"] == 10
        assert load_result["run_id"] is not None

    def test_json_pipeline(self, session):
        filepath = str(FIXTURES / "sample_calibration.json")
        df, file_hash = ExtractorFactory.extract(filepath)
        df = transform(df, source_file="sample_calibration.json")
        results = run_all_validations(df, session=session)
        load_result = load_to_database(df, results, session, "sample_calibration.json", file_hash)

        assert load_result["status"] == "success"
        assert load_result["rows_loaded"] == 6

    def test_log_pipeline(self, session):
        filepath = str(FIXTURES / "sample_calibration.log")
        df, file_hash = ExtractorFactory.extract(filepath)
        df = transform(df, source_file="sample_calibration.log")
        results = run_all_validations(df, session=session)
        load_result = load_to_database(df, results, session, "sample_calibration.log", file_hash)

        assert load_result["status"] == "success"
        assert load_result["rows_loaded"] == 6

    def test_dedup_prevents_reimport(self, session):
        filepath = str(FIXTURES / "sample_calibration.csv")
        df, file_hash = ExtractorFactory.extract(filepath)
        df = transform(df, source_file="sample_calibration.csv")
        results = run_all_validations(df, session=session)

        # First import
        r1 = load_to_database(df, results, session, "sample_calibration.csv", file_hash)
        assert r1["status"] == "success"

        # Second import should be blocked by dedup
        assert check_already_imported(session, file_hash) is True

    def test_validation_failure_blocks_load(self, session):
        """A DataFrame missing required columns should fail loading without force."""
        df = pd.DataFrame({"position": ["A", "B", "C", "D", "E"]})  # missing measurements
        results = run_all_validations(df)
        load_result = load_to_database(df, results, session, "bad_file.csv", force=False)
        assert load_result["status"] == "failed"

    def test_force_loads_despite_failures(self, session, sample_df):
        """With force=True, load proceeds even when there are validation failures."""
        results = [{"check_type": "test_fail", "status": "fail", "details": "Forced failure"}]
        load_result = load_to_database(sample_df, results, session, "forced.csv", force=True)
        assert load_result["status"] == "success"
