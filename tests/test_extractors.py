"""Unit tests for ETL extractors."""

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.etl.extractors import (
    CSVExtractor,
    ExcelExtractor,
    ExtractorFactory,
    JSONExtractor,
    LogExtractor,
)

FIXTURES = Path(__file__).parent / "fixtures"


class TestCSVExtractor:
    def test_extracts_csv(self):
        df = CSVExtractor().extract(str(FIXTURES / "sample_calibration.csv"))
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 10
        assert "position" in df.columns

    def test_columns_lowercased(self):
        df = CSVExtractor().extract(str(FIXTURES / "sample_calibration.csv"))
        for col in df.columns:
            assert col == col.lower()


class TestExcelExtractor:
    def test_extracts_xlsx(self):
        df = ExcelExtractor().extract(str(FIXTURES / "sample_calibration.xlsx"))
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 8
        assert "position" in df.columns

    def test_columns_lowercased(self):
        df = ExcelExtractor().extract(str(FIXTURES / "sample_calibration.xlsx"))
        for col in df.columns:
            assert col == col.lower()

    def test_has_expected_measurement_columns(self):
        df = ExcelExtractor().extract(str(FIXTURES / "sample_calibration.xlsx"))
        for col in ("actual_value", "expected_value", "operator", "equipment_id"):
            assert col in df.columns, f"Missing column: {col}"

    def test_raises_on_missing_file(self):
        with pytest.raises(FileNotFoundError):
            ExcelExtractor().extract(str(FIXTURES / "nonexistent.xlsx"))


class TestJSONExtractor:
    def test_extracts_json(self):
        df = JSONExtractor().extract(str(FIXTURES / "sample_calibration.json"))
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 6
        assert "actual_value" in df.columns

    def test_extracts_nested_data_key(self):
        df = JSONExtractor().extract(str(FIXTURES / "sample_calibration.json"))
        assert "equipment_id" in df.columns


class TestLogExtractor:
    def test_extracts_log(self):
        df = LogExtractor().extract(str(FIXTURES / "sample_calibration.log"))
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 6

    def test_skips_comment_lines(self):
        df = LogExtractor().extract(str(FIXTURES / "sample_calibration.log"))
        # Should not have empty rows from comment lines
        assert df.dropna(how="all").shape[0] == 6


class TestExtractorFactory:
    def test_auto_detects_csv(self):
        df, file_hash = ExtractorFactory.extract(str(FIXTURES / "sample_calibration.csv"))
        assert not df.empty
        assert len(file_hash) == 64  # SHA-256

    def test_auto_detects_json(self):
        df, _ = ExtractorFactory.extract(str(FIXTURES / "sample_calibration.json"))
        assert not df.empty

    def test_auto_detects_log(self):
        df, _ = ExtractorFactory.extract(str(FIXTURES / "sample_calibration.log"))
        assert not df.empty

    def test_auto_detects_xlsx(self):
        df, file_hash = ExtractorFactory.extract(str(FIXTURES / "sample_calibration.xlsx"))
        assert not df.empty
        assert len(file_hash) == 64

    def test_unsupported_extension_raises(self):
        with pytest.raises(ValueError, match="Unsupported file extension"):
            ExtractorFactory.get("file.xyz")

    def test_file_hash_is_deterministic(self):
        _, h1 = ExtractorFactory.extract(str(FIXTURES / "sample_calibration.csv"))
        _, h2 = ExtractorFactory.extract(str(FIXTURES / "sample_calibration.csv"))
        assert h1 == h2
