"""Extractors for CSV, Excel, JSON, and structured log files."""

import hashlib
import json
import logging
import re
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

SUPPORTED_EXTENSIONS = {".csv", ".xlsx", ".xls", ".json", ".log", ".txt"}


class BaseExtractor:
    """Abstract base for all format extractors."""

    def extract(self, filepath: str) -> pd.DataFrame:
        raise NotImplementedError

    def file_hash(self, filepath: str) -> str:
        """Compute SHA-256 hash of a file for deduplication."""
        sha256 = hashlib.sha256()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)
        return sha256.hexdigest()


class CSVExtractor(BaseExtractor):
    """Extract calibration data from CSV files."""

    def extract(self, filepath: str) -> pd.DataFrame:
        logger.info("Extracting CSV: %s", filepath)
        df = pd.read_csv(
            filepath,
            parse_dates=True,
        )
        df.columns = df.columns.str.strip().str.lower().str.replace(r"\s+", "_", regex=True)
        logger.debug("CSV extracted: %d rows, columns: %s", len(df), list(df.columns))
        return df


class ExcelExtractor(BaseExtractor):
    """Extract calibration data from Excel files (.xlsx / .xls)."""

    def extract(self, filepath: str) -> pd.DataFrame:
        logger.info("Extracting Excel: %s", filepath)
        xl = pd.ExcelFile(filepath)
        sheets = xl.sheet_names

        # Use first sheet that contains data
        for sheet in sheets:
            df = pd.read_excel(filepath, sheet_name=sheet, parse_dates=True)
            if not df.empty:
                df.columns = df.columns.str.strip().str.lower().str.replace(r"\s+", "_", regex=True)
                logger.debug("Excel sheet '%s' extracted: %d rows", sheet, len(df))
                return df

        raise ValueError(f"No non-empty sheet found in {filepath}")


class JSONExtractor(BaseExtractor):
    """Extract calibration data from JSON files (flat or nested)."""

    def extract(self, filepath: str) -> pd.DataFrame:
        logger.info("Extracting JSON: %s", filepath)
        with open(filepath, "r") as f:
            data = json.load(f)

        if isinstance(data, list):
            df = pd.json_normalize(data)
        elif isinstance(data, dict):
            for key in ("data", "measurements", "records", "results"):
                if key in data and isinstance(data[key], list):
                    df = pd.json_normalize(data[key])
                    break
            else:
                df = pd.json_normalize([data])

        df.columns = df.columns.str.strip().str.lower().str.replace(r"[\.\s]+", "_", regex=True)
        logger.debug("JSON extracted: %d rows", len(df))
        return df


class LogExtractor(BaseExtractor):
    """Parse KEY=VALUE structured log files. Timestamps auto-detected."""

    # Pattern captures key=value pairs anywhere in a line
    _KV_PATTERN = re.compile(r"(\w+)\s*=\s*([^\s,;|]+)")
    _TS_PATTERNS = [
        re.compile(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}"),
        re.compile(r"\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2}"),
        re.compile(r"\d{2}-\w{3}-\d{4}\s+\d{2}:\d{2}:\d{2}"),
    ]

    def extract(self, filepath: str) -> pd.DataFrame:
        logger.info("Extracting log file: %s", filepath)
        records = []
        with open(filepath, "r", errors="replace") as f:
            for lineno, line in enumerate(f, 1):
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                record = self._parse_line(line, lineno)
                if record:
                    records.append(record)

        if not records:
            raise ValueError(f"No parseable records found in {filepath}")

        df = pd.DataFrame(records)
        df.columns = df.columns.str.lower()
        logger.debug("Log extracted: %d records", len(df))
        return df

    def _parse_line(self, line: str, lineno: int) -> dict:
        record = {}

        # Extract timestamp
        for pattern in self._TS_PATTERNS:
            m = pattern.search(line)
            if m:
                record["timestamp"] = m.group()
                break

        # Extract key=value pairs
        for key, value in self._KV_PATTERN.findall(line):
            record[key.lower()] = value

        if len(record) < 2:
            logger.debug("Line %d skipped (insufficient fields): %s", lineno, line[:80])
            return {}
        return record


class ExtractorFactory:
    """Auto-detect file format and return appropriate extractor."""

    _MAP = {
        ".csv": CSVExtractor,
        ".xlsx": ExcelExtractor,
        ".xls": ExcelExtractor,
        ".json": JSONExtractor,
        ".log": LogExtractor,
        ".txt": LogExtractor,
    }

    @classmethod
    def get(cls, filepath: str) -> BaseExtractor:
        ext = Path(filepath).suffix.lower()
        extractor_cls = cls._MAP.get(ext)
        if extractor_cls is None:
            raise ValueError(
                f"Unsupported file extension '{ext}'. "
                f"Supported: {sorted(cls._MAP)}"
            )
        return extractor_cls()

    @classmethod
    def extract(cls, filepath: str) -> tuple[pd.DataFrame, str]:
        """Extract data and return (DataFrame, file_hash)."""
        extractor = cls.get(filepath)
        df = extractor.extract(filepath)
        file_hash = extractor.file_hash(filepath)
        return df, file_hash
