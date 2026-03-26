"""Normalize raw extracted DataFrames into a standard schema."""

import logging
from typing import Optional

import numpy as np
import pandas as pd
from dateutil import parser as dateutil_parser

logger = logging.getLogger(__name__)

# Canonical column names -> possible aliases in source files
COLUMN_ALIASES = {
    "timestamp":      ["timestamp", "time", "date", "datetime", "measured_at", "ts"],
    "position":       ["position", "pos", "location", "point", "site", "id"],
    "actual_value":   ["actual_value", "actual", "measured", "measured_value", "value", "reading"],
    "expected_value": ["expected_value", "expected", "nominal", "target", "reference", "ref"],
    "operator":       ["operator", "user", "technician", "tech", "operator_id"],
    "equipment_id":   ["equipment_id", "equipment", "machine", "tool", "tool_id", "system"],
    "axis":           ["axis", "direction", "component"],
    "unit":           ["unit", "units", "uom"],
}


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Map source column names to canonical names using alias lookup."""
    col_map = {}
    lower_cols = {c.lower().strip(): c for c in df.columns}

    for canonical, aliases in COLUMN_ALIASES.items():
        for alias in aliases:
            if alias in lower_cols:
                col_map[lower_cols[alias]] = canonical
                break

    df = df.rename(columns=col_map)
    logger.debug("Column mapping applied: %s", col_map)
    return df


def parse_timestamps(df: pd.DataFrame, col: str = "timestamp") -> pd.DataFrame:
    """Parse timestamp column to datetime, handling multiple formats."""
    if col not in df.columns:
        logger.warning("No '%s' column found; skipping timestamp parsing.", col)
        return df

    def _parse(val):
        if pd.isnull(val):
            return pd.NaT
        if isinstance(val, (pd.Timestamp, np.datetime64)):
            return pd.Timestamp(val)
        try:
            return pd.Timestamp(val)
        except Exception:
            pass
        try:
            return dateutil_parser.parse(str(val))
        except Exception:
            logger.debug("Could not parse timestamp: %r", val)
            return pd.NaT

    df[col] = df[col].apply(_parse)
    n_null = df[col].isna().sum()
    if n_null > 0:
        logger.warning("%d timestamp(s) could not be parsed and are NaT.", n_null)
    return df


def calculate_deviation(df: pd.DataFrame) -> pd.DataFrame:
    """Compute deviation = actual_value - expected_value if not already present."""
    if "deviation" not in df.columns:
        if "actual_value" in df.columns and "expected_value" in df.columns:
            df["deviation"] = df["actual_value"] - df["expected_value"]
            logger.debug("Deviation column calculated.")
        else:
            logger.warning("Cannot calculate deviation: missing actual_value or expected_value.")
    return df


def cast_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce measurement columns to float, logging any conversion failures."""
    numeric_cols = ["actual_value", "expected_value", "deviation"]
    for col in numeric_cols:
        if col in df.columns:
            original = df[col].copy()
            df[col] = pd.to_numeric(df[col], errors="coerce")
            failed = (df[col].isna() & original.notna()).sum()
            if failed > 0:
                logger.warning("Column '%s': %d value(s) could not be cast to numeric.", col, failed)
    return df


def handle_missing_values(
    df: pd.DataFrame,
    strategy: str = "flag",
    fill_value: Optional[float] = None,
) -> pd.DataFrame:
    """Handle NaNs in measurement columns. strategy: 'flag' | 'drop' | 'fill'."""
    measurement_cols = [c for c in ["actual_value", "expected_value", "deviation"] if c in df.columns]
    n_missing = df[measurement_cols].isna().any(axis=1).sum()

    if n_missing == 0:
        return df

    logger.info("Missing values detected in %d row(s).", n_missing)

    if strategy == "drop":
        df = df.dropna(subset=measurement_cols)
        logger.info("Dropped %d row(s) with missing measurement values.", n_missing)
    elif strategy == "fill":
        if fill_value is None:
            raise ValueError("fill_value must be provided when strategy='fill'.")
        df[measurement_cols] = df[measurement_cols].fillna(fill_value)
        logger.info("Filled missing values with %s.", fill_value)
    else:
        logger.info("Missing values left in place (strategy='flag').")

    return df


def normalize_position_ids(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize position labels to uppercase strings."""
    if "position" in df.columns:
        df["position"] = df["position"].astype(str).str.strip().str.upper()
    return df


def add_run_metadata(
    df: pd.DataFrame,
    source_file: str,
    equipment_id: Optional[str] = None,
    operator: Optional[str] = None,
) -> pd.DataFrame:
    """Inject run-level metadata if not already in the DataFrame."""
    if "equipment_id" not in df.columns or df["equipment_id"].isna().all():
        df["equipment_id"] = equipment_id or "UNKNOWN"
    if "operator" not in df.columns or df["operator"].isna().all():
        df["operator"] = operator or "UNKNOWN"
    df["source_file"] = source_file
    return df


def transform(
    df: pd.DataFrame,
    source_file: str = "",
    missing_strategy: str = "flag",
    equipment_id: Optional[str] = None,
    operator: Optional[str] = None,
) -> pd.DataFrame:
    """Run the full normalization chain on a raw extracted DataFrame."""
    logger.info("Starting transformation for '%s' (%d rows).", source_file, len(df))

    df = normalize_columns(df)
    df = parse_timestamps(df)
    df = cast_numeric(df)
    df = calculate_deviation(df)
    df = handle_missing_values(df, strategy=missing_strategy)
    df = normalize_position_ids(df)
    df = add_run_metadata(df, source_file, equipment_id, operator)
    df = df.dropna(how="all")

    logger.info("Transformation complete: %d rows retained.", len(df))
    return df
