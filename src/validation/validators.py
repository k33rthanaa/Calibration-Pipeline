"""
Data quality validation framework for calibration measurements.
Each check returns a standardized result dict and never mutates the DataFrame.
"""

import logging
from datetime import timedelta
from typing import Optional

import numpy as np
import pandas as pd
from sqlalchemy.orm import Session

from src.database.schema import CalibrationMeasurement

logger = logging.getLogger(__name__)


def _result(check_type: str, status: str, details: str) -> dict:
    return {"check_type": check_type, "status": status, "details": details}


def _pass(check_type: str, msg: str = "OK") -> dict:
    logger.debug("[PASS] %s: %s", check_type, msg)
    return _result(check_type, "pass", msg)


def _warn(check_type: str, msg: str) -> dict:
    logger.warning("[WARN] %s: %s", check_type, msg)
    return _result(check_type, "warn", msg)


def _fail(check_type: str, msg: str) -> dict:
    logger.error("[FAIL] %s: %s", check_type, msg)
    return _result(check_type, "fail", msg)



def validate_required_columns(df: pd.DataFrame) -> dict:
    """Ensure all required columns are present."""
    required = {"position", "actual_value", "expected_value", "deviation"}
    missing = required - set(df.columns)
    if missing:
        return _fail("required_columns", f"Missing columns: {sorted(missing)}")
    return _pass("required_columns", f"All required columns present: {sorted(required)}")


def validate_no_missing_measurements(df: pd.DataFrame) -> dict:
    """Flag rows where key measurement fields are null."""
    cols = [c for c in ["actual_value", "expected_value", "deviation"] if c in df.columns]
    null_counts = df[cols].isna().sum()
    total_nulls = null_counts.sum()
    if total_nulls == 0:
        return _pass("missing_measurements", "No missing measurement values.")
    detail = "; ".join(f"{c}: {n} null(s)" for c, n in null_counts.items() if n > 0)
    return _warn("missing_measurements", f"{total_nulls} missing value(s): {detail}")


def validate_timestamps(df: pd.DataFrame, max_gap_hours: float = 24.0) -> dict:
    """Check chronological order and detect large gaps."""
    if "timestamp" not in df.columns:
        return _warn("timestamp_consistency", "No 'timestamp' column found.")

    ts = df["timestamp"].dropna().sort_values()
    if ts.empty:
        return _warn("timestamp_consistency", "All timestamps are null.")

    issues = []

    # Check chronological order in original data
    original_ts = df["timestamp"].dropna()
    if not original_ts.is_monotonic_increasing:
        issues.append("Timestamps are not in chronological order.")

    # Detect large gaps
    diffs = ts.diff().dropna()
    threshold = timedelta(hours=max_gap_hours)
    large_gaps = diffs[diffs > threshold]
    if not large_gaps.empty:
        gap_details = [
            f"{gap!s} gap before {ts.iloc[i]}"
            for i, gap in zip(large_gaps.index, large_gaps)
        ]
        issues.append(f"{len(large_gaps)} gap(s) > {max_gap_hours}h: {gap_details[:3]}")

    if issues:
        return _warn("timestamp_consistency", " | ".join(issues))
    return _pass("timestamp_consistency", f"{len(ts)} timestamps are consistent.")


def detect_duplicates(
    df: pd.DataFrame,
    session: Optional[Session] = None,
    duplicate_window_hours: float = 1.0,
) -> dict:
    """Detect in-file duplicates and optionally check against existing DB records."""
    issues = []

    # In-file duplicates
    key_cols = [c for c in ["timestamp", "position", "actual_value"] if c in df.columns]
    if key_cols:
        dupes = df.duplicated(subset=key_cols, keep=False)
        n_dupes = dupes.sum()
        if n_dupes > 0:
            issues.append(f"{n_dupes} duplicate row(s) within file (keys: {key_cols}).")

    # DB cross-check (if session provided and timestamps present)
    if session is not None and "timestamp" in df.columns and "position" in df.columns:
        ts_min = df["timestamp"].min()
        ts_max = df["timestamp"].max()
        if pd.notna(ts_min) and pd.notna(ts_max):
            window = timedelta(hours=duplicate_window_hours)
            existing = (
                session.query(CalibrationMeasurement)
                .filter(
                    CalibrationMeasurement.timestamp >= ts_min - window,
                    CalibrationMeasurement.timestamp <= ts_max + window,
                )
                .count()
            )
            if existing > 0:
                issues.append(
                    f"Database already contains {existing} measurement(s) "
                    f"within ±{duplicate_window_hours}h of this file's time range. "
                    "Possible re-import."
                )

    if issues:
        return _warn("duplicate_detection", " | ".join(issues))
    return _pass("duplicate_detection", "No duplicates detected.")


def detect_outliers(df: pd.DataFrame, z_threshold: float = 3.0) -> dict:
    """Flag outliers using Z-score and IQR methods on the deviation column."""
    if "deviation" not in df.columns:
        return _warn("outlier_detection", "No 'deviation' column to check.")

    dev = df["deviation"].dropna()
    if len(dev) < 4:
        return _warn("outlier_detection", "Too few data points for outlier detection.")

    # Z-score
    mean, std = dev.mean(), dev.std()
    if std == 0:
        return _pass("outlier_detection", "All deviations identical — no outliers.")

    z_scores = (dev - mean).abs() / std
    z_outliers = z_scores[z_scores > z_threshold]

    # IQR
    q1, q3 = dev.quantile(0.25), dev.quantile(0.75)
    iqr = q3 - q1
    iqr_outliers = dev[(dev < q1 - 1.5 * iqr) | (dev > q3 + 1.5 * iqr)]

    outlier_count = len(z_outliers.index.union(iqr_outliers.index))
    pct = 100 * outlier_count / len(dev)

    detail = (
        f"{outlier_count} outlier(s) ({pct:.1f}%) detected. "
        f"Z-score: {len(z_outliers)}, IQR: {len(iqr_outliers)}. "
        f"Deviation stats: mean={mean:.4f}, std={std:.4f}, range=[{dev.min():.4f}, {dev.max():.4f}]."
    )

    if outlier_count == 0:
        return _pass("outlier_detection", f"No outliers. {detail}")
    if pct > 20:
        return _fail("outlier_detection", f"High outlier rate. {detail}")
    return _warn("outlier_detection", detail)


def validate_deviation_range(
    df: pd.DataFrame,
    alert_threshold: float = 0.5,
) -> dict:
    """Warn if any absolute deviation exceeds the alert threshold."""
    if "deviation" not in df.columns:
        return _warn("deviation_range", "No 'deviation' column.")

    breaches = df[df["deviation"].abs() > alert_threshold]
    if breaches.empty:
        return _pass("deviation_range",
                     f"All deviations within ±{alert_threshold}.")
    positions = breaches["position"].tolist() if "position" in breaches.columns else []
    return _warn(
        "deviation_range",
        f"{len(breaches)} measurement(s) exceed ±{alert_threshold}: "
        f"positions={positions[:10]}{'...' if len(positions) > 10 else ''}",
    )


def validate_row_count(df: pd.DataFrame, min_rows: int = 5) -> dict:
    """Ensure the file contains a minimum number of measurements."""
    n = len(df)
    if n < min_rows:
        return _fail("row_count", f"Only {n} row(s); minimum expected is {min_rows}.")
    return _pass("row_count", f"{n} rows present.")



def run_all_validations(
    df: pd.DataFrame,
    session: Optional[Session] = None,
    config: Optional[dict] = None,
) -> list[dict]:
    """
    Run the complete validation suite and return a list of result dicts.

    Args:
        df:      Transformed measurement DataFrame.
        session: SQLAlchemy session for cross-DB duplicate checks (optional).
        config:  Validation config dict (from config.yaml 'validation' section).
    """
    cfg = config or {}
    z_threshold = cfg.get("outlier_threshold", 3.0)
    max_gap_hours = cfg.get("max_gap_hours", 24.0)
    dup_window = cfg.get("duplicate_window_hours", 1.0)
    alert_threshold = cfg.get("alert_drift_threshold", 0.5)
    min_rows = cfg.get("min_measurements_per_run", 5)

    results = [
        validate_required_columns(df),
        validate_row_count(df, min_rows),
        validate_no_missing_measurements(df),
        validate_timestamps(df, max_gap_hours),
        detect_duplicates(df, session, dup_window),
        detect_outliers(df, z_threshold),
        validate_deviation_range(df, alert_threshold),
    ]

    passes = sum(1 for r in results if r["status"] == "pass")
    warns  = sum(1 for r in results if r["status"] == "warn")
    fails  = sum(1 for r in results if r["status"] == "fail")
    logger.info("Validation summary: %d pass, %d warn, %d fail.", passes, warns, fails)
    return results


def format_validation_report(results: list[dict]) -> str:
    """Render a human-readable validation report string."""
    lines = ["=" * 60, "VALIDATION REPORT", "=" * 60]
    for r in results:
        icon = {"pass": "✓", "warn": "⚠", "fail": "✗"}.get(r["status"], "?")
        lines.append(f"  {icon} [{r['status'].upper():4}] {r['check_type']}")
        lines.append(f"         {r['details']}")
    lines.append("=" * 60)
    passes = sum(1 for r in results if r["status"] == "pass")
    warns  = sum(1 for r in results if r["status"] == "warn")
    fails  = sum(1 for r in results if r["status"] == "fail")
    lines.append(f"  TOTAL: {passes} pass | {warns} warn | {fails} fail")
    lines.append("=" * 60)
    return "\n".join(lines)
