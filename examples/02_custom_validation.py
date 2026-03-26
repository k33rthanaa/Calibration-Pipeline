#!/usr/bin/env python3
"""
Example 02: Custom validation configuration.

Shows how to tighten or loosen validation thresholds and
add custom post-validation logic before loading.
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.database.schema import init_db, get_session
from src.etl.extractors import ExtractorFactory
from src.etl.transformers import transform
from src.validation.validators import (
    detect_outliers,
    format_validation_report,
    run_all_validations,
    validate_deviation_range,
    validate_required_columns,
    validate_row_count,
    validate_timestamps,
)

DB_PATH = "data/calibration.db"
FILE_PATH = "tests/fixtures/sample_calibration.csv"

engine = init_db(DB_PATH)
session = get_session(engine)

df, _ = ExtractorFactory.extract(FILE_PATH)
df = transform(df, source_file=FILE_PATH)

# ── Option A: Use built-in config dict ─────────────────────────────────────────
strict_config = {
    "outlier_threshold": 2.0,        # Tighter: flag at 2 SD instead of 3
    "max_gap_hours": 4,              # Tighter: flag gaps > 4h
    "duplicate_window_hours": 0.5,
    "alert_drift_threshold": 0.1,    # Tighter: flag deviations > 0.1
    "min_measurements_per_run": 8,
}

print("=== Strict Validation ===")
results = run_all_validations(df, session=session, config=strict_config)
print(format_validation_report(results))

# ── Option B: Run individual checks selectively ────────────────────────────────
print("=== Custom Check Selection ===")
custom_results = [
    validate_required_columns(df),
    validate_row_count(df, min_rows=3),
    detect_outliers(df, z_threshold=2.5),
    validate_deviation_range(df, alert_threshold=0.01),
]
print(format_validation_report(custom_results))

# ── Option C: Inspect which positions fail your threshold ──────────────────────
print("=== Positions exceeding ±0.003 deviation ===")
bad = df[df["deviation"].abs() > 0.003][["position", "deviation"]]
if bad.empty:
    print("  All positions within threshold.")
else:
    print(bad.to_string(index=False))

session.close()
