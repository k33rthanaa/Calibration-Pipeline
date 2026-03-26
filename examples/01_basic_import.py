#!/usr/bin/env python3
"""
Example 01: Basic single-file import.

Demonstrates extracting, transforming, validating, and loading
a single calibration file into the database.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.database.schema import init_db, get_session
from src.etl.extractors import ExtractorFactory
from src.etl.loaders import load_to_database
from src.etl.transformers import transform
from src.validation.validators import format_validation_report, run_all_validations

# ── Configuration ──────────────────────────────────────────────────────────────
DB_PATH = "data/calibration.db"
FILE_PATH = "tests/fixtures/sample_calibration.csv"

# ── Setup ──────────────────────────────────────────────────────────────────────
engine = init_db(DB_PATH)
session = get_session(engine)

# ── Step 1: Extract ────────────────────────────────────────────────────────────
print(f"Extracting: {FILE_PATH}")
df, file_hash = ExtractorFactory.extract(FILE_PATH)
print(f"  Rows extracted: {len(df)}")
print(f"  Columns: {list(df.columns)}")

# ── Step 2: Transform ──────────────────────────────────────────────────────────
print("\nTransforming data...")
df = transform(df, source_file=FILE_PATH)
print(df[["position", "actual_value", "expected_value", "deviation"]].head())

# ── Step 3: Validate ───────────────────────────────────────────────────────────
print("\nRunning validations...")
results = run_all_validations(df, session=session)
print(format_validation_report(results))

# ── Step 4: Load ───────────────────────────────────────────────────────────────
print("Loading to database...")
load_result = load_to_database(
    df=df,
    validation_results=results,
    session=session,
    source_file=FILE_PATH,
    file_hash=file_hash,
)
print(f"  Result: {load_result}")

session.close()
print("\nDone.")
