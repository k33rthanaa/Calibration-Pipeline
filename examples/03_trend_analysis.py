#!/usr/bin/env python3
"""
Example 03: Trend analysis after importing data.

Runs the full analysis suite and prints results.
Requires data to already be loaded in the database.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.analysis.metrics import (
    assess_stability,
    calculate_mean_drift,
    calculate_run_variation,
    equipment_comparison,
    generate_summary_report,
    time_based_trends,
    worst_performing_positions,
)
from src.database.schema import CalibrationRun, get_session, init_db

DB_PATH = "data/calibration.db"

engine = init_db(DB_PATH)
session = get_session(engine)

# ── Check if any runs exist ─────────────────────────────────────────────────────
total_runs = session.query(CalibrationRun).count()
if total_runs == 0:
    print("No data in database. Run example 01 first:")
    print("  python examples/01_basic_import.py")
    session.close()
    sys.exit(0)

print(f"Analyzing {total_runs} calibration run(s).\n")

# ── Worst positions ─────────────────────────────────────────────────────────────
print("=== Top 5 Worst Performing Positions ===")
worst = worst_performing_positions(session, top_n=5)
print(worst.to_string(index=False) if not worst.empty else "No data.")

# ── Equipment comparison ────────────────────────────────────────────────────────
print("\n=== Equipment Comparison ===")
equip = equipment_comparison(session)
print(equip.to_string(index=False) if not equip.empty else "No data.")

# ── Mean drift for each run ─────────────────────────────────────────────────────
print("\n=== Mean Drift Per Run ===")
runs = session.query(CalibrationRun).order_by(CalibrationRun.timestamp).all()
for run in runs:
    drift = calculate_mean_drift(run.run_id, session)
    slope = drift.get("drift_slope")
    direction = drift.get("drift_direction", "unknown")
    print(f"  Run {run.run_id} | {run.equipment_id} | slope={slope:.6f} ({direction})"
          if slope is not None else f"  Run {run.run_id}: insufficient data")

# ── Stability for first equipment found ────────────────────────────────────────
if not equip.empty:
    eq_id = equip["equipment_id"].iloc[0]
    print(f"\n=== Stability Analysis: {eq_id} ===")
    stability = assess_stability(eq_id, session, lookback_days=365)
    for k, v in stability.items():
        if k != "series":
            print(f"  {k}: {v}")

# ── Run variation for first position ───────────────────────────────────────────
if not worst.empty:
    pos = worst["position"].iloc[0]
    print(f"\n=== Run Variation: {pos} ===")
    var = calculate_run_variation(pos, session, lookback_days=365)
    for k, v in var.items():
        if k != "series":
            print(f"  {k}: {v}")

# ── Daily trend ─────────────────────────────────────────────────────────────────
print("\n=== Daily Deviation Trend ===")
daily = time_based_trends(session, freq="D")
print(daily.to_string(index=False) if not daily.empty else "No data.")

session.close()
print("\nAnalysis complete.")
