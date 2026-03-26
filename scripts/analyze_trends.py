#!/usr/bin/env python3
"""Query the calibration DB and print drift/stability analysis."""

import argparse
import logging
import sys
from pathlib import Path

import yaml

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
from src.database.schema import CalibrationRun, get_session, init_db


def load_config(config_path: str) -> dict:
    with open(config_path) as f:
        return yaml.safe_load(f)


def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def section(title: str) -> str:
    return f"\n{'=' * 60}\n{title}\n{'=' * 60}"


def df_to_text(df, max_rows: int = 20) -> str:
    if df is None or df.empty:
        return "  (no data)"
    return df.head(max_rows).to_string(index=False)


def run_analysis(session, config: dict, args: argparse.Namespace) -> str:
    analysis_cfg = config.get("analysis", {})
    lookback = args.days or analysis_cfg.get("default_lookback_days", 30)
    alert_threshold = analysis_cfg.get("alert_drift_threshold", 0.5)
    sigma = analysis_cfg.get("control_chart_sigma", 3.0)

    lines = ["CALIBRATION TREND ANALYSIS REPORT", f"Lookback: {lookback} days"]

    # ── Summary report ─────────────────────────────────────────────────
    lines.append(section("TOP 10 WORST PERFORMING POSITIONS"))
    worst = worst_performing_positions(session, top_n=10)
    lines.append(df_to_text(worst))

    lines.append(section("EQUIPMENT COMPARISON"))
    equip = equipment_comparison(session)
    lines.append(df_to_text(equip))

    lines.append(section("OPERATOR PERFORMANCE"))
    ops = operator_performance(session)
    lines.append(df_to_text(ops))

    lines.append(section("DAILY DEVIATION TREND"))
    daily = time_based_trends(session, freq="D")
    lines.append(df_to_text(daily, max_rows=30))

    # ── Equipment-specific stability ────────────────────────────────────
    if args.equipment:
        lines.append(section(f"STABILITY ANALYSIS: {args.equipment}"))
        stability = assess_stability(
            equipment_id=args.equipment,
            session=session,
            lookback_days=lookback,
            alert_threshold=alert_threshold,
            sigma=sigma,
        )
        if stability:
            for k, v in stability.items():
                if k == "series":
                    lines.append(f"  series:\n{df_to_text(v)}")
                else:
                    lines.append(f"  {k}: {v}")
        else:
            lines.append("  No data for this equipment.")

    # ── Position-specific run variation ─────────────────────────────────
    if args.position:
        lines.append(section(f"RUN VARIATION: position={args.position}"))
        var = calculate_run_variation(args.position, session, lookback_days=lookback)
        if var:
            for k, v in var.items():
                if k == "series":
                    lines.append(f"  series:\n{df_to_text(v)}")
                else:
                    lines.append(f"  {k}: {v}")
        else:
            lines.append("  No data for this position.")

    # ── Per-run mean drift for recent runs ──────────────────────────────
    lines.append(section("PER-RUN MEAN DRIFT (most recent 5 runs)"))
    recent_runs = session.query(CalibrationRun).order_by(
        CalibrationRun.timestamp.desc()
    ).limit(5).all()

    if not recent_runs:
        lines.append("  No runs found in database.")
    else:
        for run in recent_runs:
            drift = calculate_mean_drift(run.run_id, session)
            lines.append(
                f"\n  Run {run.run_id} | equipment={run.equipment_id} | "
                f"ts={run.timestamp} | slope={drift.get('drift_slope', 'N/A')} "
                f"({drift.get('drift_direction', 'N/A')})"
            )
            pp = drift.get("per_position")
            if pp is not None and not pp.empty:
                lines.append(df_to_text(pp))

    # ── Alerts ──────────────────────────────────────────────────────────
    lines.append(section("ALERTS"))
    equip_df = equipment_comparison(session)
    if not equip_df.empty:
        alerts = equip_df[equip_df["mean_dev"].abs() > alert_threshold]
        if alerts.empty:
            lines.append("  No equipment exceeds drift alert threshold.")
        else:
            lines.append(f"  *** {len(alerts)} equipment unit(s) exceed ±{alert_threshold} mean deviation:")
            lines.append(df_to_text(alerts))
    else:
        lines.append("  No data to check.")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Calibration Trend Analysis")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--equipment", default=None, help="Equipment ID for stability drill-down")
    parser.add_argument("--position", default=None, help="Position label for run-variation drill-down")
    parser.add_argument("--days", type=int, default=None, help="Lookback window in days")
    parser.add_argument("--output", default=None, help="Write report to this file path")
    args = parser.parse_args()

    config = load_config(args.config)
    setup_logging(config.get("logging", {}).get("level", "INFO"))

    engine = init_db(config["database"]["path"])
    session = get_session(engine)

    report = run_analysis(session, config, args)
    session.close()

    print(report)

    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(report)
        logging.info("Report written to %s", args.output)


if __name__ == "__main__":
    main()
