"""Drift, variation, and stability analysis against the calibration DB."""

import logging
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

from src.database.schema import CalibrationMeasurement, CalibrationRun

logger = logging.getLogger(__name__)



def _measurements_df(session: Session, run_id: Optional[int] = None) -> pd.DataFrame:
    """Load measurements into a DataFrame, optionally filtered by run_id."""
    query = session.query(
        CalibrationMeasurement.measurement_id,
        CalibrationMeasurement.run_id,
        CalibrationMeasurement.position,
        CalibrationMeasurement.actual_value,
        CalibrationMeasurement.expected_value,
        CalibrationMeasurement.deviation,
        CalibrationMeasurement.timestamp,
        CalibrationMeasurement.axis,
        CalibrationMeasurement.unit,
        CalibrationRun.equipment_id,
        CalibrationRun.operator,
    ).join(CalibrationRun)

    if run_id is not None:
        query = query.filter(CalibrationMeasurement.run_id == run_id)

    rows = query.all()
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=[
        "measurement_id", "run_id", "position", "actual_value",
        "expected_value", "deviation", "timestamp", "axis", "unit",
        "equipment_id", "operator",
    ])
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df



def calculate_mean_drift(run_id: int, session: Session) -> dict:
    """Per-position mean deviation and linear drift slope for a single run."""
    df = _measurements_df(session, run_id=run_id)
    if df.empty:
        logger.warning("No measurements found for run_id=%d.", run_id)
        return {"run_id": run_id, "per_position": pd.DataFrame(),
                "drift_slope": None, "drift_direction": "unknown"}

    per_position = (
        df.groupby("position")["deviation"]
        .agg(mean_dev="mean", n_measurements="count")
        .reset_index()
        .sort_values("mean_dev", ascending=False)
    )

    df_sorted = df.sort_values("timestamp").reset_index(drop=True)
    if len(df_sorted) >= 2:
        x = np.arange(len(df_sorted))
        slope = float(np.polyfit(x, df_sorted["deviation"].values, 1)[0])
        direction = "positive" if slope > 0 else "negative" if slope < 0 else "flat"
    else:
        slope = None
        direction = "insufficient data"

    logger.info("Mean drift for run %d: slope=%.6f (%s).", run_id, slope or 0, direction)
    return {
        "run_id": run_id,
        "per_position": per_position,
        "drift_slope": slope,
        "drift_direction": direction,
    }



def calculate_run_variation(
    position: str,
    session: Session,
    lookback_days: int = 30,
) -> dict:
    """Run-to-run deviation stats for a position over the lookback window."""
    cutoff = datetime.utcnow() - timedelta(days=lookback_days)
    df = _measurements_df(session)
    if df.empty:
        return {}

    df = df[(df["position"] == position.upper()) & (df["timestamp"] >= cutoff)]
    if df.empty:
        logger.warning("No data for position '%s' in last %d days.", position, lookback_days)
        return {"position": position, "n_runs": 0}

    series = (
        df.groupby("run_id")
        .agg(deviation=("deviation", "mean"), timestamp=("timestamp", "min"))
        .reset_index()
        .sort_values("timestamp")
    )

    devs = series["deviation"].values
    mean = float(devs.mean())
    std = float(devs.std()) if len(devs) > 1 else 0.0
    cv = (std / abs(mean) * 100) if mean != 0 else float("inf")

    result = {
        "position": position,
        "n_runs": len(series),
        "mean": mean,
        "std": std,
        "cv": cv,
        "range": float(devs.max() - devs.min()),
        "series": series,
    }
    logger.info("Run variation for position '%s': mean=%.4f, std=%.4f.", position, mean, std)
    return result



def assess_stability(
    equipment_id: str,
    session: Session,
    lookback_days: int = 30,
    alert_threshold: float = 0.5,
    sigma: float = 3.0,
) -> dict:
    """Xbar control chart (mean, UCL/LCL at ±sigma) for one piece of equipment."""
    df = _measurements_df(session)
    if df.empty:
        return {}

    cutoff = datetime.utcnow() - timedelta(days=lookback_days)
    df = df[(df["equipment_id"] == equipment_id) & (df["timestamp"] >= cutoff)]
    if df.empty:
        logger.warning("No data for equipment '%s' in last %d days.", equipment_id, lookback_days)
        return {"equipment_id": equipment_id, "alert": False}

    devs = df.sort_values("timestamp")["deviation"].values
    mean = float(devs.mean())
    std = float(devs.std()) if len(devs) > 1 else 0.0
    ucl = mean + sigma * std
    lcl = mean - sigma * std
    out_of_control = int(((devs > ucl) | (devs < lcl)).sum())

    # Trend: compare first half vs second half mean
    mid = len(devs) // 2
    if mid >= 2:
        first_mean = devs[:mid].mean()
        second_mean = devs[mid:].mean()
        delta = second_mean - first_mean
        trend = "degrading" if abs(delta) > 0.01 and delta > 0 else \
                "improving" if abs(delta) > 0.01 else "stable"
    else:
        trend = "insufficient data"

    alert = (abs(mean) > alert_threshold) or (out_of_control > 0)

    result = {
        "equipment_id": equipment_id,
        "mean": mean,
        "std": std,
        "ucl": ucl,
        "lcl": lcl,
        "out_of_control": out_of_control,
        "trend": trend,
        "alert": alert,
        "series": df[["run_id", "timestamp", "deviation"]].sort_values("timestamp"),
    }
    logger.info(
        "Stability for equipment '%s': mean=%.4f, OOC=%d, trend=%s, alert=%s.",
        equipment_id, mean, out_of_control, trend, alert,
    )
    return result



def worst_performing_positions(session: Session, top_n: int = 10) -> pd.DataFrame:
    """Return positions with the highest mean absolute deviation."""
    df = _measurements_df(session)
    if df.empty:
        return pd.DataFrame()

    return (
        df.groupby("position")["deviation"]
        .agg(mean_abs_dev=lambda x: x.abs().mean(), n=len)
        .reset_index()
        .sort_values("mean_abs_dev", ascending=False)
        .head(top_n)
    )


def equipment_comparison(session: Session) -> pd.DataFrame:
    """Compare mean deviation and stability across equipment."""
    df = _measurements_df(session)
    if df.empty:
        return pd.DataFrame()

    return (
        df.groupby("equipment_id")["deviation"]
        .agg(mean_dev="mean", std_dev="std", n_measurements="count")
        .reset_index()
        .sort_values("mean_dev", key=abs, ascending=False)
    )


def operator_performance(session: Session) -> pd.DataFrame:
    """Summarize deviation statistics grouped by operator."""
    df = _measurements_df(session)
    if df.empty:
        return pd.DataFrame()

    return (
        df.groupby("operator")["deviation"]
        .agg(mean_dev="mean", std_dev="std", n_measurements="count")
        .reset_index()
        .sort_values("std_dev", ascending=False)
    )


def time_based_trends(session: Session, freq: str = "D") -> pd.DataFrame:
    """
    Aggregate mean deviation over time periods.

    Args:
        freq: pandas offset alias — 'D' (daily), 'W' (weekly), 'ME' (monthly end).
    """
    df = _measurements_df(session)
    if df.empty or "timestamp" not in df.columns:
        return pd.DataFrame()

    df = df.set_index("timestamp").sort_index()
    return (
        df["deviation"]
        .resample(freq)
        .agg(mean_dev="mean", std_dev="std", n_measurements="count")
        .dropna(subset=["mean_dev"])
        .reset_index()
    )


def generate_summary_report(session: Session, lookback_days: int = 30) -> dict:
    """Produce a top-level summary dict for the analysis script."""
    return {
        "worst_positions":    worst_performing_positions(session),
        "equipment_summary":  equipment_comparison(session),
        "operator_summary":   operator_performance(session),
        "daily_trend":        time_based_trends(session, freq="D"),
    }
