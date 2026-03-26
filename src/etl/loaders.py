"""Write transformed calibration runs to SQLite with dedup and audit trail."""

import logging
from datetime import datetime
from typing import Optional

import pandas as pd
from sqlalchemy.orm import Session

from src.database.schema import (
    CalibrationMeasurement,
    CalibrationRun,
    FileImport,
    ValidationResult,
)

logger = logging.getLogger(__name__)


def _build_run(df: pd.DataFrame, source_file: str) -> CalibrationRun:
    """Construct a CalibrationRun ORM object from the DataFrame metadata."""
    # Use earliest timestamp in data as the run timestamp
    ts_col = "timestamp"
    if ts_col in df.columns and df[ts_col].notna().any():
        run_ts = df[ts_col].dropna().min()
        if hasattr(run_ts, "to_pydatetime"):
            run_ts = run_ts.to_pydatetime()
    else:
        run_ts = datetime.utcnow()

    equipment_id = "UNKNOWN"
    if "equipment_id" in df.columns and df["equipment_id"].notna().any():
        equipment_id = str(df["equipment_id"].dropna().iloc[0])

    operator = None
    if "operator" in df.columns and df["operator"].notna().any():
        operator = str(df["operator"].dropna().iloc[0])

    return CalibrationRun(
        timestamp=run_ts,
        operator=operator,
        equipment_id=equipment_id,
        source_file=source_file,
    )


def _build_measurements(df: pd.DataFrame, run_id: int) -> list[CalibrationMeasurement]:
    """Convert DataFrame rows to CalibrationMeasurement ORM objects."""
    measurements = []
    required = {"position", "actual_value", "expected_value", "deviation"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"DataFrame missing required columns: {missing}")

    for _, row in df.iterrows():
        ts = row.get("timestamp")
        if pd.isnull(ts):
            ts = None
        elif hasattr(ts, "to_pydatetime"):
            ts = ts.to_pydatetime()

        measurements.append(
            CalibrationMeasurement(
                run_id=run_id,
                position=str(row["position"]),
                actual_value=float(row["actual_value"]),
                expected_value=float(row["expected_value"]),
                deviation=float(row["deviation"]),
                timestamp=ts,
                axis=str(row["axis"]) if "axis" in row and pd.notna(row.get("axis")) else None,
                unit=str(row["unit"]) if "unit" in row and pd.notna(row.get("unit")) else None,
            )
        )
    return measurements


def _store_validation_results(
    validation_results: list[dict],
    run_id: int,
    session: Session,
) -> None:
    """Persist validation check outcomes linked to a run."""
    for result in validation_results:
        vr = ValidationResult(
            run_id=run_id,
            check_type=result.get("check_type", "unknown"),
            status=result.get("status", "unknown"),
            details=result.get("details"),
        )
        session.add(vr)


def load_to_database(
    df: pd.DataFrame,
    validation_results: list[dict],
    session: Session,
    source_file: str,
    file_hash: Optional[str] = None,
    force: bool = False,
) -> dict:
    """Insert a run + measurements into the DB. Returns status dict.
    Blocks on validation failures unless force=True."""
    failures = [r for r in validation_results if r.get("status") == "fail"]
    if failures and not force:
        _record_file_import(session, source_file, file_hash, "failed", 0,
                            f"{len(failures)} validation failure(s)")
        session.commit()
        logger.warning("Load aborted: %d validation failure(s) for '%s'.", len(failures), source_file)
        return {"run_id": None, "rows_loaded": 0, "status": "failed",
                "message": f"{len(failures)} validation failure(s) prevented load."}

    try:
        run = _build_run(df, source_file)
        session.add(run)
        session.flush()

        measurements = _build_measurements(df, run.run_id)
        session.bulk_save_objects(measurements)

        _store_validation_results(validation_results, run.run_id, session)
        _record_file_import(session, source_file, file_hash, "success", len(measurements))

        session.commit()
        logger.info("Loaded %d measurements for run_id=%d from '%s'.",
                    len(measurements), run.run_id, source_file)
        return {"run_id": run.run_id, "rows_loaded": len(measurements),
                "status": "success", "message": "Load successful."}

    except Exception as exc:
        session.rollback()
        _record_file_import(session, source_file, file_hash, "failed", 0, str(exc))
        try:
            session.commit()
        except Exception:
            pass
        logger.exception("Load failed for '%s': %s", source_file, exc)
        return {"run_id": None, "rows_loaded": 0, "status": "failed", "message": str(exc)}


def _record_file_import(
    session: Session,
    filename: str,
    file_hash: Optional[str],
    status: str,
    rows_imported: int,
    error_message: Optional[str] = None,
) -> None:
    fi = FileImport(
        filename=filename,
        file_hash=file_hash,
        status=status,
        rows_imported=rows_imported,
        error_message=error_message,
    )
    session.add(fi)


def check_already_imported(session: Session, file_hash: str) -> bool:
    """Return True if a file with this hash was already successfully imported."""
    result = (
        session.query(FileImport)
        .filter(FileImport.file_hash == file_hash, FileImport.status == "success")
        .first()
    )
    return result is not None
