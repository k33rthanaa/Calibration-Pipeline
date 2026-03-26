#!/usr/bin/env python3
"""Process files in data/raw/ through the calibration pipeline."""

import argparse
import logging
import shutil
import sys
from pathlib import Path

import yaml

# Allow running from project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.database.schema import init_db, get_session
from src.etl.extractors import ExtractorFactory, SUPPORTED_EXTENSIONS
from src.etl.loaders import check_already_imported, load_to_database
from src.etl.transformers import transform
from src.validation.validators import format_validation_report, run_all_validations


def setup_logging(config: dict) -> None:
    log_cfg = config.get("logging", {})
    level = getattr(logging, log_cfg.get("level", "INFO").upper(), logging.INFO)
    fmt = log_cfg.get("format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    handlers = [logging.StreamHandler(sys.stdout)]
    log_file = log_cfg.get("file")
    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(level=level, format=fmt, handlers=handlers)


def load_config(config_path: str) -> dict:
    with open(config_path) as f:
        return yaml.safe_load(f)


def collect_files(raw_dir: str, single_file: str | None) -> list[Path]:
    """Return list of files to process."""
    if single_file:
        p = Path(single_file)
        if not p.exists():
            logging.error("File not found: %s", single_file)
            sys.exit(1)
        return [p]

    raw_path = Path(raw_dir)
    if not raw_path.exists():
        logging.warning("Raw data directory does not exist: %s", raw_dir)
        return []

    files = [
        f for f in raw_path.iterdir()
        if f.is_file() and f.suffix.lower() in SUPPORTED_EXTENSIONS
    ]
    files.sort(key=lambda f: f.stat().st_mtime)
    return files


def archive_file(filepath: Path, archive_dir: str) -> None:
    archive_path = Path(archive_dir)
    archive_path.mkdir(parents=True, exist_ok=True)
    dest = archive_path / filepath.name
    # Avoid overwriting by appending a suffix
    if dest.exists():
        dest = archive_path / f"{filepath.stem}_{int(filepath.stat().st_mtime)}{filepath.suffix}"
    shutil.move(str(filepath), str(dest))
    logging.info("Archived '%s' -> '%s'.", filepath.name, dest)


def process_file(
    filepath: Path,
    session,
    config: dict,
    dry_run: bool,
    force: bool,
) -> dict:
    """Extract, transform, validate, and load a single file. Returns a status dict."""
    logger = logging.getLogger(__name__)
    result = {"file": filepath.name, "status": "unknown", "rows": 0, "run_id": None}

    # 1. Extract
    try:
        df, file_hash = ExtractorFactory.extract(str(filepath))
    except Exception as exc:
        logger.error("Extraction failed for '%s': %s", filepath.name, exc)
        result["status"] = "extract_error"
        result["message"] = str(exc)
        return result

    # 2. Dedup check
    if not force and check_already_imported(session, file_hash):
        logger.info("Skipping '%s': already imported (hash match).", filepath.name)
        result["status"] = "skipped_duplicate"
        return result

    # 3. Transform
    val_cfg = config.get("validation", {})
    try:
        df = transform(df, source_file=filepath.name)
    except Exception as exc:
        logger.error("Transform failed for '%s': %s", filepath.name, exc)
        result["status"] = "transform_error"
        result["message"] = str(exc)
        return result

    # 4. Validate
    validation_results = run_all_validations(df, session=session, config=val_cfg)
    report = format_validation_report(validation_results)
    print(report)

    if dry_run:
        logger.info("[DRY RUN] Skipping database load for '%s'.", filepath.name)
        result["status"] = "dry_run"
        result["rows"] = len(df)
        return result

    # 5. Load
    load_result = load_to_database(
        df=df,
        validation_results=validation_results,
        session=session,
        source_file=filepath.name,
        file_hash=file_hash,
        force=force,
    )
    result.update(load_result)
    result["file"] = filepath.name
    return result


def main():
    parser = argparse.ArgumentParser(description="Calibration Data Pipeline")
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    parser.add_argument("--file", default=None, help="Process a single file instead of data/raw/")
    parser.add_argument("--force", action="store_true",
                        help="Load even if validation failures; skip duplicate check")
    parser.add_argument("--dry-run", action="store_true",
                        help="Extract, transform, and validate without writing to DB")
    args = parser.parse_args()

    config = load_config(args.config)
    setup_logging(config)
    logger = logging.getLogger(__name__)

    db_path = config["database"]["path"]
    raw_dir = config["paths"]["raw"]
    archive_dir = config["paths"]["archive"]

    # Ensure directories exist
    for d in [raw_dir, config["paths"]["processed"], archive_dir]:
        Path(d).mkdir(parents=True, exist_ok=True)

    engine = init_db(db_path)
    session = get_session(engine)

    files = collect_files(raw_dir, args.file)
    if not files:
        logger.info("No files to process.")
        sys.exit(0)

    logger.info("Pipeline starting: %d file(s) to process.", len(files))
    summary = []

    for filepath in files:
        logger.info("--- Processing: %s ---", filepath.name)
        result = process_file(filepath, session, config, args.dry_run, args.force)
        summary.append(result)

        # Archive successfully loaded files
        if result.get("status") == "success" and not args.dry_run and args.file is None:
            archive_file(filepath, archive_dir)

    session.close()

    # Print summary
    print("\n" + "=" * 60)
    print("PIPELINE SUMMARY")
    print("=" * 60)
    for r in summary:
        print(f"  {r['file']:<40} status={r['status']}  rows={r.get('rows_loaded', r.get('rows', 0))}")
    print("=" * 60)

    success_count = sum(1 for r in summary if r.get("status") == "success")
    fail_count = sum(1 for r in summary if r.get("status") in ("failed", "extract_error", "transform_error"))
    logger.info("Done: %d succeeded, %d failed, %d total.", success_count, fail_count, len(summary))

    sys.exit(1 if fail_count > 0 else 0)


if __name__ == "__main__":
    main()
