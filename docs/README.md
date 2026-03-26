# Position Calibration Data Pipeline

A robust ETL pipeline for ingesting, validating, and analyzing equipment
position calibration data from multiple file formats into a SQLite database.

---

## Features

- **Multi-format ingestion**: CSV, Excel (.xlsx/.xls), JSON, and structured log files
- **Automatic column normalization**: Tolerates different column naming conventions
- **Comprehensive validation**: Timestamp consistency, duplicate detection, outlier
  flagging, deviation range checks
- **Audit trail**: Every file import and validation result is recorded in the database
- **Trend analysis**: Mean drift, run-to-run variation, stability (control charts),
  equipment comparison, operator performance
- **Configurable thresholds**: All validation and analysis parameters live in `config.yaml`

---

## Quick Start

### 1. Install dependencies

```bash
cd calibration-pipeline
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Add data files

Copy calibration files into `data/raw/`:

```
data/raw/
├── run_2024_03_01.csv
├── run_2024_03_02.xlsx
└── run_2024_03_03.json
```

### 3. Run the pipeline

```bash
python scripts/run_pipeline.py
```

All files in `data/raw/` will be processed, validated, loaded into
`data/calibration.db`, and archived to `data/archive/`.

### 4. Analyze trends

```bash
python scripts/analyze_trends.py
```

---

## File Format Requirements

All formats must contain (or have aliasable equivalents of) these fields:

| Canonical Name   | Aliases                                              |
|------------------|------------------------------------------------------|
| `timestamp`      | time, date, datetime, measured_at, ts                |
| `position`       | pos, location, point, site, id                       |
| `actual_value`   | actual, measured, measured_value, value, reading     |
| `expected_value` | expected, nominal, target, reference, ref            |
| `operator`       | user, technician, tech, operator_id                  |
| `equipment_id`   | equipment, machine, tool, tool_id, system            |

`deviation` is auto-calculated as `actual_value − expected_value` if absent.

---

## CLI Options

### `scripts/run_pipeline.py`

| Option | Description |
|--------|-------------|
| `--config PATH` | Config file path (default: `config.yaml`) |
| `--file PATH` | Process a single file instead of all files in `data/raw/` |
| `--force` | Load even if validation failures; skip duplicate check |
| `--dry-run` | Extract, transform, and validate without writing to the database |

### `scripts/analyze_trends.py`

| Option | Description |
|--------|-------------|
| `--config PATH` | Config file path (default: `config.yaml`) |
| `--equipment ID` | Drill-down stability analysis for a specific equipment ID |
| `--position POS` | Drill-down run variation for a specific position label |
| `--days N` | Lookback window in days (default from config) |
| `--output PATH` | Write the text report to a file |

---

## Configuration (`config.yaml`)

```yaml
database:
  path: "data/calibration.db"

paths:
  raw: "data/raw"
  processed: "data/processed"
  archive: "data/archive"

validation:
  outlier_threshold: 3.0        # Z-score cutoff for outlier detection
  max_gap_hours: 24             # Maximum allowed gap between timestamps
  duplicate_window_hours: 1     # Window for DB cross-check dedup
  min_measurements_per_run: 5   # Minimum expected measurements per file

analysis:
  default_lookback_days: 30
  alert_drift_threshold: 0.5    # Units of measurement
  stability_window_days: 7
  control_chart_sigma: 3.0
```

---

## Project Structure

```
calibration-pipeline/
├── data/
│   ├── raw/           ← Drop input files here
│   ├── processed/
│   └── archive/       ← Processed files moved here automatically
├── src/
│   ├── database/schema.py      ← SQLAlchemy ORM models + DB init
│   ├── etl/
│   │   ├── extractors.py       ← CSV / Excel / JSON / Log readers
│   │   ├── transformers.py     ← Normalization, deviation calc
│   │   └── loaders.py          ← Transactional DB writer
│   ├── validation/
│   │   └── validators.py       ← All quality checks + report formatter
│   └── analysis/
│       └── metrics.py          ← Drift, variation, stability, diagnostics
├── scripts/
│   ├── run_pipeline.py         ← Main pipeline entry point
│   └── analyze_trends.py       ← Trend analysis entry point
├── tests/
│   ├── fixtures/               ← Sample CSV / JSON / log files
│   ├── test_extractors.py
│   ├── test_transformers.py
│   ├── test_validators.py
│   └── test_pipeline_integration.py
├── examples/
│   ├── 01_basic_import.py
│   ├── 02_custom_validation.py
│   └── 03_trend_analysis.py
├── docs/
│   ├── README.md
│   └── user_guide.md
├── config.yaml
└── requirements.txt
```

---

## Running Tests

```bash
pytest tests/ -v
```

---

## Python Version

Requires Python 3.10+ (uses `match` type hints and `X | Y` union syntax).
