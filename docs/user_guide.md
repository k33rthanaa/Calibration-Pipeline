# User Guide — Position Calibration Data Pipeline

---

## 1. Preparing Your Data

### Supported formats

| Format | Extension | Notes |
|--------|-----------|-------|
| CSV | `.csv` | Any delimiter auto-detected by pandas |
| Excel | `.xlsx`, `.xls` | First non-empty sheet is used |
| JSON | `.json` | Top-level list, or dict with a `data`/`measurements`/`records` key |
| Structured log | `.log`, `.txt` | Lines with `KEY=VALUE` pairs; lines starting with `#` are skipped |

### Minimum required fields

Your file must contain (or name-aliasable equivalents of):

- **position** — identifier for the measurement location
- **actual_value** — the measured value
- **expected_value** — the nominal/reference value

`deviation` is computed automatically if missing. `timestamp`, `operator`, and
`equipment_id` are strongly recommended for full analysis capability.

### Example CSV

```csv
timestamp,position,actual_value,expected_value,operator,equipment_id
2024-03-01 08:00:00,POS_A1,10.002,10.000,OP001,TOOL_X1
2024-03-01 08:01:00,POS_A2,9.998,10.000,OP001,TOOL_X1
```

### Example log file

```
# My calibration log
[2024-03-01 08:00:00] POSITION=POS_A1 ACTUAL=10.002 EXPECTED=10.000 EQUIPMENT=TOOL_X1 OPERATOR=OP001
[2024-03-01 08:01:00] POSITION=POS_A2 ACTUAL=9.998 EXPECTED=10.000 EQUIPMENT=TOOL_X1 OPERATOR=OP001
```

---

## 2. Running the Pipeline

### Process all files in `data/raw/`

```bash
python scripts/run_pipeline.py
```

### Process a single file

```bash
python scripts/run_pipeline.py --file path/to/your_file.csv
```

### Dry run (no database writes)

```bash
python scripts/run_pipeline.py --dry-run
```

### Force load despite validation failures

```bash
python scripts/run_pipeline.py --force
```

---

## 3. Interpreting Validation Reports

Each check produces one of three statuses:

| Symbol | Status | Meaning |
|--------|--------|---------|
| ✓ | **pass** | Check passed with no issues |
| ⚠ | **warn** | Potential issue; load proceeds by default |
| ✗ | **fail** | Blocking issue; load is aborted unless `--force` is used |

### Checks performed

| Check | Fail condition |
|-------|---------------|
| `required_columns` | Missing position, actual_value, expected_value, or deviation |
| `row_count` | Fewer rows than `min_measurements_per_run` |
| `missing_measurements` | Null values in measurement columns |
| `timestamp_consistency` | Non-chronological order or gaps > `max_gap_hours` |
| `duplicate_detection` | Identical rows within file or overlapping time window in DB |
| `outlier_detection` | > 20% of deviations are statistical outliers (Z-score + IQR) |
| `deviation_range` | Any absolute deviation exceeds `alert_drift_threshold` |

---

## 4. Understanding Trend Metrics

### Mean Drift (`calculate_mean_drift`)

Reports the average deviation per position within a single run, and a linear
slope across the run's measurement sequence.

- **Positive slope** → deviations trending upward over the run
- **Negative slope** → deviations trending downward
- **Flat** → no systematic drift within the run

### Run-to-Run Variation (`calculate_run_variation`)

For a single position across multiple runs, reports:

- `mean` — average deviation across runs
- `std` — standard deviation (spread)
- `cv` — coefficient of variation (std / mean × 100%) — a normalized measure of variability
- `range` — max minus min deviation seen

### Stability / Control Chart (`assess_stability`)

Uses 3-sigma control limits (configurable):

- **UCL** = mean + σ × std
- **LCL** = mean − σ × std
- **out_of_control** — count of measurements outside these limits
- **trend** — compares first half vs second half mean deviation:
  - `improving` → second half mean closer to zero
  - `degrading` → second half mean further from zero
  - `stable` → no significant change

---

## 5. Extension Guidelines

### Adding a new file format

1. Create a new class in `src/etl/extractors.py` inheriting from `BaseExtractor`
2. Implement the `extract(filepath) -> pd.DataFrame` method
3. Register the extension in `ExtractorFactory._MAP`

### Adding a new validation check

1. Write a function in `src/validation/validators.py` returning a result dict
   using `_pass()`, `_warn()`, or `_fail()`
2. Add it to the `run_all_validations()` function

### Adding a new metric

1. Write a function in `src/analysis/metrics.py` that accepts a `session` parameter
2. Optionally add it to `generate_summary_report()`

---

## 6. Troubleshooting

| Problem | Likely cause | Fix |
|---------|-------------|-----|
| `Unsupported file extension` | File type not recognized | Rename file or add extractor |
| `Missing columns: {'position'}` | Column name not in aliases | Rename column or add alias to `COLUMN_ALIASES` |
| `Load aborted: N validation failure(s)` | Data quality issues | Review validation report; use `--force` if intentional |
| `Already imported (hash match)` | Same file imported twice | Use `--force` to re-import |
| `All timestamps are null` | Timestamp format not parseable | Add format to `_TS_PATTERNS` in `LogExtractor` or check CSV format |
