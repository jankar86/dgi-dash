# dgi-dash

CSV-to-SQLite ingestion utilities for dividend transaction tracking.

## What this repo does
- Creates and migrates a local SQLite database (`dividends.db`)
- Imports CSV exports from multiple sources (E*TRADE, Fidelity, generic, historical)
- Normalizes transaction types (including `REINVESTMENT`)
- Provides coverage and month-gap reporting for data quality checks

## Project layout
- `cli.py`: single entrypoint for setup, import, and reports
- `workflows.py`: orchestration and migration helpers
- `models.py`: SQLAlchemy models
- `import_utils.py`: shared import/upsert utilities
- `import_csv.py`: generic CSV importer
- `import_etrade_csv.py`: E*TRADE importer
- `import_fidelity_csv.py`: Fidelity importer
- `import_hist_csv.py`: historical importer
- `coverage_report.py`: coverage and gap analysis reports
- `data/`: local CSV input folders (ignored by git)
- `backups/`: SQL dump snapshots (ignored by git)

## Setup
```bash
python3 -m venv venv
venv/bin/pip install -r requirements.txt
venv/bin/python cli.py setup-db
```

## Import workflows
```bash
# Current drop-folder import
venv/bin/python cli.py import-current

# Archive reviewed current files into archived storage
venv/bin/python cli.py archive-processed --dry-run

# Archived/bootstrap import
venv/bin/python cli.py import-all

# Generic file
venv/bin/python cli.py import --source generic --path your_brokerage_dump.csv

# E*TRADE (single file or folder)
venv/bin/python cli.py import --source etrade --path data/archived/etrade

# Fidelity (single file or folder)
venv/bin/python cli.py import --source fidelity --path data/archived/fidelity

# Historical
venv/bin/python cli.py import --source historical --path data/archived/historical_divs.csv
```

`import-current` is the recommended day-to-day ingestion entrypoint:
- runs `setup-db` first unless `--skip-setup` is passed
- imports only current Fidelity and E*TRADE drop-folder data
- defaults to `data/fidelity` and `data/etrade`
- skips a source cleanly if the folder is missing or empty

Example:
```bash
venv/bin/python cli.py import-current
venv/bin/python cli.py import-current --fidelity-path /mounted/fidelity --etrade-path /mounted/etrade
```

`archive-processed` is the explicit post-review archive step:
- moves `.csv` files from `data/fidelity` to `data/archived/fidelity`
- moves `.csv` files from `data/etrade` to `data/archived/etrade`
- preserves subdirectories when present
- refuses to overwrite an existing archived file
- supports `--dry-run` so you can inspect the move set first

Example:
```bash
venv/bin/python cli.py archive-processed --dry-run
venv/bin/python cli.py archive-processed
```

`import-all` is the archived/bootstrap ingestion entrypoint:
- runs `setup-db` first unless `--skip-setup` is passed
- imports historical data first, then Fidelity, then E*TRADE
- auto-discovers the local archived source paths by default
- accepts `--historical-path`, `--fidelity-path`, and `--etrade-path` overrides when needed

Example:
```bash
venv/bin/python cli.py import-all
venv/bin/python cli.py import-all --skip-setup
```

Legacy wrappers still work:
- `venv/bin/python db_setup.py`
- `venv/bin/python import_csv.py`
- `venv/bin/python import_etrade_csv.py`
- `venv/bin/python import_fidelity_csv.py`
- `venv/bin/python import_hist_csv.py`

## Fidelity filename requirements
- Old-format Fidelity CSVs include an `account` column, and the importer extracts the account digits from that column.
- New-format Fidelity CSVs do not include an `account` column, so the importer requires the filename to contain 4 or more account digits immediately before `-fidelity`.
- Accepted examples: `4217-fidelity-2025.csv`, `224294217-fidelity.csv`
- Rejected examples: `fidelity-4217.csv`, `acct4217.csv`, `broker_export.csv`
- New-format Fidelity files that do not follow this pattern are rejected and are not imported.

## Reporting
```bash
# Latest transaction date per account
venv/bin/python cli.py report-coverage

# Coverage as of a specific date
venv/bin/python cli.py report-coverage --as-of 2026-03-23

# Month-level gap report
venv/bin/python cli.py report-gaps

# Compare manual ledger against DB
venv/bin/python cli.py report-manual-compare --path data/manual/dividend_log.csv

# Import manual-ledger interest rows into a dedicated account
venv/bin/python cli.py import-manual-interest --path reports/dividend_history_manual_reconciliation_manual_interest_ignored.csv
```

The manual compare report:
- accepts CSV files directly
- accepts Excel files when `openpyxl` is installed in the local `venv`
- expects columns for date, security, and amount
- ignores manual rows whose security contains `interest`
- fuzzy-matches on security plus date and amount tolerances
- writes summary and unmatched-row CSVs under `reports/`

Manual interest import:
- imports interest rows into the dedicated account `MANUAL-INTEREST` by default
- uses the existing `INTEREST` security ticker in the DB
- dedupes through the normal transaction upsert path
- accepts either a simple manual ledger file or the generated `*_manual_interest_ignored.csv` report
- if you point it at an Excel workbook, Excel support still requires `openpyxl` in the local `venv`
- existing legacy `INTEREST` rows are migrated into `MANUAL-INTEREST` during `setup-db`

## Testing
```bash
venv/bin/pytest -q
```

## Backup and restore
Create SQL snapshot:
```bash
venv/bin/python - <<'PY'
import sqlite3
from datetime import datetime
from pathlib import Path

db = Path("dividends.db")
out = Path("backups") / f"dividends_snapshot_{datetime.now():%Y%m%d_%H%M%S}.sql"
out.parent.mkdir(parents=True, exist_ok=True)
conn = sqlite3.connect(str(db))
with out.open("w", encoding="utf-8") as f:
    for line in conn.iterdump():
        f.write(line + "\n")
conn.close()
print(out)
PY
```

Restore from snapshot:
```bash
sqlite3 dividends_restored.db < backups/dividends_snapshot_YYYYMMDD_HHMMSS.sql
```

## Data and privacy notes
- Raw CSV files are ignored by git (`data/**/*.csv`)
- Local DB file is ignored by git (`dividends.db`)
- SQL snapshots are ignored by git (`backups/*.sql`)
