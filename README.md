# dgi-dash

CSV-to-SQLite ingestion utilities for dividend transaction tracking.

## What this repo does
- Creates and migrates a local SQLite database (`dividends.db`)
- Imports CSV exports from multiple sources (E*TRADE, Fidelity, generic, historical)
- Normalizes transaction types (including `REINVESTMENT`)
- Provides coverage and month-gap reporting for data quality checks

## Project layout
- `cli.py`: single entrypoint for setup, import, reports, and the web UI
- `db/`: database models and shared DB/session utilities
- `ingest/`: importers plus ingestion/archive workflows
- `reporting/`: coverage and reconciliation/reporting logic
- `webapp/`: read-only dashboard UI and query layer
- `data/`: local CSV input folders (ignored by git)
- `backups/`: SQL dump snapshots (ignored by git)

## Structure guide
- `db/models.py`: SQLAlchemy schema and enums
- `db/utils.py`: session creation, validation guards, and transaction upsert logic
- `ingest/generic.py`: generic CSV importer
- `ingest/fidelity.py`: Fidelity-specific parsing/import logic
- `ingest/etrade.py`: E*TRADE-specific parsing/import logic
- `ingest/historical.py`: archived historical import logic
- `ingest/workflows.py`: `setup-db`, `import-current`, `import-all`, and archive orchestration
- `reporting/coverage.py`: coverage and gap analysis
- `reporting/manual_compare.py`: manual-ledger reconciliation and manual-interest import
- `webapp/dashboard_data.py`: read/query layer for the dashboard
- `webapp/dashboard_app.py`: WSGI app and HTML rendering

## Setup
```bash
python3 -m venv venv
venv/bin/pip install -r requirements.txt
venv/bin/python cli.py setup-db
```

## Gmail Integration
The Gmail integration is read-only in this first pass. It authenticates with Gmail using OAuth,
reads messages from a specific label/folder, and stages message metadata into `reports/` for review.

Recommended secret file locations:
- `secrets/google_credentials.json`
- `secrets/google_token.json`

One-time auth:
```bash
venv/bin/python cli.py gmail-auth \
  --credentials-path secrets/google_credentials.json \
  --token-path secrets/google_token.json
```

Stage messages from a Gmail label:
```bash
venv/bin/python cli.py import-gmail \
  --label "Dividend Alerts" \
  --query "is:unread newer_than:30d" \
  --max-results 50 \
  --credentials-path secrets/google_credentials.json \
  --token-path secrets/google_token.json
```

This writes review artifacts under `reports/`:
- `gmail_<label>_<timestamp>.csv`
- `gmail_<label>_<timestamp>.jsonl`

Notes:
- scope is Gmail read-only only
- OAuth credentials and token files are ignored by git
- this does not import anything into the DB yet
- the next step after review is to add a parser for your specific dividend email format

### Simpler Gmail option: IMAP + app password
If you want a simpler setup first, you can use Gmail IMAP with an app password instead of OAuth.

Google notes that app passwords are not recommended and require 2-Step Verification:
- https://support.google.com/mail/answer/1173270?hl=en
- https://support.google.com/accounts/answer/2461835?hl=en

Put the app password in:
- `secrets/gmail_app_password.txt`

Then stage messages from a Gmail mailbox/folder:
```bash
venv/bin/python cli.py import-gmail-imap \
  --username yourname@gmail.com \
  --mailbox "Dividend Alerts" \
  --search 'ALL' \
  --max-results 50 \
  --include-body
```

You can also use Gmail raw search through IMAP on Gmail accounts, for example:
```bash
venv/bin/python cli.py import-gmail-imap \
  --username yourname@gmail.com \
  --mailbox "[Gmail]/All Mail" \
  --search 'X-GM-RAW "label:Dividend-Alerts newer_than:30d"' \
  --max-results 50 \
  --include-body
```

Import the reviewed parsed Gmail transactions into the DB:
```bash
venv/bin/python cli.py import-gmail-parsed \
  --path reports/gmail_imap_gmail_all_mail_YYYYMMDD_HHMMSS_parsed_transactions.csv
```

Notes:
- this uses the payment date extracted from the email body, not the email received timestamp
- by default it skips parsed `INTEREST` rows
- if a row already exists with the same account/security/amount on a different date, the importer updates the DB row to the email payment date

## Web UI
```bash
# Local read-only dashboard
venv/bin/python cli.py serve-web --host 127.0.0.1 --port 8000
```

Available routes:
- `/`: dashboard
- `/transactions`: filterable transaction table
- `/api/summary`: JSON dashboard payload
- `/api/transactions`: JSON transaction payload

The web layer is intentionally read-only and uses the existing SQLite database by default.
You can override the DB target with `--db-url` or `DATABASE_URL`.

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

The supported entrypoint is `venv/bin/python cli.py ...`.

## Fidelity filename requirements
- Fidelity imports now require an `Account Number` column in the CSV itself.
- Account assignment is derived per row from that `Account Number` value, so one CSV can safely contain transactions for multiple Fidelity accounts.
- The importer does not infer Fidelity account numbers from filenames anymore.
- If a Fidelity dividend/reinvestment row is missing a usable `Account Number`, the import fails with an explicit error instead of guessing.

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

Broker account naming:
- E*TRADE accounts are normalized to short aliases like `etr-1445`
- Fidelity accounts are normalized to short aliases like `fid-4217`
- `setup-db` migrates older `ETRADE-*` and `FIDELITY-*` account names into the short alias format

Account metadata:
- accounts now carry optional metadata such as display name, institution, last-4, account group, tax treatment, active status, and notes
- `setup-db` backfills this metadata for the known broker, manual, and legacy account patterns
- current tax designations use user-facing values such as `Taxable`, `Roth IRA`, and `Traditional IRA`

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

## Container
Build:
```bash
docker build -t dgi-dash .
```

Run with a mounted persistent DB path:
```bash
docker run --rm -p 8000:8000 \
  -e DATABASE_URL=sqlite:////app/state/dividends.db \
  -v /your/unraid/path:/app/state \
  dgi-dash
```
