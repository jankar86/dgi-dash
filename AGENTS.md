# AGENTS.md

## Repository purpose
- This repository manages database setup and CSV-based data ingestion.
- Source data lives under the `data/` directory, organized into subfolders.
- Different import scripts exist for different CSV formats and sources.

## Current file roles
- `db_setup.py` initializes or updates the database schema
- `models.py` contains the database models
- `import_csv.py` contains generic CSV import logic or shared import behavior
- `import_etrade_csv.py` imports E*TRADE CSV data
- `import_fidelity_csv.py` imports Fidelity CSV data
- `import_hist_csv.py` imports historical CSV data

## Environment
- Use the local virtual environment at `venv`
- Python 3.x
- Data files are under `data/`

## Rules
- Keep changes small and easy to review
- Follow the existing project structure unless refactoring is requested
- Do not modify CSV source data in `data/` unless explicitly asked
- Ask before changing database schema in a breaking way
- Prefer shared reusable import utilities over duplicated parsing logic
- Preserve existing import behavior unless explicitly improving it

## Commands
- Create venv: `python3 -m venv venv`
- Install dependencies: `venv/bin/pip install -r requirements.txt`
- Setup database: `venv/bin/python db_setup.py`
- Run generic CSV import: `venv/bin/python import_csv.py`
- Run E*TRADE import: `venv/bin/python import_etrade_csv.py`
- Run Fidelity import: `venv/bin/python import_fidelity_csv.py`
- Run historical import: `venv/bin/python import_hist_csv.py`
- Run tests: `venv/bin/pytest -q`

## Guidance for Codex
- First inspect the repository and explain the role of each script
- Identify duplicated parsing, validation, and database-insert logic
- Suggest opportunities to consolidate import workflows into shared modules
- Prefer evolving toward a single CLI entrypoint if it simplifies maintenance
- Before major refactors, explain the proposed structure and migration plan