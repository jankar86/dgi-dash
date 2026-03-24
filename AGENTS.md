# AGENTS.md

## Repository purpose
- This repository manages database setup and CSV-based data ingestion.
- Source data lives under the `data/` directory, organized into subfolders.
- The main entrypoint is `cli.py`, with implementation split into logical packages.

## Current file roles
- `cli.py` is the single supported command entrypoint
- `db/` contains the database models and shared DB utilities
- `ingest/` contains generic, E*TRADE, Fidelity, and historical import logic plus workflow orchestration
- `reporting/` contains coverage and reconciliation/reporting logic
- `webapp/` contains the read-only presentation layer and query code

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
- Setup database: `venv/bin/python cli.py setup-db`
- Run generic CSV import: `venv/bin/python cli.py import --source generic --path <file>`
- Run E*TRADE import: `venv/bin/python cli.py import --source etrade --path <file-or-dir>`
- Run Fidelity import: `venv/bin/python cli.py import --source fidelity --path <file-or-dir>`
- Run historical import: `venv/bin/python cli.py import --source historical --path data/archived/historical_divs.csv`
- Run current drop-folder import: `venv/bin/python cli.py import-current`
- Run archived/bootstrap import: `venv/bin/python cli.py import-all`
- Run web app: `venv/bin/python cli.py serve-web --host 127.0.0.1 --port 8000`
- Run tests: `venv/bin/pytest -q`

## Guidance for Codex
- First inspect the repository and explain the role of each script
- Explain the role of each package (`db`, `ingest`, `reporting`, `webapp`) before proposing structural changes
- Identify duplicated parsing, validation, and database-insert logic
- Suggest opportunities to consolidate import workflows into shared modules
- Prefer evolving toward a single CLI entrypoint if it simplifies maintenance
- Before major refactors, explain the proposed structure and migration plan
