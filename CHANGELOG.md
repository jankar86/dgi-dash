# Changelog

All notable changes to this project are documented in this file.

## [Unreleased]
- Refactored the codebase into logical packages: `db`, `ingest`, `reporting`, and `webapp`.
- Removed legacy top-level wrapper modules and standardized on `cli.py` as the single supported entrypoint.
- Added a read-only web dashboard and transaction explorer for the local database.
- Added a `serve-web` CLI command for running the dashboard locally.
- Added a basic `Dockerfile` and `requirements.txt` for single-container deployment.
- Added an `import-all` CLI workflow to run database setup plus the standard historical, Fidelity, and E*TRADE imports in one deterministic pass.
- Added an `import-current` CLI workflow to ingest only live drop-folder Fidelity and E*TRADE data from `data/fidelity` and `data/etrade`.
- Added an `archive-processed` CLI workflow to explicitly move reviewed current CSV files into the archived source folders without overwriting existing archived files.
- Added an `import-manual-interest` CLI workflow to import manual-ledger interest rows into a dedicated `MANUAL-INTEREST` account.
- Added a migration that reclassifies legacy `INTEREST` rows into the dedicated `MANUAL-INTEREST` account.
- Documentation updates for the recommended automated local ingestion path.
- Fidelity imports now require per-row `Account Number` data from the CSV and fail fast instead of inferring account identity from filenames.
- Broker account names are now normalized to short aliases like `etr-1445` and `fid-4217`, with a migration for existing DB rows.
- Added extended account metadata fields and backfilled inferred values for broker, manual, and legacy accounts.
- Account tax metadata now uses user-facing designations like `Taxable`, `Roth IRA`, and `Traditional IRA`.
- Added a manual-ledger comparison report to reconcile date/security/amount records against the database while ignoring interest rows.

## [2026-03-23]
### Added
- Unified CLI entrypoint in `cli.py` for setup/import/report commands.
- Coverage and month-gap reporting commands.
- Shared import utility module for centralized upsert logic.
- SQL backup workflow for SQLite dump snapshots.

### Changed
- Import workflows refactored behind `workflows.py`.
- Schema hardening for transaction precision and provenance fields.
- Row hash calculation updated to exclude source metadata (`source_system`, `source_file`).
- Transaction normalization for reinvestment handling (`REINVESTMENT`).
- Historical account handling updated to `UNALLOCATED-LEGACY` with allocation status tagging.
- Broker importers updated for backward-compatible old/new CSV export formats.

### Fixed
- Account alias merge behavior for certain Fidelity account variants.
- Multiple dedupe/import robustness issues across source importers.

### Security
- Stopped tracking raw CSV data and SQLite DB artifacts in git.
- Added ignore rules for `data/**/*.csv`, `dividends.db`, and `backups/*.sql`.
- Repository history cleaned to remove previously tracked data artifacts.
