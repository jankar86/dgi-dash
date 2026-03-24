# Changelog

All notable changes to this project are documented in this file.

## [Unreleased]
- Added an `import-all` CLI workflow to run database setup plus the standard historical, Fidelity, and E*TRADE imports in one deterministic pass.
- Added an `import-current` CLI workflow to ingest only live drop-folder Fidelity and E*TRADE data from `data/fidelity` and `data/etrade`.
- Added an `archive-processed` CLI workflow to explicitly move reviewed current CSV files into the archived source folders without overwriting existing archived files.
- Added an `import-manual-interest` CLI workflow to import manual-ledger interest rows into a dedicated `MANUAL-INTEREST` account.
- Added a migration that reclassifies legacy `INTEREST` rows into the dedicated `MANUAL-INTEREST` account.
- Documentation updates for the recommended automated local ingestion path.
- Documented the required filename pattern for new-format Fidelity CSV imports.
- New-format Fidelity CSVs with filenames that do not match the required account suffix pattern are now rejected before import.
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
