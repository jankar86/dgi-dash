import argparse

import coverage_report
import dashboard_app
import manual_compare
import workflows


def build_parser():
    parser = argparse.ArgumentParser(description="Data ingestion CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("setup-db", help="Initialize database schema")
    serve_parser = subparsers.add_parser(
        "serve-web",
        help="Run the read-only web dashboard against the local database",
    )
    serve_parser.add_argument("--host", default="127.0.0.1", help="Bind host (default: 127.0.0.1)")
    serve_parser.add_argument("--port", type=int, default=8000, help="Bind port (default: 8000)")
    serve_parser.add_argument(
        "--db-url",
        default=dashboard_app.DEFAULT_DB_URL,
        help="Database URL (default: env DATABASE_URL or sqlite:///dividends.db)",
    )
    import_all_parser = subparsers.add_parser(
        "import-all",
        help="Set up the DB and run the standard historical, Fidelity, and E*TRADE imports",
    )
    import_all_parser.add_argument(
        "--historical-path",
        help="Historical CSV path (default: auto-discovered local path)",
    )
    import_all_parser.add_argument(
        "--fidelity-path",
        help="Fidelity CSV file or directory path (default: auto-discovered local path)",
    )
    import_all_parser.add_argument(
        "--etrade-path",
        help="E*TRADE CSV file or directory path (default: auto-discovered local path)",
    )
    import_all_parser.add_argument(
        "--skip-setup",
        action="store_true",
        help="Skip database setup before running imports",
    )
    import_current_parser = subparsers.add_parser(
        "import-current",
        help="Set up the DB and import only current drop-folder Fidelity and E*TRADE data",
    )
    import_current_parser.add_argument(
        "--fidelity-path",
        help="Current Fidelity CSV file or directory path (default: data/fidelity)",
    )
    import_current_parser.add_argument(
        "--etrade-path",
        help="Current E*TRADE CSV file or directory path (default: data/etrade)",
    )
    import_current_parser.add_argument(
        "--skip-setup",
        action="store_true",
        help="Skip database setup before running imports",
    )
    archive_parser = subparsers.add_parser(
        "archive-processed",
        help="Move reviewed current Fidelity and E*TRADE CSVs into the archived folders",
    )
    archive_parser.add_argument(
        "--fidelity-path",
        help="Current Fidelity CSV file or directory path (default: data/fidelity)",
    )
    archive_parser.add_argument(
        "--etrade-path",
        help="Current E*TRADE CSV file or directory path (default: data/etrade)",
    )
    archive_parser.add_argument(
        "--archived-fidelity-path",
        help="Archived Fidelity destination path (default: data/archived/fidelity)",
    )
    archive_parser.add_argument(
        "--archived-etrade-path",
        help="Archived E*TRADE destination path (default: data/archived/etrade)",
    )
    archive_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show which files would move without changing anything",
    )
    coverage_parser = subparsers.add_parser(
        "report-coverage", help="Show latest transaction coverage by account"
    )
    coverage_parser.add_argument(
        "--as-of",
        help="Reference date in YYYY-MM-DD format (default: today)",
    )
    gaps_parser = subparsers.add_parser(
        "report-gaps", help="Show month-level data gaps by account"
    )
    gaps_parser.add_argument(
        "--as-of",
        help="Reference date in YYYY-MM-DD format (default: today)",
    )
    manual_parser = subparsers.add_parser(
        "report-manual-compare",
        help="Compare a manual dividend ledger against the database",
    )
    manual_parser.add_argument(
        "--path",
        required=True,
        help="Path to manual CSV or Excel ledger",
    )
    manual_parser.add_argument(
        "--date-tolerance-days",
        type=int,
        default=1,
        help="Maximum allowed date drift when matching rows (default: 1)",
    )
    manual_parser.add_argument(
        "--amount-tolerance",
        type=float,
        default=0.05,
        help="Maximum allowed amount drift when matching rows (default: 0.05)",
    )
    manual_interest_parser = subparsers.add_parser(
        "import-manual-interest",
        help="Import interest rows from a manual ledger into a dedicated manual-interest account",
    )
    manual_interest_parser.add_argument(
        "--path",
        required=True,
        help="Path to a manual ledger CSV/Excel file or the generated interest-only CSV report",
    )
    manual_interest_parser.add_argument(
        "--account-name",
        default=manual_compare.DEFAULT_MANUAL_INTEREST_ACCOUNT,
        help="Account name to use for imported interest rows",
    )

    import_parser = subparsers.add_parser("import", help="Run an import workflow")
    import_parser.add_argument(
        "--source",
        required=True,
        choices=["generic", "fidelity", "historical", "etrade"],
        help="Data source format",
    )
    import_parser.add_argument(
        "--path",
        help="File or directory path for source data (defaults per source)",
    )

    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "setup-db":
        workflows.setup_db()
        return 0

    if args.command == "serve-web":
        dashboard_app.serve(host=args.host, port=args.port, db_url=args.db_url)
        return 0

    if args.command == "import-all":
        workflows.run_all_imports(
            historical_path=args.historical_path,
            fidelity_path=args.fidelity_path,
            etrade_path=args.etrade_path,
            setup=not args.skip_setup,
        )
        return 0

    if args.command == "import-current":
        workflows.run_current_imports(
            fidelity_path=args.fidelity_path,
            etrade_path=args.etrade_path,
            setup=not args.skip_setup,
        )
        return 0

    if args.command == "archive-processed":
        workflows.archive_processed_files(
            fidelity_path=args.fidelity_path,
            etrade_path=args.etrade_path,
            archived_fidelity_path=args.archived_fidelity_path,
            archived_etrade_path=args.archived_etrade_path,
            dry_run=args.dry_run,
        )
        return 0

    if args.command == "import":
        if args.source == "generic":
            workflows.run_generic_import(args.path or workflows.DEFAULT_GENERIC_PATH)
        elif args.source == "fidelity":
            workflows.run_fidelity_import(args.path or workflows.get_default_fidelity_path())
        elif args.source == "historical":
            workflows.run_historical_import(args.path or workflows.get_default_historical_path())
        elif args.source == "etrade":
            workflows.run_etrade_import(args.path or workflows.get_default_etrade_path())
        return 0

    if args.command == "report-coverage":
        coverage_report.print_account_coverage(as_of=args.as_of)
        return 0

    if args.command == "report-gaps":
        coverage_report.print_monthly_gap_analysis(as_of=args.as_of)
        return 0

    if args.command == "report-manual-compare":
        manual_compare.print_manual_compare_report(
            path=args.path,
            date_tolerance_days=args.date_tolerance_days,
            amount_tolerance=args.amount_tolerance,
        )
        return 0

    if args.command == "import-manual-interest":
        manual_compare.import_manual_interest(
            path=args.path,
            account_name=args.account_name,
        )
        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
