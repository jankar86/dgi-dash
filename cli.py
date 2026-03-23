import argparse

import workflows


def build_parser():
    parser = argparse.ArgumentParser(description="Data ingestion CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("setup-db", help="Initialize database schema")

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

    if args.command == "import":
        if args.source == "generic":
            workflows.run_generic_import(args.path or workflows.DEFAULT_GENERIC_PATH)
        elif args.source == "fidelity":
            workflows.run_fidelity_import(args.path or workflows.DEFAULT_FIDELITY_PATH)
        elif args.source == "historical":
            workflows.run_historical_import(args.path or workflows.DEFAULT_HISTORICAL_PATH)
        elif args.source == "etrade":
            workflows.run_etrade_import(args.path or workflows.DEFAULT_ETRADE_PATH)
        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
