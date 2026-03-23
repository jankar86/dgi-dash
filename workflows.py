import os

from sqlalchemy import create_engine, inspect, text

from models import Base
import import_csv
import import_etrade_csv
import import_fidelity_csv
import import_hist_csv


DEFAULT_GENERIC_PATH = "your_brokerage_dump.csv"
DEFAULT_ETRADE_PATH = "data/etrade"
DEFAULT_FIDELITY_PATH = "data/fidelity/fid-dev.csv"
DEFAULT_HISTORICAL_PATH = "data/archived/historical_divs.csv"


def _log_event(event, **fields):
    details = " ".join(f"{key}={value}" for key, value in fields.items())
    if details:
        print(f"event={event} {details}")
    else:
        print(f"event={event}")


def setup_db(db_url="sqlite:///dividends.db"):
    engine = create_engine(db_url)
    Base.metadata.create_all(engine)
    _run_post_create_migrations(engine)
    _log_event("db_setup_complete", db_url=db_url)


def _run_post_create_migrations(engine):
    inspector = inspect(engine)
    if "transactions" not in inspector.get_table_names():
        return

    _ensure_transactions_support_reinvestment(engine)

    inspector = inspect(engine)
    tx_columns = {col["name"] for col in inspector.get_columns("transactions")}

    with engine.begin() as conn:
        if "allocation_status" not in tx_columns:
            conn.execute(
                text(
                    "ALTER TABLE transactions "
                    "ADD COLUMN allocation_status VARCHAR DEFAULT 'ALLOCATED'"
                )
            )

        # Keep legacy historical data, but make intent explicit.
        old_account_id = conn.execute(
            text("SELECT id FROM accounts WHERE name = 'Historical' LIMIT 1")
        ).scalar()
        new_account_id = conn.execute(
            text("SELECT id FROM accounts WHERE name = 'UNALLOCATED-LEGACY' LIMIT 1")
        ).scalar()

        if old_account_id and not new_account_id:
            conn.execute(
                text(
                    "UPDATE accounts SET name = 'UNALLOCATED-LEGACY' "
                    "WHERE id = :old_id"
                ),
                {"old_id": old_account_id},
            )

        # Historical/unallocated rows should be marked explicitly.
        conn.execute(
            text(
                "UPDATE transactions "
                "SET allocation_status = 'UNALLOCATED' "
                "WHERE account_id IN ("
                "  SELECT id FROM accounts WHERE name IN ('UNALLOCATED-LEGACY', 'Historical')"
                ")"
            )
        )
        conn.execute(
            text(
                "UPDATE transactions "
                "SET txn_type = 'REINVESTMENT' "
                "WHERE txn_type = 'DIVIDEND' AND amount < 0"
            )
        )


def _ensure_transactions_support_reinvestment(engine):
    with engine.begin() as conn:
        create_sql = conn.execute(
            text(
                "SELECT sql FROM sqlite_master "
                "WHERE type='table' AND name='transactions'"
            )
        ).scalar() or ""

        if "REINVESTMENT" in create_sql or "CHECK" not in create_sql:
            return

        conn.execute(text("PRAGMA foreign_keys=OFF"))
        conn.execute(
            text(
                """
                CREATE TABLE transactions_new (
                    id INTEGER PRIMARY KEY,
                    account_id INTEGER,
                    security_id INTEGER,
                    txn_type VARCHAR(20),
                    date DATE,
                    quantity FLOAT,
                    price FLOAT,
                    amount FLOAT,
                    is_qualified BOOLEAN,
                    allocation_status VARCHAR DEFAULT 'ALLOCATED',
                    FOREIGN KEY(account_id) REFERENCES accounts(id),
                    FOREIGN KEY(security_id) REFERENCES securities(id),
                    CONSTRAINT uix_txn_unique UNIQUE(account_id, security_id, txn_type, date, amount)
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO transactions_new
                    (id, account_id, security_id, txn_type, date, quantity, price, amount, is_qualified, allocation_status)
                SELECT
                    id,
                    account_id,
                    security_id,
                    txn_type,
                    date,
                    quantity,
                    price,
                    amount,
                    is_qualified,
                    COALESCE(allocation_status, 'ALLOCATED')
                FROM transactions
                """
            )
        )
        conn.execute(text("DROP TABLE transactions"))
        conn.execute(text("ALTER TABLE transactions_new RENAME TO transactions"))
        conn.execute(text("PRAGMA foreign_keys=ON"))


def run_generic_import(filepath=DEFAULT_GENERIC_PATH):
    _log_event("import_start", source="generic", path=filepath)
    df = import_csv.load_csv(filepath)
    imported_count, skipped_count = import_csv.import_transactions(df)
    _log_event(
        "import_complete",
        source="generic",
        path=filepath,
        imported=imported_count,
        skipped=skipped_count,
    )
    return imported_count, skipped_count


def run_fidelity_import(filepath=DEFAULT_FIDELITY_PATH):
    _log_event("import_start", source="fidelity", path=filepath)
    df = import_fidelity_csv.load_fidelity_csv(filepath)
    if df is None:
        _log_event("import_complete", source="fidelity", path=filepath, imported=0, skipped=0)
        return 0, 0

    imported_count, skipped_count = import_fidelity_csv.import_transactions(df)
    _log_event(
        "import_complete",
        source="fidelity",
        path=filepath,
        imported=imported_count,
        skipped=skipped_count,
    )
    return imported_count, skipped_count


def run_historical_import(filepath=DEFAULT_HISTORICAL_PATH):
    _log_event("import_start", source="historical", path=filepath)
    df = import_hist_csv.load_custom_historical(filepath)
    if df is None:
        _log_event("import_complete", source="historical", path=filepath, imported=0, skipped=0)
        return 0, 0

    imported_count, skipped_count = import_hist_csv.import_transactions(df)
    _log_event(
        "import_complete",
        source="historical",
        path=filepath,
        imported=imported_count,
        skipped=skipped_count,
    )
    return imported_count, skipped_count


def run_etrade_import(path=DEFAULT_ETRADE_PATH):
    total_imported = 0
    total_skipped = 0
    _log_event("import_start", source="etrade", path=path)

    if os.path.isfile(path):
        imported_count, skipped_count = import_etrade_csv.process_etrade_file(path)
        total_imported += imported_count
        total_skipped += skipped_count
    else:
        imported_count, skipped_count = import_etrade_csv.process_all_etrade_files(data_dir=path)
        total_imported += imported_count
        total_skipped += skipped_count

    _log_event(
        "import_complete",
        source="etrade",
        path=path,
        imported=total_imported,
        skipped=total_skipped,
    )
    return total_imported, total_skipped
