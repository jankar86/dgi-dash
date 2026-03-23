import os
import re

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

    _ensure_transactions_schema(engine)
    _ensure_transaction_indexes(engine)

    with engine.begin() as conn:
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
        _merge_fidelity_alias_accounts(conn)


def _ensure_transactions_schema(engine):
    with engine.begin() as conn:
        create_sql = conn.execute(
            text(
                "SELECT sql FROM sqlite_master "
                "WHERE type='table' AND name='transactions'"
            )
        ).scalar() or ""

        required_tokens = (
            "allocation_status",
            "source_system",
            "source_file",
            "source_row_hash",
            "raw_action",
            "imported_at",
        )
        needs_rebuild = any(token not in create_sql for token in required_tokens)
        needs_rebuild = needs_rebuild or "txn_type VARCHAR(20)" not in create_sql
        needs_rebuild = needs_rebuild or "NUMERIC(20, 2)" not in create_sql
        needs_rebuild = needs_rebuild or "NUMERIC(20, 6)" not in create_sql
        if not needs_rebuild:
            return

        existing_cols = {
            row[1] for row in conn.execute(text("PRAGMA table_info(transactions)")).fetchall()
        }

        def src_col(name, default_sql):
            return name if name in existing_cols else default_sql

        conn.execute(text("PRAGMA foreign_keys=OFF"))
        conn.execute(text("DROP TABLE IF EXISTS transactions_new"))
        conn.execute(
            text(
                """
                CREATE TABLE transactions_new (
                    id INTEGER PRIMARY KEY,
                    account_id INTEGER NOT NULL,
                    security_id INTEGER NOT NULL,
                    txn_type VARCHAR(20) NOT NULL,
                    date DATE NOT NULL,
                    quantity NUMERIC(20, 6) NOT NULL DEFAULT 0,
                    price NUMERIC(20, 6) NOT NULL DEFAULT 0,
                    amount NUMERIC(20, 2) NOT NULL DEFAULT 0,
                    is_qualified BOOLEAN NOT NULL DEFAULT 0,
                    allocation_status VARCHAR(32) NOT NULL DEFAULT 'ALLOCATED',
                    source_system VARCHAR(32),
                    source_file VARCHAR(255),
                    source_row_hash VARCHAR(64) UNIQUE,
                    raw_action VARCHAR(128),
                    imported_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(account_id) REFERENCES accounts(id),
                    FOREIGN KEY(security_id) REFERENCES securities(id),
                    CONSTRAINT uix_txn_unique UNIQUE(account_id, security_id, txn_type, date, amount)
                )
                """
            )
        )
        insert_sql = f"""
            INSERT INTO transactions_new
                (
                    id, account_id, security_id, txn_type, date,
                    quantity, price, amount, is_qualified, allocation_status,
                    source_system, source_file, source_row_hash, raw_action, imported_at
                )
            SELECT
                id,
                account_id,
                security_id,
                COALESCE({src_col('txn_type', "'DIVIDEND'")}, 'DIVIDEND'),
                {src_col('date', 'CURRENT_DATE')},
                COALESCE({src_col('quantity', '0')}, 0),
                COALESCE({src_col('price', '0')}, 0),
                COALESCE({src_col('amount', '0')}, 0),
                COALESCE({src_col('is_qualified', '0')}, 0),
                COALESCE({src_col('allocation_status', "'ALLOCATED'")}, 'ALLOCATED'),
                {src_col('source_system', 'NULL')},
                {src_col('source_file', 'NULL')},
                {src_col('source_row_hash', 'NULL')},
                {src_col('raw_action', 'NULL')},
                COALESCE({src_col('imported_at', 'CURRENT_TIMESTAMP')}, CURRENT_TIMESTAMP)
            FROM transactions
        """
        conn.execute(text(insert_sql))
        conn.execute(text("DROP TABLE transactions"))
        conn.execute(text("ALTER TABLE transactions_new RENAME TO transactions"))
        conn.execute(text("PRAGMA foreign_keys=ON"))


def _ensure_transaction_indexes(engine):
    with engine.begin() as conn:
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(date)"))
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_transactions_account_date "
                "ON transactions(account_id, date)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_transactions_security_date "
                "ON transactions(security_id, date)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_transactions_type_date "
                "ON transactions(txn_type, date)"
            )
        )


def _merge_fidelity_alias_accounts(conn):
    rows = conn.execute(
        text("SELECT id, name FROM accounts WHERE name LIKE 'FIDELITY-%'")
    ).fetchall()
    if not rows:
        return

    full_by_suffix = {}
    alias_rows = []
    for row in rows:
        acct_id, name = row
        m = re.match(r"^FIDELITY-(\d+)$", name or "")
        if not m:
            continue
        digits = m.group(1)
        if len(digits) >= 5:
            full_by_suffix.setdefault(digits[-4:], []).append((acct_id, name))
        elif len(digits) == 4:
            alias_rows.append((acct_id, name, digits))

    for old_id, _old_name, suffix in alias_rows:
        candidates = full_by_suffix.get(suffix, [])
        if len(candidates) != 1:
            continue

        target_id = candidates[0][0]
        conn.execute(
            text(
                """
                DELETE FROM transactions
                WHERE account_id = :old_id
                  AND EXISTS (
                    SELECT 1 FROM transactions t2
                    WHERE t2.account_id = :target_id
                      AND t2.security_id = transactions.security_id
                      AND t2.txn_type = transactions.txn_type
                      AND t2.date = transactions.date
                      AND t2.amount = transactions.amount
                  )
                """
            ),
            {"target_id": target_id, "old_id": old_id},
        )
        conn.execute(
            text("UPDATE transactions SET account_id = :target_id WHERE account_id = :old_id"),
            {"target_id": target_id, "old_id": old_id},
        )
        conn.execute(
            text("DELETE FROM accounts WHERE id = :old_id"),
            {"old_id": old_id},
        )


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
