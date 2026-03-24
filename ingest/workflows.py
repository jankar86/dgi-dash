import os
import re
import shutil

from sqlalchemy import create_engine, inspect, text

from db.models import Base
from ingest import etrade as import_etrade_csv
from ingest import fidelity as import_fidelity_csv
from ingest import generic as import_csv
from ingest import historical as import_hist_csv

DEFAULT_GENERIC_PATH = "your_brokerage_dump.csv"
DEFAULT_ETRADE_PATH = "data/archived/etrade"
DEFAULT_FIDELITY_PATH = "data/archived/fidelity"
DEFAULT_HISTORICAL_PATH = "data/archived/historical_divs.csv"
DEFAULT_CURRENT_ETRADE_PATH = "data/etrade"
DEFAULT_CURRENT_FIDELITY_PATH = "data/fidelity"
DEFAULT_AUTO_IMPORT_ORDER = ("historical", "fidelity", "etrade")


def _log_event(event, **fields):
    details = " ".join(f"{key}={value}" for key, value in fields.items())
    if details:
        print(f"event={event} {details}")
    else:
        print(f"event={event}")


def _first_existing_path(*candidates):
    for candidate in candidates:
        if candidate and os.path.exists(candidate):
            return candidate
    return candidates[0] if candidates else None


def get_default_etrade_path():
    return _first_existing_path(
        DEFAULT_ETRADE_PATH,
        DEFAULT_CURRENT_ETRADE_PATH,
    )


def get_default_fidelity_path():
    return _first_existing_path(
        DEFAULT_FIDELITY_PATH,
        DEFAULT_CURRENT_FIDELITY_PATH,
        "data/archived/fidelity/fid-dev.csv",
        "data/fidelity/fid-dev.csv",
    )


def get_default_historical_path():
    return _first_existing_path(
        DEFAULT_HISTORICAL_PATH,
        "historical_divs.csv",
    )


def get_current_etrade_path():
    return DEFAULT_CURRENT_ETRADE_PATH


def get_current_fidelity_path():
    return DEFAULT_CURRENT_FIDELITY_PATH


def _iter_csv_files(path):
    if os.path.isfile(path):
        return [path]
    if not os.path.isdir(path):
        raise FileNotFoundError(f"Import path does not exist: {path}")

    csv_files = []
    for root, _, files in os.walk(path):
        for file in sorted(files):
            if file.lower().endswith(".csv"):
                csv_files.append(os.path.join(root, file))
    return csv_files


def _path_has_csv_files(path):
    if not path or not os.path.exists(path):
        return False
    if os.path.isfile(path):
        return path.lower().endswith(".csv")
    for _root, _dirs, files in os.walk(path):
        for file in files:
            if file.lower().endswith(".csv"):
                return True
    return False


def _iter_csv_file_pairs(source_path, archive_path):
    if not source_path or not os.path.exists(source_path):
        return

    if os.path.isfile(source_path):
        if source_path.lower().endswith(".csv"):
            yield source_path, os.path.join(archive_path, os.path.basename(source_path))
        return

    for root, _, files in os.walk(source_path):
        for file in sorted(files):
            if not file.lower().endswith(".csv"):
                continue
            src_file = os.path.join(root, file)
            relative_dir = os.path.relpath(root, source_path)
            if relative_dir == ".":
                dest_file = os.path.join(archive_path, file)
            else:
                dest_file = os.path.join(archive_path, relative_dir, file)
            yield src_file, dest_file


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
        _merge_security_case_aliases(conn)
        _move_interest_transactions_to_manual_account(conn)
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


def _merge_security_case_aliases(conn):
    rows = conn.execute(
        text("SELECT id, ticker FROM securities")
    ).fetchall()
    if not rows:
        return

    grouped = {}
    for security_id, ticker in rows:
        normalized = (ticker or "").strip().upper()
        if not normalized:
            continue
        grouped.setdefault(normalized, []).append((security_id, ticker))

    for normalized, members in grouped.items():
        uppercase_member = next((member for member in members if member[1] == normalized), None)
        target_id = uppercase_member[0] if uppercase_member else members[0][0]

        conn.execute(
            text("UPDATE securities SET ticker = :ticker WHERE id = :security_id"),
            {"ticker": normalized, "security_id": target_id},
        )

        for old_id, _old_ticker in members:
            if old_id == target_id:
                continue

            conn.execute(
                text(
                    """
                    DELETE FROM transactions
                    WHERE security_id = :old_id
                      AND EXISTS (
                        SELECT 1 FROM transactions t2
                        WHERE t2.security_id = :target_id
                          AND t2.account_id = transactions.account_id
                          AND t2.txn_type = transactions.txn_type
                          AND t2.date = transactions.date
                          AND t2.amount = transactions.amount
                      )
                    """
                ),
                {"target_id": target_id, "old_id": old_id},
            )
            conn.execute(
                text("UPDATE transactions SET security_id = :target_id WHERE security_id = :old_id"),
                {"target_id": target_id, "old_id": old_id},
            )
            conn.execute(
                text("DELETE FROM securities WHERE id = :old_id"),
                {"old_id": old_id},
            )


def _move_interest_transactions_to_manual_account(conn):
    interest_security_id = conn.execute(
        text("SELECT id FROM securities WHERE ticker = 'INTEREST' LIMIT 1")
    ).scalar()
    if not interest_security_id:
        return

    manual_account_id = conn.execute(
        text("SELECT id FROM accounts WHERE name = 'MANUAL-INTEREST' LIMIT 1")
    ).scalar()
    if not manual_account_id:
        conn.execute(
            text("INSERT INTO accounts(name) VALUES ('MANUAL-INTEREST')")
        )
        manual_account_id = conn.execute(
            text("SELECT id FROM accounts WHERE name = 'MANUAL-INTEREST' LIMIT 1")
        ).scalar()

    if not manual_account_id:
        return

    conn.execute(
        text(
            """
            DELETE FROM transactions
            WHERE security_id = :interest_security_id
              AND account_id != :manual_account_id
              AND EXISTS (
                SELECT 1 FROM transactions t2
                WHERE t2.account_id = :manual_account_id
                  AND t2.security_id = transactions.security_id
                  AND t2.txn_type = transactions.txn_type
                  AND t2.date = transactions.date
                  AND t2.amount = transactions.amount
              )
            """
        ),
        {
            "interest_security_id": interest_security_id,
            "manual_account_id": manual_account_id,
        },
    )
    conn.execute(
        text(
            """
            UPDATE transactions
            SET account_id = :manual_account_id,
                allocation_status = 'ALLOCATED'
            WHERE security_id = :interest_security_id
              AND account_id != :manual_account_id
            """
        ),
        {
            "interest_security_id": interest_security_id,
            "manual_account_id": manual_account_id,
        },
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
    filepath = filepath or get_default_fidelity_path()
    total_imported = 0
    total_skipped = 0
    _log_event("import_start", source="fidelity", path=filepath)

    for csv_path in _iter_csv_files(filepath):
        df = import_fidelity_csv.load_fidelity_csv(csv_path)
        if df is None:
            continue

        imported_count, skipped_count = import_fidelity_csv.import_transactions(df)
        total_imported += imported_count
        total_skipped += skipped_count

    _log_event(
        "import_complete",
        source="fidelity",
        path=filepath,
        imported=total_imported,
        skipped=total_skipped,
    )
    return total_imported, total_skipped


def run_historical_import(filepath=DEFAULT_HISTORICAL_PATH):
    filepath = filepath or get_default_historical_path()
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
    path = path or get_default_etrade_path()
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


def run_all_imports(
    *,
    historical_path=None,
    fidelity_path=None,
    etrade_path=None,
    setup=True,
):
    if setup:
        setup_db()

    resolved_paths = {
        "historical": historical_path or get_default_historical_path(),
        "fidelity": fidelity_path or get_default_fidelity_path(),
        "etrade": etrade_path or get_default_etrade_path(),
    }
    results = {}
    total_imported = 0
    total_skipped = 0

    _log_event(
        "import_all_start",
        historical=resolved_paths["historical"],
        fidelity=resolved_paths["fidelity"],
        etrade=resolved_paths["etrade"],
    )

    for source in DEFAULT_AUTO_IMPORT_ORDER:
        path = resolved_paths[source]
        if source == "historical":
            imported_count, skipped_count = run_historical_import(path)
        elif source == "fidelity":
            imported_count, skipped_count = run_fidelity_import(path)
        else:
            imported_count, skipped_count = run_etrade_import(path)

        results[source] = {
            "path": path,
            "imported": imported_count,
            "skipped": skipped_count,
        }
        total_imported += imported_count
        total_skipped += skipped_count

    _log_event(
        "import_all_complete",
        imported=total_imported,
        skipped=total_skipped,
        sources=len(results),
    )
    return {
        "results": results,
        "imported": total_imported,
        "skipped": total_skipped,
    }


def run_current_imports(
    *,
    fidelity_path=None,
    etrade_path=None,
    setup=True,
):
    if setup:
        setup_db()

    resolved_paths = {
        "fidelity": fidelity_path or get_current_fidelity_path(),
        "etrade": etrade_path or get_current_etrade_path(),
    }
    results = {}
    total_imported = 0
    total_skipped = 0

    _log_event(
        "import_current_start",
        fidelity=resolved_paths["fidelity"],
        etrade=resolved_paths["etrade"],
    )

    for source in ("fidelity", "etrade"):
        path = resolved_paths[source]
        if not _path_has_csv_files(path):
            _log_event("import_skipped", source=source, path=path, reason="path_missing_or_empty")
            results[source] = {
                "path": path,
                "imported": 0,
                "skipped": 0,
                "status": "skipped",
            }
            continue

        if source == "fidelity":
            imported_count, skipped_count = run_fidelity_import(path)
        else:
            imported_count, skipped_count = run_etrade_import(path)

        results[source] = {
            "path": path,
            "imported": imported_count,
            "skipped": skipped_count,
            "status": "processed",
        }
        total_imported += imported_count
        total_skipped += skipped_count

    _log_event(
        "import_current_complete",
        imported=total_imported,
        skipped=total_skipped,
        sources=len(results),
    )
    return {
        "results": results,
        "imported": total_imported,
        "skipped": total_skipped,
    }


def archive_processed_files(
    *,
    fidelity_path=None,
    etrade_path=None,
    archived_fidelity_path=None,
    archived_etrade_path=None,
    dry_run=False,
):
    moves = {
        "fidelity": (
            fidelity_path or get_current_fidelity_path(),
            archived_fidelity_path or DEFAULT_FIDELITY_PATH,
        ),
        "etrade": (
            etrade_path or get_current_etrade_path(),
            archived_etrade_path or DEFAULT_ETRADE_PATH,
        ),
    }
    results = {}

    _log_event(
        "archive_processed_start",
        fidelity_source=moves["fidelity"][0],
        fidelity_archive=moves["fidelity"][1],
        etrade_source=moves["etrade"][0],
        etrade_archive=moves["etrade"][1],
        dry_run=dry_run,
    )

    for source, (source_path, archive_path) in moves.items():
        moved_count = 0
        conflict_count = 0
        skipped_count = 0

        if not _path_has_csv_files(source_path):
            _log_event("archive_skipped", source=source, path=source_path, reason="path_missing_or_empty")
            results[source] = {
                "source_path": source_path,
                "archive_path": archive_path,
                "moved": 0,
                "conflicts": 0,
                "skipped": 0,
                "status": "skipped",
            }
            continue

        for src_file, dest_file in _iter_csv_file_pairs(source_path, archive_path):
            if os.path.exists(dest_file):
                print(f"⚠️ Archive conflict, leaving file in place: {src_file} -> {dest_file}")
                conflict_count += 1
                continue

            if dry_run:
                print(f"DRY RUN move: {src_file} -> {dest_file}")
                moved_count += 1
                continue

            os.makedirs(os.path.dirname(dest_file), exist_ok=True)
            shutil.move(src_file, dest_file)
            print(f"Archived: {src_file} -> {dest_file}")
            moved_count += 1

        results[source] = {
            "source_path": source_path,
            "archive_path": archive_path,
            "moved": moved_count,
            "conflicts": conflict_count,
            "skipped": skipped_count,
            "status": "processed",
        }

    _log_event(
        "archive_processed_complete",
        fidelity_moved=results.get("fidelity", {}).get("moved", 0),
        fidelity_conflicts=results.get("fidelity", {}).get("conflicts", 0),
        etrade_moved=results.get("etrade", {}).get("moved", 0),
        etrade_conflicts=results.get("etrade", {}).get("conflicts", 0),
        dry_run=dry_run,
    )
    return results
