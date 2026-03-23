import os

from sqlalchemy import create_engine

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
    _log_event("db_setup_complete", db_url=db_url)


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
