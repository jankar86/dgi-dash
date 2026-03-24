from ingest.etrade import *  # noqa: F403
from ingest.workflows import get_default_etrade_path, run_etrade_import


if __name__ == "__main__":
    run_etrade_import(get_default_etrade_path())
