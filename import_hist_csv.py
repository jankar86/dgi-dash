from ingest.historical import *  # noqa: F403
from ingest.workflows import get_default_historical_path, run_historical_import


if __name__ == "__main__":
    run_historical_import(get_default_historical_path())
