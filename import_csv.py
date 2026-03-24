from ingest.generic import *  # noqa: F403
from ingest.workflows import DEFAULT_GENERIC_PATH, run_generic_import


if __name__ == "__main__":
    run_generic_import(DEFAULT_GENERIC_PATH)
