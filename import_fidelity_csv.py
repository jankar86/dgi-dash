from ingest.fidelity import *  # noqa: F403
from ingest.workflows import get_default_fidelity_path, run_fidelity_import


if __name__ == "__main__":
    run_fidelity_import(get_default_fidelity_path())
