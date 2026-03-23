#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PYTHON_BIN="venv/bin/python"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "error: missing $PYTHON_BIN"
  exit 1
fi

echo "[smoke] reset database"
rm -f dividends.db

echo "[smoke] setup schema"
"$PYTHON_BIN" cli.py setup-db

echo "[smoke] first import run"
"$PYTHON_BIN" cli.py import --source historical
"$PYTHON_BIN" cli.py import --source fidelity
"$PYTHON_BIN" cli.py import --source etrade

echo "[smoke] row counts after first run"
"$PYTHON_BIN" - <<'PY'
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from models import Account, Security, Transaction

engine = create_engine("sqlite:///dividends.db")
Session = sessionmaker(bind=engine)
s = Session()

print("accounts:", s.query(func.count(Account.id)).scalar())
print("securities:", s.query(func.count(Security.id)).scalar())
print("transactions:", s.query(func.count(Transaction.id)).scalar())
PY

echo "[smoke] second import run (idempotency check)"
"$PYTHON_BIN" cli.py import --source historical
"$PYTHON_BIN" cli.py import --source fidelity
"$PYTHON_BIN" cli.py import --source etrade

echo "[smoke] row counts after second run"
"$PYTHON_BIN" - <<'PY'
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from models import Account, Security, Transaction

engine = create_engine("sqlite:///dividends.db")
Session = sessionmaker(bind=engine)
s = Session()

print("accounts:", s.query(func.count(Account.id)).scalar())
print("securities:", s.query(func.count(Security.id)).scalar())
print("transactions:", s.query(func.count(Transaction.id)).scalar())
PY

echo "[smoke] done"
