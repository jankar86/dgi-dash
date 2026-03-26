import csv
import email
import imaplib
import json
import re
import sqlite3
from datetime import date
from datetime import datetime, timezone
from email.header import decode_header, make_header
from pathlib import Path

from db.models import Account, Security, Transaction, TxnType
from db.utils import create_session, upsert_transaction


DEFAULT_GMAIL_APP_PASSWORD_PATH = "secrets/gmail_app_password.txt"
DEFAULT_GMAIL_REPORT_DIR = "reports"
DEFAULT_DB_PATH = "dividends.db"
GMAIL_IMAP_HOST = "imap.gmail.com"
GMAIL_IMAP_PORT = 993


def _decode_header_value(value):
    if not value:
        return ""
    try:
        return str(make_header(decode_header(value)))
    except Exception:
        return value


def _extract_plain_text(msg):
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() != "text/plain":
                continue
            if part.get_content_disposition() == "attachment":
                continue
            payload = part.get_payload(decode=True)
            if payload is None:
                continue
            charset = part.get_content_charset() or "utf-8"
            return payload.decode(charset, errors="replace")
        return ""
    payload = msg.get_payload(decode=True)
    if payload is None:
        return ""
    charset = msg.get_content_charset() or "utf-8"
    return payload.decode(charset, errors="replace")


def _load_app_password(app_password, app_password_file):
    if app_password:
        return app_password.strip().replace(" ", "")
    path = Path(app_password_file).expanduser()
    if not path.exists():
        raise FileNotFoundError(
            f"Gmail app password file not found: {path}. Pass --app-password or create the file."
        )
    return path.read_text(encoding="utf-8").strip().replace(" ", "")


def _message_to_record(uid, msg, *, body_text, include_body=False):
    date_header = _decode_header_value(msg.get("Date", ""))
    parsed_date = ""
    if date_header:
        try:
            parsed_date = email.utils.parsedate_to_datetime(date_header).astimezone(
                timezone.utc
            ).isoformat()
        except Exception:
            parsed_date = date_header

    record = {
        "message_uid": uid,
        "message_id": _decode_header_value(msg.get("Message-ID", "")),
        "header_date": parsed_date,
        "from": _decode_header_value(msg.get("From", "")),
        "to": _decode_header_value(msg.get("To", "")),
        "subject": _decode_header_value(msg.get("Subject", "")),
        "snippet": body_text.replace("\n", " ").strip()[:500],
        "_body_text": body_text,
    }
    if include_body:
        record["plain_text_body"] = body_text
    return record


def _sanitize_mailbox_name(mailbox):
    cleaned = "".join(ch.lower() if ch.isalnum() else "_" for ch in mailbox.strip())
    while "__" in cleaned:
        cleaned = cleaned.replace("__", "_")
    return cleaned.strip("_") or "gmail"


def _extract_etrade_alert_transactions(record):
    body = record.get("_body_text") or record.get("plain_text_body") or ""
    if not body:
        return []

    account_match = re.search(r"Account:\s*X+-?(\d{4})", body, re.I)
    date_match = re.search(r"payment\(s\) on\s*(\d{4}-\d{2}-\d{2})", body, re.I)
    if not account_match or not date_match:
        return []

    account_name = f"etr-{account_match.group(1)}"
    payment_date = date_match.group(1)
    pairs = re.findall(
        r"Security:\s*(.*?)\s*\(([A-Z0-9.]+)\)\s*Amount Credited:\s*\$([0-9,]+(?:\.\d{1,2})?)",
        body,
        re.I | re.S,
    )

    extracted = []
    for security_name, raw_symbol, amount in pairs:
        normalized_name = " ".join(security_name.split())
        symbol = raw_symbol.upper()
        txn_type = "DIVIDEND"
        notes = ""
        if symbol.isdigit() and (
            "MORGAN STANLEY BANK" in normalized_name.upper()
            or "PRIVATE BANK" in normalized_name.upper()
        ):
            txn_type = "INTEREST"
            notes = "Mapped from Morgan Stanley bank sweep alert."
            symbol = "INTEREST"

        extracted.append(
            {
                "message_uid": record.get("message_uid", ""),
                "message_id": record.get("message_id", ""),
                "header_date": record.get("header_date", ""),
                "account": account_name,
                "date": payment_date,
                "security_name": normalized_name,
                "raw_symbol": raw_symbol.upper(),
                "security": symbol,
                "amount": round(float(amount.replace(",", "")), 2),
                "txn_type": txn_type,
                "subject": record.get("subject", ""),
                "notes": notes,
            }
        )
    return extracted


def _enrich_with_db_match_status(rows, db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        for row in rows:
            exact = [
                dict(r)
                for r in conn.execute(
                    """
                    SELECT t.date, s.ticker AS security, a.name AS account, ROUND(t.amount, 2) AS amount, t.txn_type
                    FROM transactions t
                    JOIN securities s ON s.id = t.security_id
                    JOIN accounts a ON a.id = t.account_id
                    WHERE a.name = ?
                      AND s.ticker = ?
                      AND t.date = ?
                      AND ROUND(t.amount, 2) = ?
                    """,
                    (row["account"], row["security"], row["date"], row["amount"]),
                )
            ]
            same_amount = [
                dict(r)
                for r in conn.execute(
                    """
                    SELECT t.date, s.ticker AS security, a.name AS account, ROUND(t.amount, 2) AS amount, t.txn_type
                    FROM transactions t
                    JOIN securities s ON s.id = t.security_id
                    JOIN accounts a ON a.id = t.account_id
                    WHERE a.name = ?
                      AND s.ticker = ?
                      AND ROUND(t.amount, 2) = ?
                    ORDER BY t.date
                    """,
                    (row["account"], row["security"], row["amount"]),
                )
            ]
            row["db_exact_match_count"] = len(exact)
            row["db_same_amount_match_count"] = len(same_amount)
            row["db_same_amount_dates"] = ",".join(match["date"] for match in same_amount)
            if exact:
                row["db_match_status"] = "EXACT_MATCH"
            elif same_amount:
                row["db_match_status"] = "AMOUNT_MATCH_DATE_DIFF"
            else:
                row["db_match_status"] = "MISSING"
    finally:
        conn.close()
    return rows


def stage_gmail_imap(
    *,
    username,
    app_password=None,
    app_password_file=DEFAULT_GMAIL_APP_PASSWORD_PATH,
    mailbox="INBOX",
    search="ALL",
    max_results=100,
    report_dir=DEFAULT_GMAIL_REPORT_DIR,
    include_body=False,
    db_path=DEFAULT_DB_PATH,
):
    password = _load_app_password(app_password, app_password_file)
    imap = imaplib.IMAP4_SSL(GMAIL_IMAP_HOST, GMAIL_IMAP_PORT)
    try:
        imap.login(username, password)
        status, _ = imap.select(f'"{mailbox}"', readonly=True)
        if status != "OK":
            raise RuntimeError(f"Unable to select mailbox: {mailbox!r}")

        status, data = imap.search(None, search)
        if status != "OK":
            raise RuntimeError(f"IMAP search failed for: {search!r}")

        uids = [uid.decode("utf-8") for uid in (data[0] or b"").split() if uid]
        uids = uids[-max_results:]

        records = []
        for uid in uids:
            fetch_status, msg_data = imap.fetch(uid, "(RFC822)")
            if fetch_status != "OK" or not msg_data:
                continue
            raw_bytes = None
            for part in msg_data:
                if isinstance(part, tuple):
                    raw_bytes = part[1]
                    break
            if not raw_bytes:
                continue
            msg = email.message_from_bytes(raw_bytes)
            body_text = _extract_plain_text(msg)
            records.append(
                _message_to_record(
                    uid,
                    msg,
                    body_text=body_text,
                    include_body=include_body,
                )
            )
    finally:
        try:
            imap.close()
        except Exception:
            pass
        imap.logout()

    report_root = Path(report_dir)
    report_root.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    mailbox_stub = _sanitize_mailbox_name(mailbox)
    csv_path = report_root / f"gmail_imap_{mailbox_stub}_{timestamp}.csv"
    jsonl_path = report_root / f"gmail_imap_{mailbox_stub}_{timestamp}.jsonl"
    parsed_csv_path = report_root / f"gmail_imap_{mailbox_stub}_{timestamp}_parsed_transactions.csv"

    fieldnames = [
        "message_uid",
        "message_id",
        "header_date",
        "from",
        "to",
        "subject",
        "snippet",
    ]
    if include_body:
        fieldnames.append("plain_text_body")

    with csv_path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)

    with jsonl_path.open("w", encoding="utf-8") as jsonl_file:
        for record in records:
            persisted = {k: v for k, v in record.items() if k != "_body_text"}
            jsonl_file.write(json.dumps(persisted, ensure_ascii=True))
            jsonl_file.write("\n")

    parsed_rows = []
    for record in records:
        parsed_rows.extend(_extract_etrade_alert_transactions(record))
    parsed_rows = _enrich_with_db_match_status(parsed_rows, db_path)

    parsed_fieldnames = [
        "message_uid",
        "message_id",
        "header_date",
        "account",
        "date",
        "security_name",
        "raw_symbol",
        "security",
        "amount",
        "txn_type",
        "db_match_status",
        "db_exact_match_count",
        "db_same_amount_match_count",
        "db_same_amount_dates",
        "subject",
        "notes",
    ]
    with parsed_csv_path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=parsed_fieldnames)
        writer.writeheader()
        writer.writerows(parsed_rows)

    print(
        f"event=gmail_imap_stage_complete mailbox={mailbox!r} search={search!r} "
        f"count={len(records)} csv={csv_path} jsonl={jsonl_path} parsed_csv={parsed_csv_path}"
    )


def import_parsed_gmail_transactions(path, *, db_url=DEFAULT_DB_PATH, skip_interest=True):
    csv_path = Path(path)
    if not csv_path.exists():
        raise FileNotFoundError(f"Parsed Gmail transaction CSV not found: {csv_path}")

    session = create_session(f"sqlite:///{db_url}" if not db_url.startswith("sqlite") else db_url)
    inserted = 0
    updated_dates = 0
    skipped_interest = 0
    skipped_existing = 0
    skipped_ambiguous = 0

    try:
        with csv_path.open("r", encoding="utf-8", newline="") as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                txn_type = (row.get("txn_type") or "").strip().upper()
                if skip_interest and txn_type == "INTEREST":
                    skipped_interest += 1
                    continue

                account_name = (row.get("account") or "").strip()
                symbol = (row.get("security") or "").strip().upper()
                txn_date = date.fromisoformat((row.get("date") or "").strip())
                amount = round(float(row.get("amount") or 0), 2)

                existing_exact = (
                    session.query(Transaction)
                    .join(Account, Account.id == Transaction.account_id)
                    .join(Security, Security.id == Transaction.security_id)
                    .filter(
                        Account.name == account_name,
                        Security.ticker == symbol,
                        Transaction.date == txn_date,
                        Transaction.amount == amount,
                        Transaction.txn_type == TxnType.DIVIDEND,
                    )
                    .first()
                )
                if existing_exact:
                    skipped_existing += 1
                    continue

                same_amount_matches = (
                    session.query(Transaction)
                    .join(Account, Account.id == Transaction.account_id)
                    .join(Security, Security.id == Transaction.security_id)
                    .filter(
                        Account.name == account_name,
                        Security.ticker == symbol,
                        Transaction.amount == amount,
                        Transaction.txn_type == TxnType.DIVIDEND,
                    )
                    .all()
                )

                if len(same_amount_matches) == 1:
                    match = same_amount_matches[0]
                    match.date = txn_date
                    match.source_system = "gmail_imap"
                    match.source_file = str(csv_path)
                    match.raw_action = "EMAIL ALERT"
                    match.annotation_note = "Date aligned to Gmail payment-date alert."
                    updated_dates += 1
                    continue

                if len(same_amount_matches) > 1:
                    skipped_ambiguous += 1
                    continue

                inserted_flag = upsert_transaction(
                    session,
                    account_name=account_name,
                    symbol=symbol,
                    txn_type=TxnType.DIVIDEND,
                    date=txn_date,
                    quantity=0,
                    price=0,
                    amount=amount,
                    source_system="gmail_imap",
                    source_file=str(csv_path),
                    raw_action="EMAIL ALERT",
                    annotation_note="Inserted from parsed Gmail dividend alert using payment date from email body.",
                )
                if inserted_flag:
                    inserted += 1
                else:
                    skipped_existing += 1

        session.commit()
    finally:
        session.close()

    print(
        "event=gmail_import_complete "
        f"path={csv_path} inserted={inserted} updated_dates={updated_dates} "
        f"skipped_interest={skipped_interest} skipped_existing={skipped_existing} "
        f"skipped_ambiguous={skipped_ambiguous}"
    )
