import base64
import csv
import json
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path


DEFAULT_GMAIL_CREDENTIALS_PATH = "secrets/google_credentials.json"
DEFAULT_GMAIL_TOKEN_PATH = "secrets/google_token.json"
DEFAULT_GMAIL_REPORT_DIR = "reports"
GMAIL_READONLY_SCOPE = ["https://www.googleapis.com/auth/gmail.readonly"]


def _require_google_client():
    try:
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
        from googleapiclient.discovery import build
    except ImportError as exc:
        raise RuntimeError(
            "Gmail support requires google-api-python-client, google-auth-httplib2, "
            "and google-auth-oauthlib. Install requirements.txt in the local venv first."
        ) from exc
    return Request, Credentials, InstalledAppFlow, build


def _ensure_parent_dir(path):
    Path(path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)


def _get_gmail_service(*, credentials_path, token_path, interactive):
    Request, Credentials, InstalledAppFlow, build = _require_google_client()
    credentials_path = Path(credentials_path).expanduser()
    token_path = Path(token_path).expanduser()

    if not credentials_path.exists():
        raise FileNotFoundError(f"Gmail credentials file not found: {credentials_path}")

    creds = None
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(
            str(token_path), GMAIL_READONLY_SCOPE
        )

    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        _ensure_parent_dir(token_path)
        token_path.write_text(creds.to_json(), encoding="utf-8")

    if not creds or not creds.valid:
        if not interactive:
            raise RuntimeError(
                "No valid Gmail token is available. Run `venv/bin/python cli.py gmail-auth` first."
            )
        flow = InstalledAppFlow.from_client_secrets_file(
            str(credentials_path), GMAIL_READONLY_SCOPE
        )
        creds = flow.run_local_server(port=0)
        _ensure_parent_dir(token_path)
        token_path.write_text(creds.to_json(), encoding="utf-8")

    return build("gmail", "v1", credentials=creds)


def authorize_gmail(*, credentials_path=DEFAULT_GMAIL_CREDENTIALS_PATH, token_path=DEFAULT_GMAIL_TOKEN_PATH):
    _get_gmail_service(
        credentials_path=credentials_path,
        token_path=token_path,
        interactive=True,
    )
    print(f"event=gmail_auth_complete token_path={token_path}")


def _resolve_label_id(service, label_name):
    response = service.users().labels().list(userId="me").execute()
    labels = response.get("labels", [])
    normalized = label_name.strip().lower()
    for label in labels:
        if label.get("name", "").strip().lower() == normalized:
            return label["id"]
    raise ValueError(f"Gmail label not found: {label_name!r}")


def _extract_header(headers, name):
    target = name.strip().lower()
    for header in headers or []:
        if header.get("name", "").strip().lower() == target:
            return header.get("value", "")
    return ""


def _decode_body_data(data):
    if not data:
        return ""
    padding = "=" * (-len(data) % 4)
    raw = base64.urlsafe_b64decode((data + padding).encode("utf-8"))
    return raw.decode("utf-8", errors="replace")


def _extract_plain_text(payload):
    mime_type = payload.get("mimeType")
    body = payload.get("body", {})
    if mime_type == "text/plain":
        return _decode_body_data(body.get("data"))

    for part in payload.get("parts", []) or []:
        text = _extract_plain_text(part)
        if text:
            return text
    return ""


def _message_to_record(message, *, include_body=False):
    payload = message.get("payload", {})
    headers = payload.get("headers", [])
    internal_ms = message.get("internalDate")
    internal_dt = None
    if internal_ms:
        internal_dt = datetime.fromtimestamp(
            int(internal_ms) / 1000, tz=timezone.utc
        ).isoformat()

    header_date = _extract_header(headers, "Date")
    parsed_header_date = ""
    if header_date:
        try:
            parsed_header_date = parsedate_to_datetime(header_date).isoformat()
        except (TypeError, ValueError, IndexError):
            parsed_header_date = header_date

    record = {
        "message_id": message.get("id", ""),
        "thread_id": message.get("threadId", ""),
        "label_ids": ",".join(message.get("labelIds", []) or []),
        "internal_date": internal_dt or "",
        "header_date": parsed_header_date,
        "from": _extract_header(headers, "From"),
        "to": _extract_header(headers, "To"),
        "subject": _extract_header(headers, "Subject"),
        "snippet": (message.get("snippet", "") or "").replace("\n", " ").strip(),
    }
    if include_body:
        record["plain_text_body"] = _extract_plain_text(payload)
    return record


def _sanitize_label_name(label):
    cleaned = "".join(ch.lower() if ch.isalnum() else "_" for ch in label.strip())
    while "__" in cleaned:
        cleaned = cleaned.replace("__", "_")
    return cleaned.strip("_") or "gmail"


def stage_gmail_label(
    *,
    label,
    query=None,
    max_results=100,
    credentials_path=DEFAULT_GMAIL_CREDENTIALS_PATH,
    token_path=DEFAULT_GMAIL_TOKEN_PATH,
    report_dir=DEFAULT_GMAIL_REPORT_DIR,
    include_body=False,
):
    service = _get_gmail_service(
        credentials_path=credentials_path,
        token_path=token_path,
        interactive=False,
    )
    label_id = _resolve_label_id(service, label)

    message_refs = []
    page_token = None
    while len(message_refs) < max_results:
        batch_size = min(100, max_results - len(message_refs))
        response = (
            service.users()
            .messages()
            .list(
                userId="me",
                labelIds=[label_id],
                q=query,
                pageToken=page_token,
                maxResults=batch_size,
            )
            .execute()
        )
        message_refs.extend(response.get("messages", []) or [])
        page_token = response.get("nextPageToken")
        if not page_token:
            break

    records = []
    for ref in message_refs:
        message = (
            service.users()
            .messages()
            .get(
                userId="me",
                id=ref["id"],
                format="full",
            )
            .execute()
        )
        records.append(_message_to_record(message, include_body=include_body))

    report_root = Path(report_dir)
    report_root.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    label_stub = _sanitize_label_name(label)
    csv_path = report_root / f"gmail_{label_stub}_{timestamp}.csv"
    jsonl_path = report_root / f"gmail_{label_stub}_{timestamp}.jsonl"

    fieldnames = [
        "message_id",
        "thread_id",
        "label_ids",
        "internal_date",
        "header_date",
        "from",
        "to",
        "subject",
        "snippet",
    ]
    if include_body:
        fieldnames.append("plain_text_body")

    with csv_path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    with jsonl_path.open("w", encoding="utf-8") as jsonl_file:
        for record in records:
            jsonl_file.write(json.dumps(record, ensure_ascii=True))
            jsonl_file.write("\n")

    print(
        f"event=gmail_stage_complete label={label!r} query={query!r} "
        f"count={len(records)} csv={csv_path} jsonl={jsonl_path}"
    )
