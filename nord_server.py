from http.server import BaseHTTPRequestHandler, HTTPServer
import base64
import datetime
from email import policy
from email.parser import BytesParser
import hashlib
import hmac
import html
import json
import os
import re
import sqlite3
import sys
import threading
from urllib.parse import parse_qs, urlsplit
from zoneinfo import ZoneInfo

XML_BODY = b'''<?xml version="1.0" encoding="UTF-8"?>
<events>
  <url id="splash_slx"
       swe="startup/ngpsplash_se.jpg"
       eng="startup/ngpsplash_eng.jpg"
       dan="startup/ngpsplash_se.jpg"
       nor="startup/ngpsplash_no.jpg"/>
  <url id="splash_ngp"
       swe="startup/ngpsplash_se.jpg"
       eng="startup/ngpsplash_eng.jpg"
       dan="startup/ngpsplash_se.jpg"
       nor="startup/ngpsplash_no.jpg"/>
  <url id="login_background"
       swe="startup/loginbackdrop.jpg"
       eng="startup/loginbackdrop_eng.jpg"
       dan="startup/loginbackdrop_eng.jpg"
       nor="startup/loginbackdrop_eng.jpg"/>
  <url id="login_newspic"
       swe="startup/accountbg.png"
       eng="startup/accountbg.png"
       dan="startup/accountbg.png"
       nor="startup/accountbg.png"/>
  <url id="login_foreground"
       swe="local/default.png"
       eng="local/default.png"
       dan="local/default.png"
       nor="local/default.png"/>
  <url id="newspic"
       swe="startup/accountbg.png"
       eng="startup/accountbg.png"
       dan="startup/accountbg.png"
       nor="startup/accountbg.png"/>
</events>
'''

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
SEED_PATH = os.environ.get("NORD_RANDOMSEED_PATH", os.path.join(BASE_DIR, "randomseed.dat"))
DB_PATH = os.environ.get("NORD_DB_PATH", os.path.join(BASE_DIR, "nord-data.sqlite"))
UPLOAD_DIR = os.environ.get("NORD_UPLOAD_DIR", os.path.join(BASE_DIR, "uploads"))
TRACKING_URLS_ENV = os.environ.get("NORD_TRACKING_URLS", "").strip()
DYNAMIC_EVENTS_XML_PATH = os.environ.get("NORD_DYNAMIC_EVENTS_XML_PATH", "").strip()
DYNAMIC_EVENTS_XML_INLINE = os.environ.get("NORD_DYNAMIC_EVENTS_XML_INLINE", "").strip()
SERVER_TIME_TZ = ZoneInfo("Europe/Stockholm")
FORUM_PAGE_SIZE = 20
SEARCH_PAGE_SIZE = 20
ROLE_ACHIEVEMENT_CATEGORY = 6
ROLE_ACHIEVEMENT_ADMIN_TYPE = 1
ROLE_ACHIEVEMENT_MODERATOR_TYPE = 2
PAYMENT_SUCCESS_STATES = {"ok", "success", "credited", "complete", "completed", "approved", "paid"}
PAYMENT_LIST_LIMIT_DEFAULT = 50
PAYMENT_LIST_LIMIT_MAX = 200
PAYMENT_GENERIC_FIELD_KEYS = {
    "event": (
        "eventId", "event_id", "providerEventId", "provider_event_id",
        "transactionId", "transaction_id", "txid", "orderId", "order_id", "id",
    ),
    "player": ("playerId", "player_id", "userId", "user_id", "userid", "uid"),
    "username": ("username", "userName", "playerName", "player_name"),
    "credits": ("credits", "amount", "coins", "reward", "slx", "points"),
    "currency": ("currency", "currencyCode", "currency_code"),
    "status": ("status", "state", "result"),
    "signature": ("signature", "sig", "hash", "token_hash"),
}
PAYMENT_PROVIDER_FIELD_TEMPLATES = {
    "sponsorpay": {
        "event": ("transaction_id", "transactionId", "transid"),
        "player": ("uid", "user_id", "playerId"),
        "credits": ("amount", "credits"),
        "status": ("status", "result"),
        "signature": ("sid", "signature", "sig"),
    },
    "superrewards": {
        "event": ("transaction_id", "transactionId", "id"),
        "player": ("userid", "user_id", "playerId"),
        "credits": ("points", "amount", "credits"),
        "status": ("status", "state", "result"),
        "signature": ("signature", "sig", "hash"),
    },
    "paypal": {
        "event": ("txn_id", "transaction_id", "transactionId", "id"),
        "player": ("custom", "playerId", "userid"),
        "credits": ("amount", "credits"),
        "currency": ("mc_currency", "currency"),
        "status": ("payment_status", "status", "state"),
        "signature": ("verify_sign", "signature", "sig"),
    },
    "paynova": {
        "event": ("transaction_id", "order_id", "id"),
        "player": ("userid", "playerId"),
        "credits": ("amount", "credits"),
        "status": ("status", "state", "result"),
        "signature": ("signature", "sig", "hash"),
    },
    "mopay": {
        "event": ("transaction_id", "txid", "id"),
        "player": ("userid", "playerId"),
        "credits": ("amount", "credits"),
        "status": ("status", "state", "result"),
        "signature": ("signature", "sig", "hash"),
    },
}

DEFAULT_FACE_IMAGE_GIF = base64.b64decode("R0lGODlhAQABAIAAAAAAAP///ywAAAAAAQABAAACAUwAOw==")


def normalize_provider_name(value):
    provider = str(value or "").strip().lower()
    return re.sub(r"[^a-z0-9_-]+", "", provider)


def parse_enabled_env(name, default=False):
    raw = os.environ.get(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


def parse_provider_set(raw, fallback):
    if not raw:
        return set(fallback)
    values = set()
    for part in re.split(r"[,;\s]+", str(raw).strip()):
        provider = normalize_provider_name(part)
        if provider:
            values.add(provider)
    return values or set(fallback)


PAYMENTS_CALLBACK_ENABLED = parse_enabled_env("NORD_PAYMENTS_CALLBACK_ENABLED", default=False)
PAYMENTS_CALLBACK_TOKEN = os.environ.get("NORD_PAYMENTS_CALLBACK_TOKEN", "").strip()
PAYMENTS_RECONCILE_TOKEN = os.environ.get("NORD_PAYMENTS_RECONCILE_TOKEN", PAYMENTS_CALLBACK_TOKEN).strip()
PAYMENTS_ALLOWED_PROVIDERS = parse_provider_set(
    os.environ.get("NORD_PAYMENTS_ALLOWED_PROVIDERS", ""),
    ("sponsorpay", "superrewards", "paypal", "paynova", "mopay"),
)
PAYMENTS_SIGNATURE_MODE_DEFAULT = os.environ.get("NORD_PAYMENTS_SIGNATURE_MODE", "").strip().lower()
PAYMENTS_SIGNATURE_SECRET_DEFAULT = os.environ.get("NORD_PAYMENTS_SIGNATURE_SECRET", "").strip()
HTTP_TRACE_UNKNOWN = parse_enabled_env("NORD_HTTP_TRACE_UNKNOWN", default=False)
try:
    _http_trace_body_limit = int(os.environ.get("NORD_HTTP_TRACE_BODY_LIMIT", "512") or "512")
except Exception:
    _http_trace_body_limit = 512
HTTP_TRACE_BODY_LIMIT = max(128, _http_trace_body_limit)


def parse_env_positive_int(name, default_value, minimum=1):
    raw = os.environ.get(name, str(default_value)).strip()
    try:
        parsed = int(raw)
    except Exception:
        parsed = int(default_value)
    return max(minimum, parsed)


def parse_allowed_upload_types(raw):
    allowed = set()
    for part in re.split(r"[,;\s]+", str(raw or "").strip().lower()):
        token = part.strip()
        if token == "":
            continue
        if token == "jpg":
            token = "jpeg"
        if token in ("jpeg", "png"):
            allowed.add(token)
    return allowed or {"jpeg"}


UPLOAD_IMAGE_MAX_BYTES = parse_env_positive_int("NORD_UPLOAD_IMAGE_MAX_BYTES", 8 * 1024 * 1024)
UPLOAD_IMAGE_MAX_WIDTH = parse_env_positive_int("NORD_UPLOAD_IMAGE_MAX_WIDTH", 4096)
UPLOAD_IMAGE_MAX_HEIGHT = parse_env_positive_int("NORD_UPLOAD_IMAGE_MAX_HEIGHT", 4096)
UPLOAD_IMAGE_MAX_PIXELS = parse_env_positive_int("NORD_UPLOAD_IMAGE_MAX_PIXELS", 16_777_216)
UPLOAD_IMAGE_ALLOWED_TYPES = parse_allowed_upload_types(
    os.environ.get("NORD_UPLOAD_IMAGE_ALLOWED_TYPES", "jpeg,jpg")
)


def trace_unknown_http(method, path, query, content_type="", body=b"", note=""):
    if not HTTP_TRACE_UNKNOWN:
        return
    query_keys = sorted((query or {}).keys())
    preview = ""
    if body:
        raw_preview = body[:HTTP_TRACE_BODY_LIMIT]
        preview = raw_preview.decode("utf-8", errors="replace").replace("\n", "\\n")
    sys.stdout.write(
        "[TRACE_UNKNOWN_HTTP] "
        f"method={method} path={path} queryKeys={query_keys} "
        f"contentType={content_type!r} bodyPreview={preview!r} note={note!r}\n"
    )
    sys.stdout.flush()


def load_seed_bytes():
    if not os.path.isfile(SEED_PATH):
        return None
    with open(SEED_PATH, "rb") as f:
        return f.read()


def db_connect():
    conn = sqlite3.connect(DB_PATH, timeout=5.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


def init_schema():
    os.makedirs(UPLOAD_DIR, exist_ok=True)

    with db_connect() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS forum_categories (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                sort_order INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS forums (
                id INTEGER PRIMARY KEY,
                category_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                sort_order INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY(category_id) REFERENCES forum_categories(id)
            );

            CREATE TABLE IF NOT EXISTS forum_threads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                forum_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                player_id INTEGER NOT NULL,
                username TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                sticky INTEGER NOT NULL DEFAULT 0,
                locked INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY(forum_id) REFERENCES forums(id)
            );

            CREATE TABLE IF NOT EXISTS forum_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                thread_id INTEGER NOT NULL,
                player_id INTEGER NOT NULL,
                username TEXT NOT NULL,
                sex TEXT NOT NULL DEFAULT 'male',
                image TEXT NOT NULL DEFAULT 'local/default.png',
                message TEXT NOT NULL,
                written INTEGER NOT NULL,
                FOREIGN KEY(thread_id) REFERENCES forum_threads(id)
            );

            CREATE TABLE IF NOT EXISTS forum_reads (
                player_id INTEGER NOT NULL,
                forum_id INTEGER NOT NULL,
                lastread INTEGER NOT NULL,
                PRIMARY KEY (player_id, forum_id),
                FOREIGN KEY(forum_id) REFERENCES forums(id)
            );

            CREATE TABLE IF NOT EXISTS forum_thread_reads (
                player_id INTEGER NOT NULL,
                thread_id INTEGER NOT NULL,
                lastread INTEGER NOT NULL,
                PRIMARY KEY (player_id, thread_id),
                FOREIGN KEY(thread_id) REFERENCES forum_threads(id)
            );

            CREATE TABLE IF NOT EXISTS uploaded_pictures (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_id INTEGER NOT NULL,
                server_name TEXT NOT NULL,
                image_path TEXT NOT NULL,
                thumbnail_path TEXT NOT NULL DEFAULT '',
                created_at INTEGER NOT NULL,
                checksum_valid INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS track_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                query_data TEXT,
                remote_addr TEXT,
                user_agent TEXT,
                created_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS legacy_web_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                path TEXT NOT NULL,
                query_data TEXT,
                remote_addr TEXT,
                user_agent TEXT,
                created_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS payment_callbacks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                provider TEXT NOT NULL,
                provider_event_id TEXT NOT NULL,
                player_id INTEGER NOT NULL DEFAULT 0,
                username TEXT NOT NULL DEFAULT '',
                credits INTEGER NOT NULL DEFAULT 0,
                currency TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT '',
                applied INTEGER NOT NULL DEFAULT 0,
                applied_at INTEGER NOT NULL DEFAULT 0,
                total_credits_after INTEGER NOT NULL DEFAULT 0,
                error TEXT NOT NULL DEFAULT '',
                payload_json TEXT NOT NULL DEFAULT '',
                payload_hash TEXT NOT NULL DEFAULT '',
                received_at INTEGER NOT NULL DEFAULT 0,
                UNIQUE(provider, provider_event_id)
            );

            CREATE TABLE IF NOT EXISTS companion_ownership (
                player_id INTEGER NOT NULL,
                companion_kind TEXT NOT NULL,
                companion_type INTEGER NOT NULL,
                acquired_at INTEGER NOT NULL DEFAULT 0,
                source TEXT NOT NULL DEFAULT '',
                PRIMARY KEY(player_id, companion_kind, companion_type),
                FOREIGN KEY(player_id) REFERENCES users(user_id) ON DELETE CASCADE
            );

            """
        )
        try:
            conn.execute("ALTER TABLE uploaded_pictures ADD COLUMN thumbnail_path TEXT NOT NULL DEFAULT ''")
        except sqlite3.Error:
            # Column already exists on upgraded databases.
            pass

        conn.execute("CREATE INDEX IF NOT EXISTS idx_payment_callbacks_received ON payment_callbacks(received_at DESC, id DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_payment_callbacks_applied ON payment_callbacks(applied, received_at DESC, id DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_payment_callbacks_player ON payment_callbacks(player_id, received_at DESC, id DESC)")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_companion_ownership_player "
            "ON companion_ownership(player_id, companion_kind, companion_type)"
        )

        existing_categories = conn.execute("SELECT COUNT(1) FROM forum_categories").fetchone()[0]
        if existing_categories == 0:
            conn.executemany(
                "INSERT INTO forum_categories(id, name, sort_order) VALUES (?, ?, ?)",
                [
                    (1, "Nord Forums", 0),
                    (2, "Community", 1),
                ],
            )
            conn.executemany(
                "INSERT INTO forums(id, category_id, name, description, sort_order) VALUES (?, ?, ?, ?, ?)",
                [
                    (1, 1, "Announcements", "Server news and updates", 0),
                    (2, 2, "General Discussion", "Talk about anything Nord-related", 0),
                    (3, 2, "Help", "Questions and support", 1),
                ],
            )


def now_ms():
    return int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)


def first_query_value(query, *keys):
    for key in keys:
        values = query.get(key)
        if not values:
            continue
        value = str(values[0]).strip()
        if value != "":
            return value
    return ""


def log_legacy_web_request(path, query_data, remote_addr, user_agent):
    try:
        with db_connect() as conn:
            conn.execute(
                "INSERT INTO legacy_web_requests(path, query_data, remote_addr, user_agent, created_at) VALUES (?, ?, ?, ?, ?)",
                (path, query_data, remote_addr, user_agent, now_ms()),
            )
    except Exception:
        # Keep legacy page compatibility resilient even if logging fails.
        pass


def legacy_page_html(title, headline, description, metadata):
    details = ""
    if metadata:
        lines = []
        for key, value in metadata:
            lines.append(
                f"<li><b>{html.escape(str(key))}</b>: {html.escape(str(value))}</li>"
            )
        details = "<ul>" + "".join(lines) + "</ul>"
    return f"""<!doctype html>
<html>
  <head><meta charset="utf-8"><title>{html.escape(title)}</title></head>
  <body style="font-family:sans-serif;background:#10151d;color:#f1f5f9;margin:0;padding:20px;">
    <h1 style="margin:0 0 8px 0;">{html.escape(headline)}</h1>
    <p style="margin:0 0 16px 0;color:#cbd5e1;">{html.escape(description)}</p>
    {details}
  </body>
</html>
"""


def guess_legacy_static_content_type(filename):
    name = str(filename or "").lower()
    if name.endswith(".jnlp"):
        return "application/x-java-jnlp-file"
    if name.endswith(".jar"):
        return "application/java-archive"
    if name.endswith(".jpg") or name.endswith(".jpeg"):
        return "image/jpeg"
    if name.endswith(".gif"):
        return "image/gif"
    if name.endswith(".png"):
        return "image/png"
    if name.endswith(".xml"):
        return "text/xml; charset=utf-8"
    if name.endswith(".txt"):
        return "text/plain; charset=utf-8"
    return "application/octet-stream"


def normalize_page(value):
    try:
        page = int(value)
    except Exception as exc:
        raise ValueError("page must be an integer") from exc
    if page < 1:
        page = 1
    return page


def parse_int(payload, key, *, required=True, default=None):
    value = payload.get(key, default)
    if value is None:
        if required:
            raise ValueError(f"{key} is required")
        return None
    try:
        return int(value)
    except Exception as exc:
        raise ValueError(f"{key} must be an integer") from exc


def parse_bool(payload, key, *, required=True, default=False):
    value = payload.get(key)
    if value is None:
        if required:
            raise ValueError(f"{key} is required")
        return bool(default)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in ("true", "1", "yes", "on"):
            return True
        if lowered in ("false", "0", "no", "off"):
            return False
    raise ValueError(f"{key} must be a boolean")


def parse_text(payload, key, *, required=True):
    value = payload.get(key)
    if value is None:
        if required:
            raise ValueError(f"{key} is required")
        return ""
    text = str(value).strip()
    if required and text == "":
        raise ValueError(f"{key} must not be empty")
    return text


def parse_positive_int(value):
    if value is None:
        return 0
    text = str(value).strip()
    if text == "":
        return 0
    try:
        return max(0, int(text))
    except Exception:
        try:
            return max(0, int(float(text)))
        except Exception:
            return 0


def parse_optional_bool(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    lowered = str(value).strip().lower()
    if lowered in ("1", "true", "yes", "on"):
        return True
    if lowered in ("0", "false", "no", "off"):
        return False
    return None


def first_payload_value(payload, *keys):
    for key in keys:
        if key in payload:
            value = payload.get(key)
            if value is not None and str(value).strip() != "":
                return value
    return None


def provider_env_suffix(provider):
    normalized = normalize_provider_name(provider)
    if not normalized:
        return ""
    return normalized.upper().replace("-", "_")


def get_provider_signature_mode(provider):
    suffix = provider_env_suffix(provider)
    if suffix:
        raw = os.environ.get(f"NORD_PAYMENTS_SIGNATURE_MODE_{suffix}", "").strip().lower()
        if raw:
            return raw
    return PAYMENTS_SIGNATURE_MODE_DEFAULT


def get_provider_signature_secret(provider):
    suffix = provider_env_suffix(provider)
    if suffix:
        raw = os.environ.get(f"NORD_PAYMENTS_SIGNATURE_SECRET_{suffix}", "").strip()
        if raw:
            return raw
    return PAYMENTS_SIGNATURE_SECRET_DEFAULT


def payment_field_keys(provider, field_name):
    provider_name = normalize_provider_name(provider)
    provider_map = PAYMENT_PROVIDER_FIELD_TEMPLATES.get(provider_name, {})
    provider_keys = provider_map.get(field_name, ())
    generic_keys = PAYMENT_GENERIC_FIELD_KEYS.get(field_name, ())
    combined = []
    for key in provider_keys:
        if key not in combined:
            combined.append(key)
    for key in generic_keys:
        if key not in combined:
            combined.append(key)
    return tuple(combined)


def resolve_payment_field(payload, provider, field_name):
    keys = payment_field_keys(provider, field_name)
    if not keys:
        return None
    return first_payload_value(payload, *keys)


def extract_payment_signature(handler, query, payload, provider):
    header_signature = handler.headers.get("X-Nord-Payments-Signature", "").strip()
    if header_signature:
        return header_signature
    alternate_header_signature = handler.headers.get("X-Signature", "").strip()
    if alternate_header_signature:
        return alternate_header_signature
    for key in payment_field_keys(provider, "signature"):
        query_signature = query.get(key, [""])[0].strip()
        if query_signature:
            return query_signature
    for key in payment_field_keys(provider, "signature"):
        payload_signature = str(payload.get(key, "")).strip()
        if payload_signature:
            return payload_signature
    return ""


def compute_payment_signature(mode, secret, raw_body_bytes, canonical_payload_json):
    normalized_mode = (mode or "").strip().lower()
    raw = raw_body_bytes or b""
    secret_bytes = (secret or "").encode("utf-8")
    if normalized_mode in ("hmac-sha256", "hmac_sha256", "payload-hmac-sha256"):
        return hmac.new(secret_bytes, raw, hashlib.sha256).hexdigest()
    if normalized_mode in ("hmac-sha1", "hmac_sha1", "payload-hmac-sha1"):
        return hmac.new(secret_bytes, raw, hashlib.sha1).hexdigest()
    if normalized_mode in ("payload-sha256", "sha256"):
        return hashlib.sha256(secret_bytes + b":" + raw).hexdigest()
    if normalized_mode in ("json-hmac-sha256", "payload-json-hmac-sha256"):
        message = (canonical_payload_json or "").encode("utf-8")
        return hmac.new(secret_bytes, message, hashlib.sha256).hexdigest()
    raise ValueError(f"Unsupported payment signature mode: {mode}")


def verify_payment_signature(handler, query, payload, provider, raw_body_bytes, canonical_payload_json):
    mode = get_provider_signature_mode(provider)
    secret = get_provider_signature_secret(provider)
    if not mode and not secret:
        return True, ""
    if not mode:
        mode = "hmac-sha256"
    if not secret:
        return False, f"missing signature secret for provider '{provider}'"

    provided_signature = extract_payment_signature(handler, query, payload, provider)
    if not provided_signature:
        return False, "missing callback signature"

    expected_signature = compute_payment_signature(mode, secret, raw_body_bytes, canonical_payload_json)
    if not hmac.compare_digest(provided_signature.strip().lower(), expected_signature.strip().lower()):
        return False, "invalid callback signature"
    return True, ""


def payload_without_signature_fields(payload, provider):
    if not isinstance(payload, dict):
        return {}
    filtered = {}
    signature_keys = set(payment_field_keys(provider, "signature"))
    for key, value in payload.items():
        if key in signature_keys:
            continue
        filtered[key] = value
    return filtered


def get_user_profile(conn, player_id):
    row = None
    try:
        row = conn.execute("SELECT username FROM users WHERE user_id = ?", (player_id,)).fetchone()
    except sqlite3.Error:
        row = None
    if row is None:
        return {
            "username": f"Player {player_id}",
            "sex": "male",
            "image": "local/default.png",
        }
    return {
        "username": row["username"],
        "sex": "male",
        "image": "local/default.png",
    }


def upsert_forum_lastread(conn, player_id, forum_id, timestamp):
    conn.execute(
        """
        INSERT INTO forum_reads(player_id, forum_id, lastread)
        VALUES (?, ?, ?)
        ON CONFLICT(player_id, forum_id) DO UPDATE SET lastread = excluded.lastread
        """,
        (player_id, forum_id, timestamp),
    )


def upsert_thread_lastread(conn, player_id, thread_id, timestamp):
    conn.execute(
        """
        INSERT INTO forum_thread_reads(player_id, thread_id, lastread)
        VALUES (?, ?, ?)
        ON CONFLICT(player_id, thread_id) DO UPDATE SET lastread = excluded.lastread
        """,
        (player_id, thread_id, timestamp),
    )


def total_pages(total_rows, page_size):
    return max(1, (total_rows + page_size - 1) // page_size)


def build_highlight(source_text, query):
    collapsed = re.sub(r"\s+", " ", source_text).strip()
    if not collapsed:
        return ""

    match = re.search(re.escape(query), collapsed, flags=re.IGNORECASE)
    if match is None:
        return collapsed[:140]

    start = max(0, match.start() - 50)
    end = min(len(collapsed), match.end() + 70)
    prefix = "..." if start > 0 else ""
    suffix = "..." if end < len(collapsed) else ""
    snippet = collapsed[start:end]

    def replacer(found):
        return f"<b>{found.group(0)}</b>"

    highlighted = re.sub(re.escape(query), replacer, snippet, count=1, flags=re.IGNORECASE)
    return prefix + highlighted + suffix


def can_moderate_forum(conn, player_id):
    try:
        row = conn.execute(
            """
            SELECT 1
            FROM achievements
            WHERE player_id = ?
              AND category = ?
              AND type IN (?, ?)
              AND value > 0
            LIMIT 1
            """,
            (
                player_id,
                ROLE_ACHIEVEMENT_CATEGORY,
                ROLE_ACHIEVEMENT_ADMIN_TYPE,
                ROLE_ACHIEVEMENT_MODERATOR_TYPE,
            ),
        ).fetchone()
        return row is not None
    except sqlite3.Error:
        return False


def require_forum_moderator(conn, player_id):
    if not can_moderate_forum(conn, player_id):
        raise ValueError("Moderator permissions required")


def forum_list_forums(conn, payload):
    player_id = parse_int(payload, "playerID")

    categories = conn.execute(
        "SELECT id, name FROM forum_categories ORDER BY sort_order, id"
    ).fetchall()

    forums = conn.execute(
        """
        SELECT
            f.id,
            f.category_id,
            f.name,
            f.description,
            MAX(t.updated_at) AS lastwrite,
            fr.lastread AS lastread
        FROM forums f
        LEFT JOIN forum_threads t ON t.forum_id = f.id
        LEFT JOIN forum_reads fr ON fr.forum_id = f.id AND fr.player_id = ?
        GROUP BY f.id
        ORDER BY f.sort_order, f.id
        """,
        (player_id,),
    ).fetchall()

    forums_by_category = {}
    for forum in forums:
        forums_by_category.setdefault(forum["category_id"], []).append(
            {
                "id": int(forum["id"]),
                "name": forum["name"],
                "description": forum["description"],
                "lastwrite": int(forum["lastwrite"]) if forum["lastwrite"] is not None else None,
                "lastread": int(forum["lastread"]) if forum["lastread"] is not None else None,
            }
        )

    response_categories = []
    for category in categories:
        response_categories.append(
            {
                "id": int(category["id"]),
                "name": category["name"],
                "forums": forums_by_category.get(category["id"], []),
            }
        )

    return {"categories": response_categories}


def forum_list_threads(conn, payload):
    player_id = parse_int(payload, "playerID")
    forum_id = parse_int(payload, "forum")
    page = normalize_page(payload.get("page", 1))

    forum_row = conn.execute(
        "SELECT id, name FROM forums WHERE id = ?",
        (forum_id,),
    ).fetchone()
    if forum_row is None:
        raise ValueError("Unknown forum")

    now = now_ms()
    upsert_forum_lastread(conn, player_id, forum_id, now)

    total = conn.execute(
        "SELECT COUNT(*) FROM forum_threads WHERE forum_id = ?",
        (forum_id,),
    ).fetchone()[0]
    nrpages = total_pages(total, FORUM_PAGE_SIZE)
    page = min(page, nrpages)
    offset = (page - 1) * FORUM_PAGE_SIZE

    rows = conn.execute(
        """
        SELECT
            t.id,
            t.name,
            t.updated_at AS lastwrite,
            t.locked,
            t.sticky,
            COALESCE(m.msg_count, 0) AS nrmessages,
            tr.lastread
        FROM forum_threads t
        LEFT JOIN (
            SELECT thread_id, COUNT(*) AS msg_count
            FROM forum_messages
            GROUP BY thread_id
        ) m ON m.thread_id = t.id
        LEFT JOIN forum_thread_reads tr ON tr.thread_id = t.id AND tr.player_id = ?
        WHERE t.forum_id = ?
        ORDER BY t.sticky DESC, t.updated_at DESC, t.id DESC
        LIMIT ? OFFSET ?
        """,
        (player_id, forum_id, FORUM_PAGE_SIZE, offset),
    ).fetchall()

    threads = []
    for row in rows:
        threads.append(
            {
                "id": int(row["id"]),
                "name": row["name"],
                "lastwrite": int(row["lastwrite"]),
                "lastread": int(row["lastread"]) if row["lastread"] is not None else None,
                "locked": bool(row["locked"]),
                "sticky": bool(row["sticky"]),
                "nrmessages": int(row["nrmessages"]),
            }
        )

    return {
        "forum": int(forum_row["id"]),
        "forumname": forum_row["name"],
        "page": page,
        "nrpages": nrpages,
        "canmoderate": can_moderate_forum(conn, player_id),
        "cancreate": True,
        "threads": threads,
    }


def forum_list_messages(conn, payload):
    player_id = parse_int(payload, "playerID")
    thread_id = parse_int(payload, "thread")
    page = normalize_page(payload.get("page", 1))

    thread_row = conn.execute(
        """
        SELECT t.id, t.name AS threadname, t.forum_id, t.locked, f.name AS forumname
        FROM forum_threads t
        JOIN forums f ON f.id = t.forum_id
        WHERE t.id = ?
        """,
        (thread_id,),
    ).fetchone()
    if thread_row is None:
        raise ValueError("Unknown thread")

    now = now_ms()
    upsert_thread_lastread(conn, player_id, thread_id, now)
    upsert_forum_lastread(conn, player_id, int(thread_row["forum_id"]), now)

    total = conn.execute(
        "SELECT COUNT(*) FROM forum_messages WHERE thread_id = ?",
        (thread_id,),
    ).fetchone()[0]
    nrpages = total_pages(total, FORUM_PAGE_SIZE)
    page = min(page, nrpages)
    offset = (page - 1) * FORUM_PAGE_SIZE

    rows = conn.execute(
        """
        SELECT id, message, image, username, player_id, written, sex
        FROM forum_messages
        WHERE thread_id = ?
        ORDER BY written ASC, id ASC
        LIMIT ? OFFSET ?
        """,
        (thread_id, FORUM_PAGE_SIZE, offset),
    ).fetchall()

    messages = []
    for row in rows:
        messages.append(
            {
                "id": int(row["id"]),
                "message": row["message"],
                "image": row["image"] or "local/default.png",
                "username": row["username"],
                "playerID": int(row["player_id"]),
                "online": False,
                "written": int(row["written"]),
                "sex": row["sex"] or "male",
            }
        )

    return {
        "forum": int(thread_row["forum_id"]),
        "thread": int(thread_row["id"]),
        "threadname": thread_row["threadname"],
        "forumname": thread_row["forumname"],
        "page": page,
        "nrpages": nrpages,
        "canmoderate": can_moderate_forum(conn, player_id),
        "locked": bool(thread_row["locked"]),
        "messages": messages,
    }


def forum_add_message(conn, payload):
    player_id = parse_int(payload, "playerID")
    message = parse_text(payload, "message")
    profile = get_user_profile(conn, player_id)
    timestamp = now_ms()

    thread_id_value = payload.get("thread")
    if thread_id_value is not None:
        thread_id = parse_int(payload, "thread")
        thread_row = conn.execute(
            "SELECT id, forum_id, locked FROM forum_threads WHERE id = ?",
            (thread_id,),
        ).fetchone()
        if thread_row is None:
            raise ValueError("Unknown thread")
        if bool(thread_row["locked"]):
            raise ValueError("Thread is locked")

        conn.execute(
            """
            INSERT INTO forum_messages(thread_id, player_id, username, sex, image, message, written)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                thread_id,
                player_id,
                profile["username"],
                profile["sex"],
                profile["image"],
                message,
                timestamp,
            ),
        )
        conn.execute(
            "UPDATE forum_threads SET updated_at = ? WHERE id = ?",
            (timestamp, thread_id),
        )
        upsert_thread_lastread(conn, player_id, thread_id, timestamp)
        upsert_forum_lastread(conn, player_id, int(thread_row["forum_id"]), timestamp)
        return {"ok": True}

    thread_name = parse_text(payload, "name")
    forum_id = parse_int(payload, "forum")
    forum_row = conn.execute("SELECT id FROM forums WHERE id = ?", (forum_id,)).fetchone()
    if forum_row is None:
        raise ValueError("Unknown forum")

    cursor = conn.execute(
        """
        INSERT INTO forum_threads(forum_id, name, player_id, username, created_at, updated_at, sticky, locked)
        VALUES (?, ?, ?, ?, ?, ?, 0, 0)
        """,
        (forum_id, thread_name, player_id, profile["username"], timestamp, timestamp),
    )
    thread_id = int(cursor.lastrowid)

    conn.execute(
        """
        INSERT INTO forum_messages(thread_id, player_id, username, sex, image, message, written)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (thread_id, player_id, profile["username"], profile["sex"], profile["image"], message, timestamp),
    )
    upsert_thread_lastread(conn, player_id, thread_id, timestamp)
    upsert_forum_lastread(conn, player_id, forum_id, timestamp)
    return {"thread": thread_id}


def forum_search(conn, payload):
    _player_id = parse_int(payload, "playerID")
    query = parse_text(payload, "query", required=False)
    page = normalize_page(payload.get("page", 1))

    if query == "":
        return {"query": "", "page": 1, "nrpages": 1, "matches": []}

    pattern = f"%{query}%"
    total = conn.execute(
        """
        SELECT COUNT(*)
        FROM forum_messages m
        JOIN forum_threads t ON t.id = m.thread_id
        WHERE m.message LIKE ? COLLATE NOCASE
           OR t.name LIKE ? COLLATE NOCASE
           OR m.username LIKE ? COLLATE NOCASE
        """,
        (pattern, pattern, pattern),
    ).fetchone()[0]
    nrpages = total_pages(total, SEARCH_PAGE_SIZE)
    page = min(page, nrpages)
    offset = (page - 1) * SEARCH_PAGE_SIZE

    rows = conn.execute(
        """
        SELECT
            m.thread_id AS id,
            t.name AS threadname,
            m.message,
            m.image,
            m.username,
            m.player_id,
            m.written
        FROM forum_messages m
        JOIN forum_threads t ON t.id = m.thread_id
        WHERE m.message LIKE ? COLLATE NOCASE
           OR t.name LIKE ? COLLATE NOCASE
           OR m.username LIKE ? COLLATE NOCASE
        ORDER BY m.written DESC, m.id DESC
        LIMIT ? OFFSET ?
        """,
        (pattern, pattern, pattern, SEARCH_PAGE_SIZE, offset),
    ).fetchall()

    matches = []
    for row in rows:
        source = row["message"] if row["message"] else row["threadname"]
        matches.append(
            {
                "id": int(row["id"]),
                "highlight": build_highlight(source, query),
                "image": row["image"] or "local/default.png",
                "username": row["username"],
                "playerID": int(row["player_id"]),
                "online": False,
                "written": int(row["written"]),
                "threadname": row["threadname"],
            }
        )

    return {
        "query": query,
        "page": page,
        "nrpages": nrpages,
        "matches": matches,
    }


def forum_sticky_thread(conn, payload):
    player_id = parse_int(payload, "playerID")
    thread_id = parse_int(payload, "thread")
    sticky = parse_bool(payload, "sticky")
    require_forum_moderator(conn, player_id)

    result = conn.execute(
        "UPDATE forum_threads SET sticky = ? WHERE id = ?",
        (1 if sticky else 0, thread_id),
    )
    if result.rowcount == 0:
        raise ValueError("Unknown thread")
    return {"ok": True}


def forum_lock_thread(conn, payload):
    player_id = parse_int(payload, "playerID")
    thread_id = parse_int(payload, "thread")
    locked = parse_bool(payload, "locked")
    require_forum_moderator(conn, player_id)

    result = conn.execute(
        "UPDATE forum_threads SET locked = ? WHERE id = ?",
        (1 if locked else 0, thread_id),
    )
    if result.rowcount == 0:
        raise ValueError("Unknown thread")
    return {"ok": True}


def forum_delete_message(conn, payload):
    player_id = parse_int(payload, "playerID")
    message_id = parse_int(payload, "id")
    thread_id = parse_int(payload, "thread")

    row = conn.execute(
        """
        SELECT id, thread_id, player_id
        FROM forum_messages
        WHERE id = ? AND thread_id = ?
        """,
        (message_id, thread_id),
    ).fetchone()
    if row is None:
        raise ValueError("Unknown message")

    can_manage = player_id == int(row["player_id"]) or can_moderate_forum(conn, player_id)
    if not can_manage:
        raise ValueError("Not allowed to remove this message")

    conn.execute(
        "DELETE FROM forum_messages WHERE id = ? AND thread_id = ?",
        (message_id, thread_id),
    )

    remaining = conn.execute(
        "SELECT COUNT(*) FROM forum_messages WHERE thread_id = ?",
        (thread_id,),
    ).fetchone()[0]
    if remaining == 0:
        conn.execute("DELETE FROM forum_thread_reads WHERE thread_id = ?", (thread_id,))
        conn.execute("DELETE FROM forum_threads WHERE id = ?", (thread_id,))
        return {"removedthread": True}

    last_write = conn.execute(
        "SELECT MAX(written) AS lastwrite FROM forum_messages WHERE thread_id = ?",
        (thread_id,),
    ).fetchone()
    updated_at = int(last_write["lastwrite"]) if last_write and last_write["lastwrite"] is not None else now_ms()
    conn.execute(
        "UPDATE forum_threads SET updated_at = ? WHERE id = ?",
        (updated_at, thread_id),
    )
    return {"removedthread": False}


def forum_edit_message(conn, payload):
    player_id = parse_int(payload, "playerID")
    message_id = parse_int(payload, "id")
    message = parse_text(payload, "message")

    row = conn.execute(
        """
        SELECT id, player_id
        FROM forum_messages
        WHERE id = ?
        """,
        (message_id,),
    ).fetchone()
    if row is None:
        raise ValueError("Unknown message")

    can_manage = player_id == int(row["player_id"]) or can_moderate_forum(conn, player_id)
    if not can_manage:
        raise ValueError("Not allowed to edit this message")

    conn.execute(
        "UPDATE forum_messages SET message = ? WHERE id = ?",
        (message, message_id),
    )
    return {"ok": True}


def handle_forum_request(payload):
    method = payload.get("method")
    if not method:
        raise ValueError("method is required")

    with db_connect() as conn:
        if method == "listForums":
            return forum_list_forums(conn, payload)
        if method == "listThreads":
            return forum_list_threads(conn, payload)
        if method == "listMessages":
            return forum_list_messages(conn, payload)
        if method == "addMessage":
            return forum_add_message(conn, payload)
        if method == "search":
            return forum_search(conn, payload)
        if method == "stickyThread":
            return forum_sticky_thread(conn, payload)
        if method == "lockThread":
            return forum_lock_thread(conn, payload)
        if method == "deleteMessage":
            return forum_delete_message(conn, payload)
        if method == "editMessage":
            return forum_edit_message(conn, payload)
    raise ValueError(f"Unknown forum method: {method}")


def expected_upload_checksum(player_id_text):
    return hashlib.md5((player_id_text + "thisissomerandomtext").encode("utf-8")).hexdigest()


def presentation_picture_refs_image(picture_url, image_id):
    url_text = str(picture_url or "").strip()
    normalized_image_id = int(image_id or 0)
    if url_text == "" or normalized_image_id <= 0:
        return False

    parsed = urlsplit(url_text)
    path = (parsed.path or url_text).strip()
    if path == "":
        return False

    suffixes = (
        f"/dl/{normalized_image_id}",
        f"/uploads/{normalized_image_id}",
        f"/uploads/{normalized_image_id}.jpg",
    )
    return any(path.endswith(suffix) for suffix in suffixes)


def sniff_image_type(image_bytes):
    data = image_bytes or b""
    if len(data) >= 3 and data[:3] == b"\xff\xd8\xff":
        return "jpeg"
    if len(data) >= 8 and data[:8] == b"\x89PNG\r\n\x1a\n":
        return "png"
    return ""


def extract_png_dimensions(image_bytes):
    data = image_bytes or b""
    if len(data) < 24:
        return 0, 0
    if data[:8] != b"\x89PNG\r\n\x1a\n":
        return 0, 0
    # PNG stores dimensions in the IHDR chunk.
    if data[12:16] != b"IHDR":
        return 0, 0
    width = int.from_bytes(data[16:20], "big", signed=False)
    height = int.from_bytes(data[20:24], "big", signed=False)
    return width, height


def extract_jpeg_dimensions(image_bytes):
    data = image_bytes or b""
    if len(data) < 4 or data[:2] != b"\xff\xd8":
        return 0, 0

    sof_markers = {
        0xC0, 0xC1, 0xC2, 0xC3,
        0xC5, 0xC6, 0xC7,
        0xC9, 0xCA, 0xCB,
        0xCD, 0xCE, 0xCF,
    }
    index = 2
    size = len(data)
    while index + 1 < size:
        while index < size and data[index] != 0xFF:
            index += 1
        while index < size and data[index] == 0xFF:
            index += 1
        if index >= size:
            break
        marker = data[index]
        index += 1
        if marker in (0xD8, 0xD9):
            continue
        if marker == 0xDA:
            break
        if index + 1 >= size:
            break
        segment_len = (data[index] << 8) | data[index + 1]
        if segment_len < 2 or index + segment_len > size:
            return 0, 0
        if marker in sof_markers:
            if segment_len < 7:
                return 0, 0
            height = (data[index + 3] << 8) | data[index + 4]
            width = (data[index + 5] << 8) | data[index + 6]
            return width, height
        index += segment_len
    return 0, 0


def extract_image_dimensions(image_bytes, image_type):
    if image_type == "jpeg":
        return extract_jpeg_dimensions(image_bytes)
    if image_type == "png":
        return extract_png_dimensions(image_bytes)
    return 0, 0


def validate_uploaded_image_bytes(image_bytes, field_name):
    data = image_bytes or b""
    if len(data) == 0:
        return f"Empty {field_name}"
    if len(data) > UPLOAD_IMAGE_MAX_BYTES:
        return (
            f"{field_name} too large ({len(data)} bytes > {UPLOAD_IMAGE_MAX_BYTES} bytes)"
        )

    image_type = sniff_image_type(data)
    if image_type == "":
        return f"Invalid {field_name} format"
    if image_type not in UPLOAD_IMAGE_ALLOWED_TYPES:
        return f"Unsupported {field_name} format ({image_type})"

    width, height = extract_image_dimensions(data, image_type)
    if width <= 0 or height <= 0:
        return f"Invalid {field_name} dimensions"
    if width > UPLOAD_IMAGE_MAX_WIDTH or height > UPLOAD_IMAGE_MAX_HEIGHT:
        return (
            f"{field_name} dimensions too large "
            f"({width}x{height} > {UPLOAD_IMAGE_MAX_WIDTH}x{UPLOAD_IMAGE_MAX_HEIGHT})"
        )
    if width * height > UPLOAD_IMAGE_MAX_PIXELS:
        return (
            f"{field_name} pixel count too large "
            f"({width * height} > {UPLOAD_IMAGE_MAX_PIXELS})"
        )
    return ""


def parse_positive_int_text(text):
    value = str(text or "").strip()
    if not value.isdigit():
        return 0
    try:
        return max(0, int(value))
    except Exception:
        return 0


def resolve_player_id_from_session(conn, session_text):
    session_id = parse_positive_int_text(session_text)
    if session_id <= 0:
        return 0

    # Preferred source: explicit session-to-player rows emitted by the game server.
    try:
        row = conn.execute(
            """
            SELECT player_id
            FROM server_session_parameters
            WHERE session_id = ?
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            (session_id,),
        ).fetchone()
        if row is not None:
            return max(0, int(row["player_id"] or 0))
    except sqlite3.Error:
        pass

    # Compatibility fallback: session captured with twitter-token flow.
    try:
        row = conn.execute(
            """
            SELECT player_id
            FROM twitter_tokens
            WHERE session_id = ?
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (session_id,),
        ).fetchone()
        if row is not None:
            return max(0, int(row["player_id"] or 0))
    except sqlite3.Error:
        pass

    return 0


def build_tracking_urls(handler):
    if TRACKING_URLS_ENV:
        urls = [line.strip() for line in TRACKING_URLS_ENV.splitlines() if line.strip()]
        return urls

    host = handler.headers.get("Host", "127.0.0.1:8080")
    return [f"http://{host}/track/?data=sidecar_ping"]


def load_dynamic_events_xml():
    if DYNAMIC_EVENTS_XML_INLINE:
        return DYNAMIC_EVENTS_XML_INLINE.encode("utf-8")

    if DYNAMIC_EVENTS_XML_PATH:
        try:
            with open(DYNAMIC_EVENTS_XML_PATH, "rb") as f:
                loaded = f.read()
            if loaded.strip():
                return loaded
        except OSError as exc:
            sys.stderr.write(
                f"[nord_server.py] Failed to read NORD_DYNAMIC_EVENTS_XML_PATH='{DYNAMIC_EVENTS_XML_PATH}': {exc}\n"
            )
            sys.stderr.flush()

    return XML_BODY


def parse_multipart_form(content_type, body):
    message_bytes = (
        f"Content-Type: {content_type}\r\n"
        "MIME-Version: 1.0\r\n\r\n"
    ).encode("utf-8") + body
    message = BytesParser(policy=policy.default).parsebytes(message_bytes)
    if not message.is_multipart():
        raise ValueError("Invalid multipart payload")

    fields = {}
    files = {}
    for part in message.iter_parts():
        name = part.get_param("name", header="content-disposition")
        if not name:
            continue
        filename = part.get_filename()
        data = part.get_payload(decode=True) or b""
        if filename is None:
            fields[name] = data.decode("utf-8", errors="replace")
        else:
            files[name] = data
    return fields, files


def parse_payment_payload(content_type, body_bytes):
    raw_text = body_bytes.decode("utf-8", errors="replace")
    lowered_type = (content_type or "").lower()
    if "application/json" in lowered_type:
        payload = json.loads(raw_text or "{}")
        if not isinstance(payload, dict):
            raise ValueError("Payment callback JSON body must be an object")
        return payload

    parsed = parse_qs(raw_text, keep_blank_values=True)
    payload = {}
    for key, values in parsed.items():
        if not values:
            continue
        payload[key] = values[0]
    return payload


def extract_auth_token(handler, query, payload):
    header_token = handler.headers.get("X-Nord-Payments-Token", "").strip()
    if header_token:
        return header_token
    auth_header = handler.headers.get("Authorization", "").strip()
    if auth_header.lower().startswith("bearer "):
        bearer = auth_header[7:].strip()
        if bearer:
            return bearer
    query_token = query.get("token", [""])[0].strip()
    if query_token:
        return query_token
    payload_token = str(payload.get("token", "")).strip()
    if payload_token:
        return payload_token
    return ""


def require_payments_auth(handler, expected_token, query, payload):
    if not expected_token:
        return False
    provided = extract_auth_token(handler, query, payload)
    return provided == expected_token


def normalize_payment_status(status_value, payload):
    status_text = str(status_value or "").strip().lower()
    if status_text == "":
        success_flag = parse_optional_bool(payload.get("success"))
        if success_flag is True:
            return "success"
        if success_flag is False:
            return "failed"
    return status_text


def status_allows_payment_credit(status_text, payload):
    normalized = normalize_payment_status(status_text, payload)
    if normalized in PAYMENT_SUCCESS_STATES:
        return True
    success_flag = parse_optional_bool(payload.get("success"))
    return success_flag is True


def resolve_payment_user(conn, player_id, username):
    safe_player_id = max(0, int(player_id or 0))
    safe_username = str(username or "").strip()
    try:
        if safe_player_id > 0:
            row = conn.execute(
                "SELECT user_id, username FROM users WHERE user_id = ?",
                (safe_player_id,),
            ).fetchone()
            if row is not None:
                return int(row["user_id"]), row["username"]
        if safe_username:
            row = conn.execute(
                "SELECT user_id, username FROM users WHERE lower(username) = lower(?)",
                (safe_username,),
            ).fetchone()
            if row is not None:
                return int(row["user_id"]), row["username"]
    except sqlite3.Error:
        return 0, safe_username
    return 0, safe_username


def process_payment_callback(conn, callback):
    provider = normalize_provider_name(callback.get("provider"))
    event_id = str(callback.get("provider_event_id", "")).strip()
    if provider == "":
        raise ValueError("provider is required")
    if provider not in PAYMENTS_ALLOWED_PROVIDERS:
        raise ValueError(f"provider '{provider}' is not allowed")
    if event_id == "":
        raise ValueError("provider event id is required")

    payload_json = callback.get("payload_json")
    if payload_json is None:
        payload_json = json.dumps(callback.get("payload", {}), sort_keys=True, separators=(",", ":"))
    payload_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
    received_at = int(callback.get("received_at") or now_ms())

    existing = conn.execute(
        """
        SELECT id, player_id, username, credits, currency, status, applied, applied_at, total_credits_after, error
        FROM payment_callbacks
        WHERE provider = ? AND provider_event_id = ?
        """,
        (provider, event_id),
    ).fetchone()
    if existing is not None and int(existing["applied"]) != 0:
        return {
            "ok": True,
            "provider": provider,
            "providerEventId": event_id,
            "duplicate": True,
            "alreadyApplied": True,
            "applied": False,
            "playerId": int(existing["player_id"]),
            "creditsApplied": int(existing["credits"]),
            "totalCreditsAfter": int(existing["total_credits_after"]),
            "status": existing["status"],
            "error": existing["error"] or "",
        }

    status_text = normalize_payment_status(callback.get("status"), callback.get("payload", {}))
    credits = max(0, int(callback.get("credits") or 0))
    callback_player_id = max(0, int(callback.get("player_id") or 0))
    callback_username = str(callback.get("username") or "").strip()
    currency = str(callback.get("currency") or "").strip().upper()
    if currency == "":
        currency = "SLX"

    resolved_player_id, resolved_username = resolve_payment_user(conn, callback_player_id, callback_username)
    record_player_id = resolved_player_id if resolved_player_id > 0 else callback_player_id
    record_username = resolved_username if resolved_player_id > 0 else callback_username
    should_credit = status_allows_payment_credit(status_text, callback.get("payload", {}))
    applied = 0
    applied_at = 0
    total_after = 0
    error = ""

    if not should_credit:
        error = "callback status not crediting"
    elif credits <= 0:
        error = "credits must be positive"
    elif resolved_player_id <= 0:
        error = "player not found"
    else:
        try:
            update_result = conn.execute(
                "UPDATE users SET slx_credits = MAX(0, slx_credits + ?) WHERE user_id = ?",
                (credits, resolved_player_id),
            )
            if update_result.rowcount <= 0:
                error = "player not found"
            else:
                row = conn.execute(
                    "SELECT slx_credits FROM users WHERE user_id = ?",
                    (resolved_player_id,),
                ).fetchone()
                total_after = int(row["slx_credits"]) if row is not None else 0
                applied = 1
                applied_at = now_ms()
        except sqlite3.Error as exc:
            error = f"credit apply failed: {exc}"

    if existing is None:
        conn.execute(
            """
            INSERT INTO payment_callbacks(
                provider, provider_event_id, player_id, username, credits, currency, status,
                applied, applied_at, total_credits_after, error, payload_json, payload_hash, received_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                provider,
                event_id,
                record_player_id,
                record_username,
                credits,
                currency,
                status_text,
                applied,
                applied_at,
                total_after,
                error,
                payload_json,
                payload_hash,
                received_at,
            ),
        )
    else:
        conn.execute(
            """
            UPDATE payment_callbacks
            SET player_id = ?,
                username = ?,
                credits = ?,
                currency = ?,
                status = ?,
                applied = ?,
                applied_at = ?,
                total_credits_after = ?,
                error = ?,
                payload_json = ?,
                payload_hash = ?,
                received_at = ?
            WHERE id = ?
            """,
            (
                record_player_id,
                record_username,
                credits,
                currency,
                status_text,
                applied,
                applied_at,
                total_after,
                error,
                payload_json,
                payload_hash,
                received_at,
                int(existing["id"]),
            ),
        )

    return {
        "ok": applied == 1,
        "provider": provider,
        "providerEventId": event_id,
        "duplicate": existing is not None,
        "alreadyApplied": False,
        "applied": applied == 1,
        "playerId": record_player_id,
        "creditsApplied": credits if applied == 1 else 0,
        "totalCreditsAfter": total_after,
        "status": status_text,
        "error": error,
    }


def handle_payment_callback(conn, provider, payload):
    provider_event_id = resolve_payment_field(payload, provider, "event")
    player_id_value = resolve_payment_field(payload, provider, "player")
    username_value = resolve_payment_field(payload, provider, "username")
    credits_value = resolve_payment_field(payload, provider, "credits")
    currency_value = resolve_payment_field(payload, provider, "currency")
    status_value = resolve_payment_field(payload, provider, "status")

    callback = {
        "provider": provider,
        "provider_event_id": str(provider_event_id or "").strip(),
        "player_id": parse_positive_int(player_id_value),
        "username": str(username_value or "").strip(),
        "credits": parse_positive_int(credits_value),
        "currency": str(currency_value or "").strip(),
        "status": str(status_value or "").strip(),
        "payload": payload,
        "payload_json": json.dumps(payload, sort_keys=True, separators=(",", ":")),
        "received_at": now_ms(),
    }
    return process_payment_callback(conn, callback)


def list_payment_callbacks(conn, provider=None, applied=None, limit=PAYMENT_LIST_LIMIT_DEFAULT):
    safe_limit = max(1, min(PAYMENT_LIST_LIMIT_MAX, int(limit)))
    clauses = []
    params = []
    normalized_provider = normalize_provider_name(provider)
    if normalized_provider:
        clauses.append("provider = ?")
        params.append(normalized_provider)
    if applied is True:
        clauses.append("applied = 1")
    elif applied is False:
        clauses.append("applied = 0")

    where_sql = ""
    if clauses:
        where_sql = " WHERE " + " AND ".join(clauses)

    rows = conn.execute(
        f"""
        SELECT provider, provider_event_id, player_id, username, credits, currency, status,
               applied, applied_at, total_credits_after, error, received_at
        FROM payment_callbacks
        {where_sql}
        ORDER BY received_at DESC, id DESC
        LIMIT ?
        """,
        tuple(params + [safe_limit]),
    ).fetchall()

    summary = conn.execute(
        f"""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN applied != 0 THEN 1 ELSE 0 END) AS applied_count,
            SUM(CASE WHEN applied = 0 THEN 1 ELSE 0 END) AS unapplied_count,
            SUM(CASE WHEN applied = 0 AND error != '' THEN 1 ELSE 0 END) AS error_count
        FROM payment_callbacks
        {where_sql}
        """,
        tuple(params),
    ).fetchone()

    events = []
    for row in rows:
        events.append(
            {
                "provider": row["provider"],
                "providerEventId": row["provider_event_id"],
                "playerId": int(row["player_id"]),
                "username": row["username"],
                "credits": int(row["credits"]),
                "currency": row["currency"],
                "status": row["status"],
                "applied": bool(row["applied"]),
                "appliedAt": int(row["applied_at"]),
                "totalCreditsAfter": int(row["total_credits_after"]),
                "error": row["error"],
                "receivedAt": int(row["received_at"]),
            }
        )

    return {
        "providers": sorted(PAYMENTS_ALLOWED_PROVIDERS),
        "summary": {
            "total": int(summary["total"] or 0),
            "applied": int(summary["applied_count"] or 0),
            "unapplied": int(summary["unapplied_count"] or 0),
            "withError": int(summary["error_count"] or 0),
        },
        "events": events,
    }


def retry_payment_callback(conn, provider, provider_event_id):
    normalized_provider = normalize_provider_name(provider)
    event_id = str(provider_event_id or "").strip()
    if normalized_provider == "" or event_id == "":
        raise ValueError("provider and providerEventId are required")

    row = conn.execute(
        """
        SELECT provider, provider_event_id, player_id, username, credits, currency, status, payload_json
        FROM payment_callbacks
        WHERE provider = ? AND provider_event_id = ?
        """,
        (normalized_provider, event_id),
    ).fetchone()
    if row is None:
        raise ValueError("Payment callback event not found")

    payload = {}
    raw_payload = row["payload_json"] or ""
    if raw_payload:
        try:
            parsed = json.loads(raw_payload)
            if isinstance(parsed, dict):
                payload = parsed
        except Exception:
            payload = {}

    callback = {
        "provider": row["provider"],
        "provider_event_id": row["provider_event_id"],
        "player_id": int(row["player_id"] or 0),
        "username": row["username"],
        "credits": int(row["credits"] or 0),
        "currency": row["currency"],
        "status": row["status"],
        "payload": payload,
        "payload_json": raw_payload,
        "received_at": now_ms(),
    }
    return process_payment_callback(conn, callback)


def load_companion_ownership(conn, player_id):
    result = {
        "playerId": max(0, int(player_id or 0)),
        "mounts": [],
        "pets": [],
        "source": "companion_ownership",
        "fetchedAt": now_ms(),
    }
    if result["playerId"] <= 0:
        return result
    try:
        rows = conn.execute(
            """
            SELECT companion_kind, companion_type
            FROM companion_ownership
            WHERE player_id = ?
            ORDER BY companion_kind ASC, companion_type ASC
            """,
            (result["playerId"],),
        ).fetchall()
    except sqlite3.Error:
        result["source"] = "companion_ownership_unavailable"
        return result

    mounts = []
    pets = []
    for row in rows:
        raw_kind = str(row["companion_kind"] or "").strip().lower()
        companion_type = parse_positive_int(row["companion_type"])
        if companion_type <= 0:
            continue
        if raw_kind in ("mount", "horse"):
            if companion_type not in mounts:
                mounts.append(companion_type)
        elif raw_kind == "pet":
            if companion_type not in pets:
                pets.append(companion_type)
    result["mounts"] = mounts
    result["pets"] = pets
    return result


def normalize_report_filter_text(value, max_length=120):
    text = str(value or "").strip()
    if text == "":
        return ""
    if len(text) > max_length:
        text = text[:max_length]
    return text


def report_payload_int(payload, *keys):
    if not isinstance(payload, dict):
        return 0
    for key in keys:
        if key not in payload:
            continue
        value = parse_positive_int(payload.get(key))
        if value > 0:
            return value
    return 0


def report_payload_text(payload, *keys, max_length=120):
    if not isinstance(payload, dict):
        return ""
    for key in keys:
        if key not in payload:
            continue
        value = str(payload.get(key) or "").strip()
        if value == "":
            continue
        if len(value) > max_length:
            value = value[:max_length]
        return value
    return ""


def decode_report_row(entry_row):
    raw_data = entry_row["data"]
    if isinstance(raw_data, memoryview):
        raw_data = raw_data.tobytes()
    if raw_data is None:
        raw_data = b""
    if isinstance(raw_data, str):
        raw_bytes = raw_data.encode("utf-8", errors="replace")
    elif isinstance(raw_data, bytes):
        raw_bytes = raw_data
    else:
        raw_bytes = bytes(raw_data)

    preview_text = raw_bytes.decode("utf-8", errors="replace").replace("\r", " ").replace("\n", " ").strip()
    if len(preview_text) > 220:
        preview_text = preview_text[:220].rstrip() + "..."

    payload = None
    if raw_bytes and len(raw_bytes) <= 262144:
        try:
            parsed = json.loads(raw_bytes.decode("utf-8", errors="replace"))
            if isinstance(parsed, dict):
                payload = parsed
        except Exception:
            payload = None

    payload_source = report_payload_text(payload, "source", "reportSource", "contextSource", max_length=80)
    payload_village_id = report_payload_int(payload, "villageId", "village_id")
    payload_village_name = report_payload_text(payload, "villageName", "village_name")
    payload_image_id = report_payload_int(payload, "imageId", "image_id")
    payload_event_type = report_payload_int(payload, "eventType", "event_type")
    payload_guestbook_post_id = report_payload_int(payload, "guestbookPostId", "guestbook_post_id")

    decoded_search_text = raw_bytes[:65536].decode("utf-8", errors="replace").lower()
    if payload_source:
        decoded_search_text = f"{decoded_search_text}\n{payload_source.lower()}"
    if payload_village_name:
        decoded_search_text = f"{decoded_search_text}\n{payload_village_name.lower()}"

    return {
        "reportId": int(entry_row["report_id"] or 0),
        "reporterPlayerId": int(entry_row["player_id"] or 0),
        "reportedPlayerId": int(entry_row["report_player_id"] or 0),
        "reportType": int(entry_row["report_type"] or 0),
        "dataB64": base64.b64encode(raw_bytes).decode("ascii"),
        "dataPreview": preview_text,
        "dataSize": len(raw_bytes),
        "createdAt": int(entry_row["created_at"] or 0),
        "payloadSource": payload_source,
        "payloadVillageId": payload_village_id,
        "payloadVillageName": payload_village_name,
        "payloadImageId": payload_image_id,
        "payloadEventType": payload_event_type,
        "payloadGuestbookPostId": payload_guestbook_post_id,
        "_searchText": decoded_search_text,
    }


def strip_internal_report_row_fields(entry):
    if not isinstance(entry, dict):
        return {}
    return {
        key: value
        for key, value in entry.items()
        if not str(key).startswith("_")
    }


def report_row_matches_filters(entry, query_text="", source_filter="", village_id=0, image_id=0):
    query = str(query_text or "").strip().lower()
    if query:
        haystack = str(entry.get("_searchText") or "").lower()
        if query not in haystack:
            return False

    source = str(source_filter or "").strip().lower()
    if source:
        payload_source = str(entry.get("payloadSource") or "").strip().lower()
        if payload_source != source:
            return False

    normalized_village_id = max(0, int(village_id or 0))
    if normalized_village_id > 0:
        if int(entry.get("payloadVillageId") or 0) != normalized_village_id:
            return False

    normalized_image_id = max(0, int(image_id or 0))
    if normalized_image_id > 0:
        if int(entry.get("payloadImageId") or 0) != normalized_image_id:
            return False

    return True


def load_report_history(
    conn,
    player_id,
    limit,
    offset=0,
    reporter_player_id=0,
    reported_player_id=0,
    report_type=0,
    query_text="",
    source_filter="",
    village_id=0,
    image_id=0,
    created_after=0,
    created_before=0,
    include_summary=False,
):
    normalized_player_id = max(0, int(player_id or 0))
    normalized_limit = max(1, min(200, int(limit or 20)))
    normalized_offset = max(0, int(offset or 0))
    normalized_reporter_player_id = max(0, int(reporter_player_id or 0))
    normalized_reported_player_id = max(0, int(reported_player_id or 0))
    normalized_report_type = max(0, int(report_type or 0))
    normalized_query_text = normalize_report_filter_text(query_text, 120)
    normalized_source_filter = normalize_report_filter_text(source_filter, 80).lower()
    normalized_village_id = max(0, int(village_id or 0))
    normalized_image_id = max(0, int(image_id or 0))
    normalized_created_after = max(0, int(created_after or 0))
    normalized_created_before = max(0, int(created_before or 0))
    normalized_include_summary = bool(include_summary)
    result = {
        "playerId": normalized_player_id,
        "limit": normalized_limit,
        "offset": normalized_offset,
        "reporterPlayerId": normalized_reporter_player_id,
        "reportedPlayerId": normalized_reported_player_id,
        "reportType": normalized_report_type,
        "queryText": normalized_query_text,
        "sourceFilter": normalized_source_filter,
        "villageIdFilter": normalized_village_id,
        "imageIdFilter": normalized_image_id,
        "createdAfter": normalized_created_after,
        "createdBefore": normalized_created_before,
        "reports": [],
        "retrievedCount": 0,
        "hasMore": False,
        "nextOffset": normalized_offset,
        "source": "report_history",
        "fetchedAt": now_ms(),
    }
    if normalized_player_id <= 0:
        return result
    try:
        where_parts = ["player_id = ?"]
        params = [
            normalized_reporter_player_id
            if normalized_reporter_player_id > 0
            else normalized_player_id
        ]
        if normalized_reported_player_id > 0:
            where_parts.append("report_player_id = ?")
            params.append(normalized_reported_player_id)
        if normalized_report_type > 0:
            where_parts.append("report_type = ?")
            params.append(normalized_report_type)
        if normalized_created_after > 0:
            where_parts.append("created_at >= ?")
            params.append(normalized_created_after)
        if normalized_created_before > 0:
            where_parts.append("created_at <= ?")
            params.append(normalized_created_before)
        base_query = f"""
            SELECT report_id, player_id, report_player_id, report_type, data, created_at
            FROM reports
            WHERE {" AND ".join(where_parts)}
            ORDER BY created_at DESC, report_id DESC
            LIMIT ?
            OFFSET ?
        """
        should_filter_by_payload = any(
            (
                normalized_query_text != "",
                normalized_source_filter != "",
                normalized_village_id > 0,
                normalized_image_id > 0,
            )
        )
        if not should_filter_by_payload:
            rows = conn.execute(
                base_query,
                tuple(params + [normalized_limit + 1, normalized_offset]),
            ).fetchall()
            has_more = len(rows) > normalized_limit
            visible_rows = rows[:normalized_limit]
            for row in visible_rows:
                result["reports"].append(strip_internal_report_row_fields(decode_report_row(row)))
            result["retrievedCount"] = len(result["reports"])
            result["hasMore"] = has_more
            result["nextOffset"] = normalized_offset + len(result["reports"])
            if normalized_include_summary:
                total_matches_row = conn.execute(
                    f"""
                    SELECT COUNT(1) AS total
                    FROM reports
                    WHERE {" AND ".join(where_parts)}
                    """,
                    tuple(params),
                ).fetchone()
                type_rows = conn.execute(
                    f"""
                    SELECT report_type, COUNT(1) AS count
                    FROM reports
                    WHERE {" AND ".join(where_parts)}
                    GROUP BY report_type
                    ORDER BY count DESC, report_type ASC
                    LIMIT 12
                    """,
                    tuple(params),
                ).fetchall()
                target_rows = conn.execute(
                    f"""
                    SELECT report_player_id, COUNT(1) AS count
                    FROM reports
                    WHERE {" AND ".join(where_parts)}
                    GROUP BY report_player_id
                    ORDER BY count DESC, report_player_id ASC
                    LIMIT 12
                    """,
                    tuple(params),
                ).fetchall()
                reporter_rows = conn.execute(
                    f"""
                    SELECT player_id, COUNT(1) AS count
                    FROM reports
                    WHERE {" AND ".join(where_parts)}
                    GROUP BY player_id
                    ORDER BY count DESC, player_id ASC
                    LIMIT 12
                    """,
                    tuple(params),
                ).fetchall()
                reporter_type_rows = conn.execute(
                    f"""
                    SELECT player_id, report_type, COUNT(1) AS count
                    FROM reports
                    WHERE {" AND ".join(where_parts)}
                    GROUP BY player_id, report_type
                    ORDER BY count DESC, player_id ASC, report_type ASC
                    LIMIT 12
                    """,
                    tuple(params),
                ).fetchall()
                type_target_rows = conn.execute(
                    f"""
                    SELECT report_type, report_player_id, COUNT(1) AS count
                    FROM reports
                    WHERE {" AND ".join(where_parts)}
                    GROUP BY report_type, report_player_id
                    ORDER BY count DESC, report_type ASC, report_player_id ASC
                    LIMIT 12
                    """,
                    tuple(params),
                ).fetchall()
                source_counts = {}
                type_source_counts = {}
                for entry in result["reports"]:
                    source = str(entry.get("payloadSource") or "").strip()
                    if source == "":
                        source = ""
                    report_type_key = int(entry.get("reportType") or 0)
                    if source:
                        source_counts[source] = source_counts.get(source, 0) + 1
                    if report_type_key > 0 and source:
                        type_source_key = (report_type_key, source)
                        type_source_counts[type_source_key] = type_source_counts.get(type_source_key, 0) + 1
                result["summary"] = {
                    "scope": "dataset",
                    "totalMatches": int(total_matches_row["total"] or 0),
                    "reportTypeCounts": [
                        {
                            "reportType": int(row["report_type"] or 0),
                            "count": int(row["count"] or 0),
                        }
                        for row in type_rows
                        if int(row["report_type"] or 0) > 0
                    ],
                    "reportedPlayerCounts": [
                        {
                            "reportedPlayerId": int(row["report_player_id"] or 0),
                            "count": int(row["count"] or 0),
                        }
                        for row in target_rows
                        if int(row["report_player_id"] or 0) > 0
                    ],
                    "reporterPlayerCounts": [
                        {
                            "reporterPlayerId": int(row["player_id"] or 0),
                            "count": int(row["count"] or 0),
                        }
                        for row in reporter_rows
                        if int(row["player_id"] or 0) > 0
                    ],
                    "reporterTypeCounts": [
                        {
                            "reporterPlayerId": int(row["player_id"] or 0),
                            "reportType": int(row["report_type"] or 0),
                            "count": int(row["count"] or 0),
                        }
                        for row in reporter_type_rows
                        if int(row["player_id"] or 0) > 0 and int(row["report_type"] or 0) > 0
                    ],
                    "payloadSourceCounts": [
                        {
                            "payloadSource": source,
                            "count": int(count),
                        }
                        for source, count in sorted(
                            source_counts.items(),
                            key=lambda item: (-item[1], item[0].lower()),
                        )[:12]
                    ],
                    "reportTypeTargetCounts": [
                        {
                            "reportType": int(row["report_type"] or 0),
                            "reportedPlayerId": int(row["report_player_id"] or 0),
                            "count": int(row["count"] or 0),
                        }
                        for row in type_target_rows
                        if int(row["report_type"] or 0) > 0 and int(row["report_player_id"] or 0) > 0
                    ],
                    "reportTypeSourceCounts": [
                        {
                            "reportType": int(report_type),
                            "payloadSource": source,
                            "count": int(count),
                        }
                        for (report_type, source), count in sorted(
                            type_source_counts.items(),
                            key=lambda item: (-item[1], item[0][0], item[0][1].lower()),
                        )[:12]
                    ],
                    "payloadSourceScope": "page",
                }
            return result

        filtered_rows = []
        scan_chunk_size = max(50, min(500, normalized_limit * 5))
        scan_offset = 0
        matched_before_offset = 0
        matched_total = 0
        has_more = False
        summary_type_counts = {}
        summary_target_counts = {}
        summary_reporter_counts = {}
        summary_source_counts = {}
        summary_type_target_counts = {}
        summary_type_source_counts = {}
        summary_reporter_type_counts = {}
        while True:
            batch = conn.execute(
                base_query,
                tuple(params + [scan_chunk_size, scan_offset]),
            ).fetchall()
            if not batch:
                break
            scan_offset += len(batch)
            for row in batch:
                decoded = decode_report_row(row)
                if not report_row_matches_filters(
                    decoded,
                    query_text=normalized_query_text,
                    source_filter=normalized_source_filter,
                    village_id=normalized_village_id,
                    image_id=normalized_image_id,
                ):
                    continue
                matched_total += 1
                if normalized_include_summary:
                    report_type_key = int(decoded.get("reportType") or 0)
                    if report_type_key > 0:
                        summary_type_counts[report_type_key] = summary_type_counts.get(report_type_key, 0) + 1
                    target_key = int(decoded.get("reportedPlayerId") or 0)
                    if target_key > 0:
                        summary_target_counts[target_key] = summary_target_counts.get(target_key, 0) + 1
                    reporter_key = int(decoded.get("reporterPlayerId") or 0)
                    if reporter_key > 0:
                        summary_reporter_counts[reporter_key] = summary_reporter_counts.get(reporter_key, 0) + 1
                    source_key = str(decoded.get("payloadSource") or "").strip()
                    if source_key:
                        summary_source_counts[source_key] = summary_source_counts.get(source_key, 0) + 1
                    if reporter_key > 0 and report_type_key > 0:
                        reporter_type_key = (reporter_key, report_type_key)
                        summary_reporter_type_counts[reporter_type_key] = summary_reporter_type_counts.get(reporter_type_key, 0) + 1
                    if report_type_key > 0 and target_key > 0:
                        type_target_key = (report_type_key, target_key)
                        summary_type_target_counts[type_target_key] = summary_type_target_counts.get(type_target_key, 0) + 1
                    if report_type_key > 0 and source_key:
                        type_source_key = (report_type_key, source_key)
                        summary_type_source_counts[type_source_key] = summary_type_source_counts.get(type_source_key, 0) + 1
                if matched_before_offset < normalized_offset:
                    matched_before_offset += 1
                    continue
                if len(filtered_rows) < normalized_limit:
                    filtered_rows.append(decoded)
                    continue
                has_more = True
                if not normalized_include_summary:
                    break
            if (has_more and not normalized_include_summary) or len(batch) < scan_chunk_size:
                break
    except sqlite3.Error:
        result["source"] = "report_history_unavailable"
        return result

    for row in filtered_rows:
        result["reports"].append(strip_internal_report_row_fields(row))
    result["retrievedCount"] = len(result["reports"])
    if normalized_include_summary:
        result["hasMore"] = matched_total > (normalized_offset + len(result["reports"]))
    else:
        result["hasMore"] = has_more
    result["nextOffset"] = normalized_offset + len(result["reports"])
    if normalized_include_summary:
        result["summary"] = {
            "scope": "filtered_match_set",
            "totalMatches": int(matched_total),
            "reportTypeCounts": [
                {
                    "reportType": int(report_type),
                    "count": int(count),
                }
                for report_type, count in sorted(
                    summary_type_counts.items(),
                    key=lambda item: (-item[1], item[0]),
                )[:12]
            ],
            "reportedPlayerCounts": [
                {
                    "reportedPlayerId": int(reported_player_id),
                    "count": int(count),
                }
                for reported_player_id, count in sorted(
                    summary_target_counts.items(),
                    key=lambda item: (-item[1], item[0]),
                )[:12]
            ],
            "reporterPlayerCounts": [
                {
                    "reporterPlayerId": int(reporter_player_id),
                    "count": int(count),
                }
                for reporter_player_id, count in sorted(
                    summary_reporter_counts.items(),
                    key=lambda item: (-item[1], item[0]),
                )[:12]
            ],
            "reporterTypeCounts": [
                {
                    "reporterPlayerId": int(reporter_player_id),
                    "reportType": int(report_type),
                    "count": int(count),
                }
                for (reporter_player_id, report_type), count in sorted(
                    summary_reporter_type_counts.items(),
                    key=lambda item: (-item[1], item[0][0], item[0][1]),
                )[:12]
            ],
            "payloadSourceCounts": [
                {
                    "payloadSource": source,
                    "count": int(count),
                }
                for source, count in sorted(
                    summary_source_counts.items(),
                    key=lambda item: (-item[1], item[0].lower()),
                )[:12]
            ],
            "reportTypeTargetCounts": [
                {
                    "reportType": int(report_type),
                    "reportedPlayerId": int(reported_player_id),
                    "count": int(count),
                }
                for (report_type, reported_player_id), count in sorted(
                    summary_type_target_counts.items(),
                    key=lambda item: (-item[1], item[0][0], item[0][1]),
                )[:12]
            ],
            "reportTypeSourceCounts": [
                {
                    "reportType": int(report_type),
                    "payloadSource": source,
                    "count": int(count),
                }
                for (report_type, source), count in sorted(
                    summary_type_source_counts.items(),
                    key=lambda item: (-item[1], item[0][0], item[0][1].lower()),
                )[:12]
            ],
            "payloadSourceScope": "filtered_match_set",
        }
    return result


class Handler(BaseHTTPRequestHandler):
    def _send_bytes(self, status_code, body, content_type):
        self.send_response(status_code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _send_text(self, status_code, text, content_type="text/plain; charset=utf-8"):
        self._send_bytes(status_code, text.encode("utf-8"), content_type)

    def _send_json(self, status_code, payload):
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self._send_bytes(status_code, body, "application/json; charset=utf-8")

    def _handle_share_page(self, share_id):
        with db_connect() as conn:
            row = conn.execute(
                "SELECT id, image_path, player_id, server_name, created_at FROM uploaded_pictures WHERE id = ?",
                (share_id,),
            ).fetchone()

        if row is None:
            self._send_text(404, "Share not found")
            return

        image_name = os.path.basename(row["image_path"])
        body = f"""<!doctype html>
<html>
  <head><meta charset=\"utf-8\"><title>Nord Screenshot #{int(row['id'])}</title></head>
  <body style=\"font-family:sans-serif;background:#111;color:#eee;margin:0;padding:16px;\">
    <h1 style=\"margin-top:0\">Nord Screenshot #{int(row['id'])}</h1>
    <p>Player {int(row['player_id'])} · {html.escape(row['server_name'])}</p>
    <img src=\"/uploads/{html.escape(image_name)}\" style=\"max-width:100%;border:1px solid #333\" alt=\"Nord screenshot\">
  </body>
</html>
"""
        self._send_text(200, body, "text/html; charset=utf-8")

    def _serve_uploaded_file(self, filename):
        path = self._resolve_uploaded_asset_path(filename)
        if not path:
            self.send_error(404, "file not found")
            return
        with open(path, "rb") as f:
            data = f.read()
        self._send_bytes(200, data, "image/jpeg")

    def _resolve_uploaded_asset_path(self, filename):
        safe_name = os.path.basename(str(filename or ""))
        if safe_name == "":
            return None

        image_id = 0
        prefer_thumbnail = False

        thumb_match = re.fullmatch(r"(\d+)thm\.jpg", safe_name)
        if thumb_match is not None:
            image_id = int(thumb_match.group(1))
            prefer_thumbnail = True
        else:
            full_match = re.fullmatch(r"(\d+)(?:\.jpg)?", safe_name)
            if full_match is not None:
                image_id = int(full_match.group(1))
            else:
                explicit_thumb_match = re.fullmatch(r"(\d+)_thumb\.jpg", safe_name)
                if explicit_thumb_match is not None:
                    image_id = int(explicit_thumb_match.group(1))
                    prefer_thumbnail = True

        if image_id > 0:
            try:
                with db_connect() as conn:
                    row = conn.execute(
                        "SELECT image_path, thumbnail_path FROM uploaded_pictures WHERE id = ?",
                        (image_id,),
                    ).fetchone()
            except sqlite3.Error:
                row = None

            candidates = []
            if row is not None:
                image_path = str(row["image_path"] or "").strip()
                thumbnail_path = str(row["thumbnail_path"] or "").strip()
                if prefer_thumbnail:
                    candidates.extend([thumbnail_path, image_path])
                else:
                    candidates.extend([image_path, thumbnail_path])

            if prefer_thumbnail:
                candidates.extend(
                    [
                        os.path.join(UPLOAD_DIR, f"{image_id}_thumb.jpg"),
                        os.path.join(UPLOAD_DIR, f"{image_id}.jpg"),
                    ]
                )
            else:
                candidates.extend(
                    [
                        os.path.join(UPLOAD_DIR, f"{image_id}.jpg"),
                        os.path.join(UPLOAD_DIR, f"{image_id}_thumb.jpg"),
                    ]
                )

            upload_root = os.path.abspath(UPLOAD_DIR)
            for raw_path in candidates:
                path = str(raw_path or "").strip()
                if path == "":
                    continue
                resolved = os.path.abspath(path)
                if not resolved.startswith(upload_root + os.sep) and resolved != upload_root:
                    continue
                if os.path.isfile(resolved):
                    return resolved
            return None

        direct_path = os.path.join(UPLOAD_DIR, safe_name)
        if os.path.isfile(direct_path):
            return direct_path
        if "." not in safe_name:
            jpeg_path = os.path.join(UPLOAD_DIR, safe_name + ".jpg")
            if os.path.isfile(jpeg_path):
                return jpeg_path
        return None

    def _serve_legacy_project_file(self, relative_name):
        safe_name = os.path.basename(relative_name)
        if safe_name == "" or safe_name != relative_name:
            self.send_error(404, "file not found")
            return
        path = os.path.join(PROJECT_ROOT, safe_name)
        if not os.path.isfile(path):
            self.send_error(404, "file not found")
            return
        with open(path, "rb") as f:
            data = f.read()
        self._send_bytes(200, data, guess_legacy_static_content_type(safe_name))

    def do_GET(self):
        parsed = urlsplit(self.path)
        path = parsed.path
        path_lower = path.lower()
        query = parse_qs(parsed.query, keep_blank_values=True)

        sys.stdout.write(f"GET {self.path}\n")
        sys.stdout.flush()

        if path == "/randomseed.version":
            seed = load_seed_bytes()
            if seed is None:
                self.send_error(404, "randomseed.dat not found")
                return
            version = hashlib.sha256(seed).hexdigest().encode("ascii")
            self._send_bytes(200, version, "text/plain; charset=utf-8")
            return

        if path == "/randomseed.dat":
            seed = load_seed_bytes()
            if seed is None:
                self.send_error(404, "randomseed.dat not found")
                return
            self._send_bytes(200, seed, "application/octet-stream")
            return

        if path.startswith("/ServerTime.jsp"):
            now = datetime.datetime.now(SERVER_TIME_TZ).strftime("%Y-%m-%d %H:%M:%S")
            self._send_text(200, now)
            return

        if path.startswith("/events/dynamic"):
            self._send_bytes(200, load_dynamic_events_xml(), "text/xml; charset=utf-8")
            return

        if path == "/clients/tracking.jsp":
            urls = build_tracking_urls(self)
            body = "\n".join(urls)
            if body and not body.endswith("\n"):
                body += "\n"
            self._send_text(200, body)
            return

        if path.startswith("/track/"):
            query_data = query.get("data", [""])[0]
            with db_connect() as conn:
                conn.execute(
                    "INSERT INTO track_requests(query_data, remote_addr, user_agent, created_at) VALUES (?, ?, ?, ?)",
                    (query_data, self.client_address[0], self.headers.get("User-Agent", ""), now_ms()),
                )
            self._send_text(200, "1\n")
            return

        if path == "":
            path = "/"
            path_lower = "/"

        if path == "/":
            referral_id = first_query_value(query, "referralid", "referralId", "id", "playerid", "playerId")
            log_legacy_web_request(path, parsed.query, self.client_address[0], self.headers.get("User-Agent", ""))
            self._send_text(
                200,
                legacy_page_html(
                    "Nord",
                    "Nord",
                    "Legacy root page placeholder for revived-server local environments.",
                    [
                        ("path", path),
                        ("referralId", referral_id or "missing"),
                    ],
                ),
                "text/html; charset=utf-8",
            )
            return

        if path_lower in ("/jnlp", "/jnlp/"):
            log_legacy_web_request(path, parsed.query, self.client_address[0], self.headers.get("User-Agent", ""))
            self._send_text(
                200,
                legacy_page_html(
                    "Nord JNLP",
                    "JNLP",
                    "Legacy WebStart endpoint placeholder.",
                    [
                        ("path", path),
                        ("hint", "Use /jnlp/<filename> to fetch local project files by basename"),
                    ],
                ),
                "text/html; charset=utf-8",
            )
            return

        if path_lower.startswith("/jnlp/"):
            resource_name = path.split("/", 2)[2] if path.count("/") >= 2 else ""
            log_legacy_web_request(path, parsed.query, self.client_address[0], self.headers.get("User-Agent", ""))
            self._serve_legacy_project_file(os.path.basename(resource_name))
            return

        if path_lower == "/tempimages/face.jpg":
            log_legacy_web_request(path, parsed.query, self.client_address[0], self.headers.get("User-Agent", ""))
            self._send_bytes(200, DEFAULT_FACE_IMAGE_GIF, "image/gif")
            return

        if path_lower == "/support.jsp":
            log_legacy_web_request(path, parsed.query, self.client_address[0], self.headers.get("User-Agent", ""))
            self._send_text(
                200,
                legacy_page_html(
                    "Nord Support",
                    "Support",
                    "Local support landing page for revived server environments.",
                    [
                        ("status", "online"),
                        ("path", path),
                    ],
                ),
                "text/html; charset=utf-8",
            )
            return

        if path_lower == "/tutorial.jsp":
            log_legacy_web_request(path, parsed.query, self.client_address[0], self.headers.get("User-Agent", ""))
            self._send_text(
                200,
                legacy_page_html(
                    "Nord Tutorial",
                    "Tutorial",
                    "Legacy tutorial entrypoint placeholder for local parity.",
                    [
                        ("status", "available"),
                        ("path", path),
                    ],
                ),
                "text/html; charset=utf-8",
            )
            return

        if path_lower == "/referral.jsp":
            referral_player_id = first_query_value(query, "id", "playerid", "playerId")
            referral_server = first_query_value(query, "server")
            referral_checksum = first_query_value(query, "checksum").lower()
            expected_checksum = ""
            checksum_valid = False
            if referral_player_id != "":
                expected_checksum = hashlib.md5((referral_player_id + "sudhoxre").encode("utf-8")).hexdigest()
                checksum_valid = referral_checksum == expected_checksum
            log_legacy_web_request(path, parsed.query, self.client_address[0], self.headers.get("User-Agent", ""))
            self._send_text(
                200,
                legacy_page_html(
                    "Nord Referral",
                    "Referral",
                    "Legacy referral landing endpoint.",
                    [
                        ("playerId", referral_player_id or "missing"),
                        ("server", referral_server or "missing"),
                        ("checksumValid", str(checksum_valid).lower()),
                    ],
                ),
                "text/html; charset=utf-8",
            )
            return

        if path_lower == "/hamsterredirect.jsp":
            log_legacy_web_request(path, parsed.query, self.client_address[0], self.headers.get("User-Agent", ""))
            location = "/coins/"
            if parsed.query:
                location += "?" + parsed.query
            self.send_response(302)
            self.send_header("Location", location)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return

        if path_lower in ("/sponsorpay/launch.jsp", "/superrewards/launch.jsp", "/mopay", "/mopay/") or path_lower == "/coins" or path_lower.startswith("/coins/"):
            provider_name = "coins"
            if path_lower.startswith("/sponsorpay/"):
                provider_name = "sponsorpay"
            elif path_lower.startswith("/superrewards/"):
                provider_name = "superrewards"
            elif path_lower.startswith("/mopay"):
                provider_name = "mopay"
            player_id = first_query_value(query, "playerid", "playerId", "userid", "userId")
            from_player_id = first_query_value(query, "fromuserid", "fromUserId")
            server_name = first_query_value(query, "server")
            lang = first_query_value(query, "lang")
            log_legacy_web_request(path, parsed.query, self.client_address[0], self.headers.get("User-Agent", ""))
            self._send_text(
                200,
                legacy_page_html(
                    "Nord Coins",
                    "Coins / Offers",
                    "Payment features are currently disabled on this revived server.",
                    [
                        ("provider", provider_name),
                        ("playerId", player_id or "missing"),
                        ("fromUserId", from_player_id or "missing"),
                        ("server", server_name or "missing"),
                        ("lang", lang or "missing"),
                    ],
                ),
                "text/html; charset=utf-8",
            )
            return

        if path.startswith("/share/"):
            share_id = path.split("/", 2)[2]
            if not share_id.isdigit():
                self._send_text(404, "Invalid share id")
                return
            self._handle_share_page(int(share_id))
            return

        if path.startswith("/uploads/"):
            filename = path.split("/", 2)[2]
            self._serve_uploaded_file(filename)
            return

        if path.startswith("/dl/"):
            filename = path.split("/", 2)[2]
            self._serve_uploaded_file(filename)
            return

        if path == "/api/companion-ownership":
            player_id = parse_positive_int(query.get("playerId", ["0"])[0])
            try:
                with db_connect() as conn:
                    result = load_companion_ownership(conn, player_id)
                self._send_json(200, result)
            except Exception as exc:
                self._send_json(500, {"error": str(exc)})
            return

        if path == "/api/report-history":
            player_id = parse_positive_int(query.get("playerId", ["0"])[0])
            limit = parse_positive_int(query.get("limit", ["20"])[0])
            offset = parse_positive_int(query.get("offset", ["0"])[0])
            reporter_player_id = parse_positive_int(first_query_value(query, "reporterPlayerId", "reporterId"))
            reported_player_id = parse_positive_int(query.get("reportedPlayerId", ["0"])[0])
            report_type = parse_positive_int(query.get("reportType", ["0"])[0])
            query_text = first_query_value(query, "query", "search", "text")
            source_filter = first_query_value(query, "source", "payloadSource")
            village_id_filter = parse_positive_int(first_query_value(query, "villageId", "payloadVillageId"))
            image_id_filter = parse_positive_int(first_query_value(query, "imageId", "payloadImageId"))
            created_after = parse_positive_int(first_query_value(query, "createdAfter", "after"))
            created_before = parse_positive_int(first_query_value(query, "createdBefore", "before"))
            include_summary_raw = first_query_value(query, "includeSummary", "summary")
            include_summary = parse_optional_bool(include_summary_raw) is True
            if limit <= 0:
                limit = 20
            try:
                with db_connect() as conn:
                    result = load_report_history(
                        conn,
                        player_id,
                        limit,
                        offset=offset,
                        reporter_player_id=reporter_player_id,
                        reported_player_id=reported_player_id,
                        report_type=report_type,
                        query_text=query_text,
                        source_filter=source_filter,
                        village_id=village_id_filter,
                        image_id=image_id_filter,
                        created_after=created_after,
                        created_before=created_before,
                        include_summary=include_summary,
                    )
                self._send_json(200, result)
            except Exception as exc:
                self._send_json(500, {"error": str(exc)})
            return

        if path == "/payments/reconciliation":
            if not PAYMENTS_CALLBACK_ENABLED:
                self.send_error(404, "Not found")
                return
            if not require_payments_auth(self, PAYMENTS_RECONCILE_TOKEN, query, {}):
                self._send_json(403, {"error": "Unauthorized"})
                return
            provider = query.get("provider", [""])[0]
            applied_filter = parse_optional_bool(query.get("applied", [None])[0])
            limit = parse_positive_int(query.get("limit", [PAYMENT_LIST_LIMIT_DEFAULT])[0]) or PAYMENT_LIST_LIMIT_DEFAULT
            try:
                with db_connect() as conn:
                    result = list_payment_callbacks(conn, provider=provider, applied=applied_filter, limit=limit)
                self._send_json(200, result)
            except Exception as exc:
                self._send_json(500, {"error": str(exc)})
            return

        # Default: return well-formed XML so XML parsers don't choke.
        trace_unknown_http("GET", path, query, note="fallback-xml")
        self._send_bytes(200, XML_BODY, "text/xml; charset=utf-8")

    def do_POST(self):
        parsed = urlsplit(self.path)
        path = parsed.path
        query = parse_qs(parsed.query, keep_blank_values=True)

        sys.stdout.write(f"POST {self.path}\n")
        sys.stdout.flush()

        callback_match = re.fullmatch(r"/payments/callback/([^/]+)", path)
        if callback_match is not None:
            if not PAYMENTS_CALLBACK_ENABLED:
                self.send_error(404, "Not found")
                return
            if PAYMENTS_CALLBACK_TOKEN == "":
                self._send_json(503, {"error": "Payment callbacks are enabled but NORD_PAYMENTS_CALLBACK_TOKEN is not configured"})
                return
            provider = normalize_provider_name(callback_match.group(1))
            length = int(self.headers.get("Content-Length", "0") or "0")
            raw = self.rfile.read(length)
            try:
                payload = parse_payment_payload(self.headers.get("Content-Type", ""), raw)
                if not require_payments_auth(self, PAYMENTS_CALLBACK_TOKEN, query, payload):
                    self._send_json(403, {"error": "Unauthorized"})
                    return
                canonical_payload_json = json.dumps(
                    payload_without_signature_fields(payload, provider),
                    sort_keys=True,
                    separators=(",", ":"),
                )
                signature_ok, signature_error = verify_payment_signature(
                    self,
                    query,
                    payload,
                    provider,
                    raw,
                    canonical_payload_json,
                )
                if not signature_ok:
                    self._send_json(403, {"error": signature_error})
                    return
                with db_connect() as conn:
                    result = handle_payment_callback(conn, provider, payload)
                self._send_json(200, result)
            except Exception as exc:
                self._send_json(400, {"error": str(exc)})
            return

        if path == "/payments/reconciliation/retry":
            if not PAYMENTS_CALLBACK_ENABLED:
                self.send_error(404, "Not found")
                return
            if PAYMENTS_RECONCILE_TOKEN == "":
                self._send_json(503, {"error": "Payment reconciliation is enabled but NORD_PAYMENTS_RECONCILE_TOKEN is not configured"})
                return
            length = int(self.headers.get("Content-Length", "0") or "0")
            raw = self.rfile.read(length)
            try:
                payload = parse_payment_payload(self.headers.get("Content-Type", ""), raw)
                if not require_payments_auth(self, PAYMENTS_RECONCILE_TOKEN, query, payload):
                    self._send_json(403, {"error": "Unauthorized"})
                    return
                provider = first_payload_value(payload, "provider")
                provider_event_id = first_payload_value(payload, "providerEventId", "provider_event_id", "eventId", "event_id")
                with db_connect() as conn:
                    result = retry_payment_callback(conn, provider, provider_event_id)
                self._send_json(200, result)
            except Exception as exc:
                self._send_json(400, {"error": str(exc)})
            return

        if path == "/forum/":
            length = int(self.headers.get("Content-Length", "0") or "0")
            raw = self.rfile.read(length)
            try:
                payload = json.loads(raw.decode("utf-8"))
                if not isinstance(payload, dict):
                    raise ValueError("Invalid request body")
                result = handle_forum_request(payload)
                self._send_json(200, result)
            except Exception as exc:
                if "Unknown forum method" in str(exc):
                    trace_unknown_http(
                        "POST",
                        path,
                        query,
                        content_type=self.headers.get("Content-Type", ""),
                        body=raw,
                        note="unknown-forum-method",
                    )
                self._send_json(200, {"error": str(exc)})
            return

        if path in ("/ul", "/ul/"):
            try:
                content_type = self.headers.get("Content-Type", "")
                if not content_type.startswith("multipart/form-data"):
                    self._send_text(200, "ERR:Invalid content type")
                    return

                length = int(self.headers.get("Content-Length", "0") or "0")
                body = self.rfile.read(length)
                fields, files = parse_multipart_form(content_type, body)

                image_bytes = files.get("image")
                thumb_bytes = files.get("thumb")
                session_text = str(fields.get("session", "")).strip()

                if image_bytes is None:
                    self._send_text(200, "ERR:Missing image")
                    return
                if not image_bytes:
                    self._send_text(200, "ERR:Empty image")
                    return
                if not thumb_bytes:
                    thumb_bytes = image_bytes

                image_error = validate_uploaded_image_bytes(image_bytes, "image")
                if image_error:
                    self._send_text(200, f"ERR:{image_error}")
                    return
                thumb_error = validate_uploaded_image_bytes(thumb_bytes, "thumb")
                if thumb_error:
                    self._send_text(200, f"ERR:{thumb_error}")
                    return

                created_at = now_ms()
                with db_connect() as conn:
                    player_id = resolve_player_id_from_session(conn, session_text)
                    if player_id <= 0:
                        self._send_text(200, "ERR:Invalid session")
                        return

                    server_name = (self.headers.get("Host", "") or "").strip()
                    cur = conn.execute(
                        """
                        INSERT INTO uploaded_pictures(player_id, server_name, image_path, thumbnail_path, created_at, checksum_valid)
                        VALUES (?, ?, '', '', ?, 1)
                        """,
                        (player_id, server_name, created_at),
                    )
                    image_id = int(cur.lastrowid)
                    image_path = os.path.join(UPLOAD_DIR, f"{image_id}.jpg")
                    thumbnail_path = os.path.join(UPLOAD_DIR, f"{image_id}_thumb.jpg")

                    with open(image_path, "wb") as out:
                        out.write(image_bytes)
                    with open(thumbnail_path, "wb") as out:
                        out.write(thumb_bytes)

                    conn.execute(
                        "UPDATE uploaded_pictures SET image_path = ?, thumbnail_path = ? WHERE id = ?",
                        (image_path, thumbnail_path, image_id),
                    )

                self._send_text(200, f"OK:{image_id}")
                return
            except Exception as exc:
                self._send_text(200, f"ERR:{exc}")
                return

        if path in ("/delete", "/delete/"):
            try:
                content_type = self.headers.get("Content-Type", "")
                if not content_type.startswith("multipart/form-data"):
                    self._send_text(200, "ERR:Invalid content type")
                    return

                length = int(self.headers.get("Content-Length", "0") or "0")
                body = self.rfile.read(length)
                fields, _files = parse_multipart_form(content_type, body)

                session_text = str(fields.get("session", "")).strip()
                image_id = parse_positive_int_text(fields.get("imageID", ""))
                if image_id <= 0:
                    self._send_text(200, "ERR:Invalid imageID")
                    return

                with db_connect() as conn:
                    player_id = resolve_player_id_from_session(conn, session_text)
                    if player_id <= 0:
                        self._send_text(200, "ERR:Invalid session")
                        return

                    row = conn.execute(
                        "SELECT player_id, image_path, thumbnail_path FROM uploaded_pictures WHERE id = ?",
                        (image_id,),
                    ).fetchone()
                    if row is None:
                        self._send_text(200, "ERR:Image not found")
                        return
                    if int(row["player_id"] or 0) != player_id:
                        self._send_text(200, "ERR:Not allowed")
                        return

                    conn.execute("DELETE FROM uploaded_pictures WHERE id = ?", (image_id,))
                    profile_row = conn.execute(
                        "SELECT presentation_picture_url FROM account_profiles WHERE player_id = ?",
                        (player_id,),
                    ).fetchone()
                    if profile_row and presentation_picture_refs_image(
                        profile_row["presentation_picture_url"],
                        image_id,
                    ):
                        conn.execute(
                            "UPDATE account_profiles SET presentation_picture_url = '' WHERE player_id = ?",
                            (player_id,),
                        )

                    upload_root = os.path.abspath(UPLOAD_DIR)
                    for raw_path in (row["image_path"], row["thumbnail_path"]):
                        path = str(raw_path or "").strip()
                        if path == "":
                            continue
                        resolved = os.path.abspath(path)
                        if not resolved.startswith(upload_root + os.sep) and resolved != upload_root:
                            continue
                        try:
                            if os.path.isfile(resolved):
                                os.remove(resolved)
                        except OSError:
                            pass

                self._send_text(200, f"OK:{image_id}")
                return
            except Exception as exc:
                self._send_text(200, f"ERR:{exc}")
                return

        if path == "/uploadPicture":
            try:
                content_type = self.headers.get("Content-Type", "")
                if not content_type.startswith("multipart/form-data"):
                    self._send_text(200, ":ERROR:Invalid content type")
                    return

                length = int(self.headers.get("Content-Length", "0") or "0")
                body = self.rfile.read(length)
                fields, files = parse_multipart_form(content_type, body)

                image_bytes = files.get("image")
                player_id_text = str(fields.get("playerid", "")).strip()
                server_name = str(fields.get("server", "")).strip()
                checksum = str(fields.get("checksum", "")).strip().lower()

                if image_bytes is None:
                    self._send_text(200, ":ERROR:Missing image")
                    return
                if not player_id_text.isdigit():
                    self._send_text(200, ":ERROR:Invalid playerid")
                    return
                if server_name == "":
                    self._send_text(200, ":ERROR:Missing server")
                    return

                expected = expected_upload_checksum(player_id_text)
                if checksum != expected:
                    self._send_text(200, ":ERROR:Invalid checksum")
                    return

                if not image_bytes:
                    self._send_text(200, ":ERROR:Empty image")
                    return

                image_error = validate_uploaded_image_bytes(image_bytes, "image")
                if image_error:
                    self._send_text(200, f":ERROR:{image_error}")
                    return

                created_at = now_ms()
                with db_connect() as conn:
                    cur = conn.execute(
                        """
                        INSERT INTO uploaded_pictures(player_id, server_name, image_path, created_at, checksum_valid)
                        VALUES (?, ?, '', ?, 1)
                        """,
                        (int(player_id_text), server_name, created_at),
                    )
                    share_id = int(cur.lastrowid)
                    filename = f"{share_id}.jpg"
                    file_path = os.path.join(UPLOAD_DIR, filename)
                    with open(file_path, "wb") as out:
                        out.write(image_bytes)
                    conn.execute(
                        "UPDATE uploaded_pictures SET image_path = ? WHERE id = ?",
                        (file_path, share_id),
                    )

                self._send_text(200, str(share_id))
                return
            except Exception as exc:
                self._send_text(200, f":ERROR:{exc}")
                return

        length = int(self.headers.get("Content-Length", "0") or "0")
        raw = self.rfile.read(length) if length > 0 else b""
        trace_unknown_http(
            "POST",
            path,
            query,
            content_type=self.headers.get("Content-Type", ""),
            body=raw,
            note="404-not-found",
        )
        self.send_error(404, "Not found")

    def log_message(self, format, *args):
        return


if __name__ == "__main__":
    init_schema()
    bind_host = os.environ.get("NORD_HTTP_BIND", "0.0.0.0")
    bind_port = int(os.environ.get("NORD_HTTP_PORT", "8080"))
    image_port_raw = (
        os.environ.get("NORD_IMAGE_PORT", "").strip()
        or os.environ.get("NORD_SERVER_IMAGE_PORT", "").strip()
        or "31876"
    )
    try:
        image_port = int(image_port_raw)
    except Exception:
        image_port = 31876

    server = HTTPServer((bind_host, bind_port), Handler)
    print(f"Nord HTTP sidecar listening on {bind_host}:{bind_port}")

    if image_port > 0 and image_port != bind_port:
        try:
            image_server = HTTPServer((bind_host, image_port), Handler)

            def serve_image_port():
                print(f"Nord image compatibility sidecar listening on {bind_host}:{image_port}")
                image_server.serve_forever()

            threading.Thread(target=serve_image_port, name="nord-image-http", daemon=True).start()
        except OSError as exc:
            print(
                f"Nord image compatibility sidecar failed to bind {bind_host}:{image_port}: {exc}. "
                "Continuing with primary HTTP sidecar only."
            )

    server.serve_forever()
