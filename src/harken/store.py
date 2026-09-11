"""SQLite persistence. One file, no server, your data stays on your box.

The store is deliberately tiny: upsert mentions (de-duplicated by content id),
query them back with filters, and compute the aggregates the dashboard needs.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from harken.auth import validate_role, validate_username
from harken.models import Mention, Sentiment

_CREATE_MENTIONS = """
CREATE TABLE mentions (
    id            TEXT NOT NULL,
    source        TEXT NOT NULL,
    query         TEXT NOT NULL,
    author        TEXT,
    title         TEXT,
    text          TEXT,
    url           TEXT,
    created_at    TEXT NOT NULL,
    score         INTEGER,
    sentiment     TEXT,
    sentiment_score REAL,
    theme         TEXT,
    fetched_at    TEXT NOT NULL,
    published_at  TEXT,
    publication_provenance TEXT NOT NULL DEFAULT 'legacy_unknown',
    source_updated_at TEXT,
    body_expired INTEGER NOT NULL DEFAULT 0,
    source_id TEXT NOT NULL DEFAULT '',
    source_item_id TEXT,
    author_id TEXT,
    indexed_at TEXT,
    PRIMARY KEY (id, query)
);
"""

_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_query ON mentions(query);
CREATE INDEX IF NOT EXISTS idx_created ON mentions(created_at);
CREATE INDEX IF NOT EXISTS idx_fetched_page ON mentions(fetched_at, id, query);
"""

_ALERT_SCHEMA = """
CREATE TABLE IF NOT EXISTS alert_outbox (
    query         TEXT NOT NULL,
    mention_id    TEXT NOT NULL,
    target_key    TEXT NOT NULL,
    enqueued_at   TEXT NOT NULL,
    delivered_at TEXT,
    attempts      INTEGER NOT NULL DEFAULT 0,
    last_error    TEXT,
    PRIMARY KEY (query, mention_id, target_key)
);
CREATE INDEX IF NOT EXISTS idx_alert_pending
    ON alert_outbox(target_key, query, delivered_at, enqueued_at);
"""

_TRACKING_SCHEMA = """
CREATE TABLE IF NOT EXISTS tracked_queries (
    query           TEXT PRIMARY KEY,
    sources         TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    last_scanned_at TEXT
);
CREATE TABLE IF NOT EXISTS source_scan_state (
    query              TEXT NOT NULL,
    source             TEXT NOT NULL,
    newest_at          TEXT,
    oldest_at          TEXT,
    incremental_cursor TEXT,
    incremental_since  TEXT,
    backfill_cursor    TEXT,
    backfill_complete  INTEGER NOT NULL DEFAULT 0,
    last_success_at    TEXT,
    last_error         TEXT,
    PRIMARY KEY (query, source)
);
CREATE INDEX IF NOT EXISTS idx_scan_state_query ON source_scan_state(query);
"""

_THRESHOLD_ALERT_SCHEMA = """
CREATE TABLE IF NOT EXISTS threshold_alerts (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    query        TEXT NOT NULL,
    event_type   TEXT NOT NULL,
    target_key   TEXT NOT NULL,
    text         TEXT NOT NULL,
    payload      TEXT NOT NULL,
    triggered_at TEXT NOT NULL,
    delivered_at TEXT,
    cleared_at   TEXT,
    attempts     INTEGER NOT NULL DEFAULT 0,
    last_error   TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_threshold_alert_active
    ON threshold_alerts(query, event_type, target_key)
    WHERE cleared_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_threshold_alert_pending
    ON threshold_alerts(target_key, delivered_at, cleared_at, triggered_at);
"""

_SOURCE_METRICS_SCHEMA = """
CREATE TABLE IF NOT EXISTS source_metrics (
    source                 TEXT PRIMARY KEY,
    scans_total            INTEGER NOT NULL DEFAULT 0,
    errors_total           INTEGER NOT NULL DEFAULT 0,
    fetched_total          INTEGER NOT NULL DEFAULT 0,
    pages_total            INTEGER NOT NULL DEFAULT 0,
    retries_total          INTEGER NOT NULL DEFAULT 0,
    duration_seconds_total REAL NOT NULL DEFAULT 0,
    last_duration_seconds  REAL NOT NULL DEFAULT 0,
    last_fetched           INTEGER NOT NULL DEFAULT 0,
    last_pages             INTEGER NOT NULL DEFAULT 0,
    last_retries           INTEGER NOT NULL DEFAULT 0,
    last_success           INTEGER NOT NULL DEFAULT 0,
    last_scan_at           TEXT NOT NULL,
    last_success_at        TEXT,
    last_error_at          TEXT,
    last_error             TEXT
);
"""

DEFAULT_PROJECT_ID = 1

_PROJECT_SCHEMA = """
CREATE TABLE IF NOT EXISTS projects (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    name       TEXT NOT NULL COLLATE NOCASE UNIQUE,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
INSERT OR IGNORE INTO projects (id, name, created_at, updated_at)
VALUES (1, 'Default', strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
        strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));
CREATE TABLE IF NOT EXISTS project_queries (
    project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    query      TEXT NOT NULL REFERENCES tracked_queries(query) ON DELETE CASCADE,
    added_at   TEXT NOT NULL,
    PRIMARY KEY (project_id, query)
);
CREATE INDEX IF NOT EXISTS idx_project_queries_query ON project_queries(query);
"""

_AUTH_SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    username      TEXT NOT NULL COLLATE NOCASE UNIQUE,
    password_hash TEXT NOT NULL,
    role          TEXT NOT NULL CHECK(role IN ('viewer', 'operator', 'admin')),
    active        INTEGER NOT NULL DEFAULT 1,
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL,
    last_login_at TEXT
);
CREATE TABLE IF NOT EXISTS auth_sessions (
    token_hash TEXT PRIMARY KEY,
    user_id    INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_auth_sessions_expiry ON auth_sessions(expires_at);
CREATE INDEX IF NOT EXISTS idx_auth_sessions_user ON auth_sessions(user_id);
"""

# Bumped when the on-disk schema or a one-time reconciliation step changes.
# Stored in `PRAGMA user_version` so _ensure_schema() can skip the expensive
# whole-table reconciliation on every connection once a DB is up to date.
_SCHEMA_VERSION = 4

_OBSERVATION_SCHEMA = """
CREATE TABLE IF NOT EXISTS observation_meta (
    key TEXT PRIMARY KEY, value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS observation_heads (
    id TEXT PRIMARY KEY, revision TEXT NOT NULL, reference TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS observation_changes (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    changed_at TEXT NOT NULL, reference TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS collection_attempts (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    finished_at TEXT NOT NULL, record TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_mentions_native ON mentions(source_id,source_item_id);
CREATE INDEX IF NOT EXISTS idx_mentions_identity_url ON mentions(source_id,url);
CREATE INDEX IF NOT EXISTS idx_observation_change_id
    ON observation_changes(json_extract(reference,'$.id'));
CREATE INDEX IF NOT EXISTS idx_attempt_scope
    ON collection_attempts(json_extract(record,'$.attempt_scope_digest'),seq);
"""


class CursorError(ValueError):
    """A continuation cannot be safely resumed against this store and scope."""

    def __init__(self, code: str, detail: str):
        self.code = code
        super().__init__(detail)


class Store:
    def __init__(self, path: str | Path = "harken.db"):
        raw_path = str(path)
        self.path = raw_path if raw_path == ":memory:" else str(Path(raw_path).expanduser())
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.path, timeout=30.0)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA busy_timeout = 30000")
        self._conn.execute("PRAGMA foreign_keys = ON")
        if self.path != ":memory:":
            self._conn.execute("PRAGMA journal_mode = WAL")
        self._ensure_schema()
        self._ensure_observations()
        self._conn.commit()

    def _ensure_schema(self) -> None:
        with closing(self._conn.cursor()) as cur:
            cur.execute("PRAGMA user_version")
            if cur.fetchone()[0] >= _SCHEMA_VERSION:
                # Already reconciled; skip the whole-table GROUP BY scan that
                # would otherwise run on every connection (once per web request).
                return
            cur.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'projects'")
            had_project_schema = cur.fetchone() is not None
            cur.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'mentions'")
            if cur.fetchone() is None:
                cur.executescript(
                    _CREATE_MENTIONS
                    + _INDEXES
                    + _ALERT_SCHEMA
                    + _TRACKING_SCHEMA
                    + _THRESHOLD_ALERT_SCHEMA
                    + _SOURCE_METRICS_SCHEMA
                    + _PROJECT_SCHEMA
                    + _AUTH_SCHEMA
                )
                cur.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")
                return

            cur.execute("PRAGMA table_info(mentions)")
            primary_key = [
                row["name"]
                for row in sorted(cur.fetchall(), key=lambda row: row["pk"])
                if row["pk"]
            ]
            if primary_key == ["id"]:
                # v0.1 stored one query directly on a globally unique mention,
                # so the same post could not belong to two tracked keywords.
                cur.executescript(
                    "BEGIN IMMEDIATE;\n"
                    "ALTER TABLE mentions RENAME TO mentions_v1;\n"
                    "DROP INDEX IF EXISTS idx_query;\n"
                    "DROP INDEX IF EXISTS idx_created;\n"
                    + _CREATE_MENTIONS
                    + "INSERT INTO mentions (id,source,query,author,title,text,url,created_at,"
                    "score,sentiment,sentiment_score,theme,fetched_at) "
                    "SELECT id,source,query,author,title,text,url,created_at,"
                    "score,sentiment,sentiment_score,theme,fetched_at FROM mentions_v1;\n"
                    "DROP TABLE mentions_v1;\n"
                    "COMMIT;\n"
                )
            elif primary_key != ["id", "query"]:
                raise RuntimeError(f"Unsupported Harken database schema: primary key {primary_key}")
            columns = {row["name"] for row in cur.execute("PRAGMA table_info(mentions)")}
            for name, declaration in (
                ("published_at", "TEXT"),
                ("publication_provenance", "TEXT NOT NULL DEFAULT 'legacy_unknown'"),
                ("source_updated_at", "TEXT"),
                ("body_expired", "INTEGER NOT NULL DEFAULT 0"),
                ("source_id", "TEXT NOT NULL DEFAULT ''"),
                ("source_item_id", "TEXT"),
                ("author_id", "TEXT"),
                ("indexed_at", "TEXT"),
            ):
                if name not in columns:
                    cur.execute(f"ALTER TABLE mentions ADD COLUMN {name} {declaration}")
            # Historical RSS created_at may be fetched-now or updated time.
            # Never promote that ambiguous value into publication evidence.
            cur.execute(
                "UPDATE mentions SET published_at=NULL, "
                "publication_provenance='legacy_unknown' "
                "WHERE publication_provenance IN ('legacy_unknown','source_created_at')"
            )
            cur.execute("UPDATE mentions SET source_id=source WHERE source_id=''")
            cur.executescript(
                _INDEXES
                + _ALERT_SCHEMA
                + _TRACKING_SCHEMA
                + _THRESHOLD_ALERT_SCHEMA
                + _SOURCE_METRICS_SCHEMA
                + _PROJECT_SCHEMA
                + _AUTH_SCHEMA
            )
            cur.execute(
                """
                INSERT OR IGNORE INTO tracked_queries
                    (query, sources, created_at, updated_at, last_scanned_at)
                SELECT query, '[]', MIN(fetched_at), MAX(fetched_at), MAX(fetched_at)
                FROM mentions GROUP BY query
                """
            )
            if not had_project_schema:
                cur.execute(
                    """
                    INSERT OR IGNORE INTO project_queries (project_id, query, added_at)
                    SELECT ?, query, COALESCE(created_at, updated_at)
                    FROM tracked_queries
                    """,
                    (DEFAULT_PROJECT_ID,),
                )
            cur.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")

    def _ensure_observations(self) -> None:
        self._conn.executescript(_OBSERVATION_SCHEMA)
        with self._conn:
            self._conn.execute("BEGIN IMMEDIATE")
            exists = self._conn.execute(
                "SELECT value FROM observation_meta WHERE key='epoch'"
            ).fetchone()
            identity_ready = self._conn.execute(
                "SELECT value FROM observation_meta WHERE key='native_identity_version'"
            ).fetchone()
            if exists and identity_ready:
                return
            if not exists:
                self._conn.executemany(
                    "INSERT INTO observation_meta(key,value) VALUES (?,?)",
                    [("epoch", secrets.token_hex(16)), ("cursor_key", secrets.token_hex(32)),
                     ("changes_floor", "0"), ("coverage_floor", "0")],
                )
            now = datetime.now(timezone.utc).isoformat()
            with closing(self._conn.cursor()) as cur:
                legacy = cur.execute(
                    "SELECT DISTINCT id,url FROM mentions WHERE source='hackernews' "
                    "AND source_item_id IS NULL"
                ).fetchall()
                for row in legacy:
                    native_id = self._hn_native_id(row["url"])
                    if native_id:
                        cur.execute("UPDATE mentions SET source_item_id=? WHERE id=?",
                                    (native_id, row["id"]))
                ids = [r[0] for r in cur.execute("SELECT DISTINCT id FROM mentions")]
                for item_id in ids:
                    self._sync_observation(cur, item_id, now)
                cur.execute("INSERT OR REPLACE INTO observation_meta(key,value) "
                            "VALUES ('native_identity_version','1')")

    @staticmethod
    def _hn_native_id(url: str | None) -> str | None:
        """Recover only HN's documented item identity, never a linked article ID."""
        if not url:
            return None
        try:
            parsed = urlsplit(url)
            ids = parse_qs(parsed.query).get("id", [])
            if (parsed.scheme in {"http", "https"} and parsed.hostname == "news.ycombinator.com"
                    and parsed.path == "/item" and len(ids) == 1 and ids[0].isascii()
                    and ids[0].isdigit()):
                return ids[0]
        except ValueError:
            pass
        return None

    def resolve_mentions(self, mentions: list[Mention]) -> None:
        """Preserve retained canonical IDs when a native-aware adapter replaces legacy data.

        Exact URL reconciliation requires the same source scope, no conflicting
        native identity, and (for RSS) a known feed. Bluesky handles can change
        owners, so a legacy handle URL is never promoted to an AT identity.
        """
        with closing(self._conn.cursor()) as cur:
            for mention in mentions:
                self._resolve_mention(cur, mention)

    def _resolve_mention(self, cur: sqlite3.Cursor, mention: Mention) -> None:
        if mention.source == "hackernews" and not mention.source_item_id:
            mention.source_item_id = self._hn_native_id(mention.url)
        native = cur.execute(
            "SELECT DISTINCT id,source_item_id FROM mentions WHERE source_id=? AND source_item_id=?",
            (mention.source_id, mention.source_item_id),
        ).fetchall() if mention.source_item_id else []
        if not native and mention.url and mention.source != "bluesky" and (
            mention.source != "rss" or mention.source_id.startswith("rss:")
        ):
            native = cur.execute(
                "SELECT DISTINCT id,source_item_id FROM mentions WHERE source_id=? AND url=? "
                "AND (source_item_id IS NULL OR source_item_id=?)",
                (mention.source_id, mention.url, mention.source_item_id),
            ).fetchall()
        if len(native) > 1:
            raise ValueError("Conflicting retained object identities require explicit reconciliation")
        if native:
            mention.id = native[0]["id"]
            mention.source_item_id = mention.source_item_id or native[0]["source_item_id"]

    def _sync_observation(self, cur: sqlite3.Cursor, item_id: str, now: str) -> None:
        """Commit an object reference/change with the owning mutation transaction.

        Query memberships stay in mentions; the head/journal never copy bodies.
        All source fields for a live object are reconciled during upsert.
        """
        old = cur.execute("SELECT * FROM observation_heads WHERE id=?", (item_id,)).fetchone()
        rows = cur.execute(
            "SELECT * FROM mentions WHERE id=? ORDER BY fetched_at,query", (item_id,)
        ).fetchall()
        if rows:
            row = rows[0]
            evidence = {key: row[key] for key in (
                "source", "source_id", "source_item_id", "author_id", "author", "url",
                "title", "text", "published_at", "publication_provenance", "indexed_at",
                "source_updated_at", "body_expired", "score",
            )}
            revision = hashlib.sha256(json.dumps(
                evidence, sort_keys=True, separators=(",", ":")
            ).encode()).hexdigest()
            reference = {key: row[key] for key in (
                "id", "source", "source_id", "source_item_id", "author_id", "author", "url",
                "published_at", "publication_provenance", "indexed_at", "source_updated_at",
            )}
            reference.update(
                revision=revision, queries=sorted({r["query"] for r in rows}),
                fetched_at=row["fetched_at"],
                availability="expired" if row["body_expired"] else "available",
                identity_provenance="native" if row["source_item_id"] else "legacy_uncertain",
            )
        elif old:
            reference = json.loads(old["reference"])
            reference.update(availability="removed", queries=[], removed_at=now)
            self._scrub_identity(reference)
            # A recent edit must not extend an old identity's lifetime. Scrub
            # every retained journal version, preserving sequence and membership
            # references so existing continuations can still receive removals.
            events = cur.execute("SELECT seq,reference FROM observation_changes "
                                 "WHERE json_extract(reference,'$.id')=?", (item_id,)).fetchall()
            for event_row in events:
                event_reference = json.loads(event_row["reference"])
                self._scrub_identity(event_reference)
                cur.execute("UPDATE observation_changes SET reference=? WHERE seq=?",
                            (json.dumps(event_reference, sort_keys=True), event_row["seq"]))
            reference["revision"] = hashlib.sha256(
                f"{old['revision']}:removed".encode()
            ).hexdigest()
        else:
            return
        serialized = json.dumps(reference, sort_keys=True)
        if old and old["reference"] == serialized:
            return
        kind = ("removed" if not rows else "expired" if reference["availability"] == "expired"
                else "created" if old is None else "updated")
        event = {**reference, "kind": kind, "changed_at": now}
        # Membership before removal is needed to deliver tombstones to scoped readers.
        event["matched_queries"] = sorted(set(reference["queries"]) | (
            set(json.loads(old["reference"])["queries"]) if old else set()
        ))
        cur.execute(
            "INSERT INTO observation_changes(changed_at,reference) VALUES (?,?)",
            (now, json.dumps(event, sort_keys=True)),
        )
        cur.execute(
            "INSERT INTO observation_heads(id,revision,reference) VALUES (?,?,?) "
            "ON CONFLICT(id) DO UPDATE SET revision=excluded.revision,reference=excluded.reference",
            (item_id, reference["revision"], serialized),
        )

    @staticmethod
    def _scrub_identity(reference: dict) -> None:
        for key in ("author", "author_id", "url", "source_item_id", "published_at",
                    "indexed_at", "source_updated_at"):
            reference[key] = None
        reference.update(publication_provenance="unavailable", identity_provenance="unavailable",
                         availability="removed")

    def observation(self, item_id: str) -> dict | None:
        self._conn.execute("BEGIN")
        try:
            head = self._conn.execute(
                "SELECT reference FROM observation_heads WHERE id=?", (item_id,)
            ).fetchone()
            if head is None:
                return None
            reference = json.loads(head[0])
            row = self._conn.execute(
                "SELECT title,text FROM mentions WHERE id=? ORDER BY fetched_at,query LIMIT 1",
                (item_id,),
            ).fetchone()
            reference.update(title=row[0] if row else None, text=row[1] if row else None)
            return reference
        finally:
            self._conn.rollback()

    def record_attempt(self, record: dict) -> dict:
        """Append a state revision of one attempt; the latest attempt_id wins."""
        with self._conn, closing(self._conn.cursor()) as cur:
            return self._insert_attempt(cur, record)

    def _insert_attempt(self, cur: sqlite3.Cursor, record: dict) -> dict:
        allowed = {
            "source", "source_id", "source_ids", "query", "profile_id", "profile_version",
            "method", "attempted_at", "finished_at", "requested_since", "effective_since",
            "page_limit", "row_limit_per_page", "truncated", "http_requests", "provider_status",
            "retry_after_seconds", "next_eligible_at", "execution", "returned_count", "pages",
            "retries", "duration_seconds", "error", "error_class", "completeness", "reason",
            "attempt_id", "acquisition", "landing", "landed_at", "stored_count",
        }
        value = {key: item for key, item in record.items() if key in allowed}
        value.setdefault("attempt_id", secrets.token_hex(16))
        scope = {key: value.get(key) for key in (
            "source", "source_id", "source_ids", "query", "profile_id", "profile_version",
            "method", "requested_since", "effective_since", "page_limit", "row_limit_per_page",
        )}
        if scope["source_ids"] is not None:
            scope["source_ids"] = sorted(set(scope["source_ids"]))
        value["attempt_scope_digest"] = hashlib.sha256(
            json.dumps(scope, sort_keys=True).encode()
        ).hexdigest()
        previous = cur.execute(
            "SELECT record FROM collection_attempts "
            "WHERE json_extract(record,'$.attempt_scope_digest')=? ORDER BY seq DESC LIMIT 1",
            (value["attempt_scope_digest"],),
        ).fetchone()
        value["last_success_at"] = (
            value["landed_at"] if value.get("acquisition") == "completed"
            and value.get("landing") == "stored" else
            json.loads(previous[0]).get("last_success_at") if previous else None
        )
        value["recorded_at"] = datetime.now(timezone.utc).isoformat()
        cur.execute(
            "INSERT INTO collection_attempts(finished_at,record) VALUES (?,?)",
            (value["recorded_at"], json.dumps(value, sort_keys=True)),
        )
        return value

    @property
    def store_epoch(self) -> str:
        return self._conn.execute(
            "SELECT value FROM observation_meta WHERE key='epoch'"
        ).fetchone()[0]

    def rotate_epoch(self, expected_epoch: str) -> str:
        """Invalidate continuations after an operator-directed history restore."""
        with self._conn:
            self._conn.execute("BEGIN IMMEDIATE")
            if self.store_epoch != expected_epoch:
                raise CursorError("cursor_epoch_mismatch", "Store epoch changed; inspect it before rotation.")
            epoch = secrets.token_hex(16)
            self._conn.executemany("UPDATE observation_meta SET value=? WHERE key=?", [
                (epoch, "epoch"), (secrets.token_hex(32), "cursor_key"),
            ])
            return epoch

    def _seal_cursor(self, payload: dict) -> str:
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        key = self._conn.execute(
            "SELECT value FROM observation_meta WHERE key='cursor_key'"
        ).fetchone()[0].encode()
        signed = raw + hmac.new(key, raw, hashlib.sha256).digest()
        return base64.urlsafe_b64encode(signed).decode().rstrip("=")

    def _open_cursor(self, token: str) -> dict:
        try:
            if len(token) > 4096:
                raise ValueError()
            signed = base64.b64decode(token + "=" * (-len(token) % 4), altchars=b"-_", validate=True)
            raw, mac = signed[:-32], signed[-32:]
            value = json.loads(raw)
            if not isinstance(value, dict):
                raise ValueError()
            if value.get("epoch") != self.store_epoch:
                raise CursorError("cursor_epoch_mismatch", "Store epoch changed; select a new baseline.")
            key = self._conn.execute(
                "SELECT value FROM observation_meta WHERE key='cursor_key'"
            ).fetchone()[0].encode()
            if not hmac.compare_digest(mac, hmac.new(key, raw, hashlib.sha256).digest()):
                raise ValueError()
            return value
        except CursorError:
            raise
        except (ValueError, TypeError, UnicodeError) as exc:
            raise CursorError("invalid_cursor", "Invalid continuation; preserve the old checkpoint.") from exc

    def observation_changes(
        self, *, cursor: str | None = None, source: str | None = None,
        query: str | None = None, profile_id: str | None = None,
        profile_version: str | None = None, limit: int = 200,
    ) -> dict:
        return self._reference_page("changes", cursor=cursor, source=source, query=query,
                                    profile_id=profile_id, profile_version=profile_version, limit=limit)

    def coverage_page(self, **kwargs) -> dict:
        return self._reference_page("coverage", **kwargs)

    def _reference_page(
        self, stream: str, *, cursor: str | None = None, source: str | None = None,
        query: str | None = None, profile_id: str | None = None,
        profile_version: str | None = None, limit: int = 200,
    ) -> dict:
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        if (profile_id is None) != (profile_version is None):
            raise ValueError("Supply profile_id and profile_version together")
        scope = {"stream": stream, "source": source, "query": query,
                 "profile_id": profile_id, "profile_version": profile_version}
        digest = hashlib.sha256(json.dumps(scope, sort_keys=True).encode()).hexdigest()
        table, column = (("observation_changes", "reference") if stream == "changes"
                         else ("collection_attempts", "record"))
        # A read transaction fixes epoch, horizon and page rows together.
        with closing(self._conn.cursor()) as cur:
            cur.execute("BEGIN")
            try:
                epoch = self.store_epoch
                high = cur.execute(f"SELECT COALESCE(MAX(seq),0) FROM {table}").fetchone()[0]
                floor = int(cur.execute(
                    "SELECT value FROM observation_meta WHERE key=?", (f"{stream}_floor",)
                ).fetchone()[0])
                high = max(high, floor)
                position = self._open_cursor(cursor) if cursor else {"after": floor, "upper": None}
                if cursor and position.get("scope") != digest:
                    raise CursorError("cursor_scope_mismatch", "Continuation scope differs; select a new baseline.")
                after = position["after"]
                upper = high if position["upper"] is None else position["upper"]
                if after < floor:
                    raise CursorError("cursor_expired", "Change history expired; select a bounded new baseline.")
                if upper > high or after > upper:
                    raise CursorError("cursor_history_gap", "Store history regressed; select a new baseline.")
                sql = f"SELECT seq,{column} FROM {table} WHERE seq>? AND seq<=?"
                args: list = [after, upper]
                if source is not None:
                    sql += f" AND json_extract({column},'$.source')=?"
                    args.append(source)
                if query is not None:
                    if stream == "changes":
                        sql += f" AND EXISTS(SELECT 1 FROM json_each({column},'$.matched_queries') WHERE value=?)"
                    else:
                        sql += f" AND json_extract({column},'$.query')=?"
                    args.append(query)
                sql += " ORDER BY seq LIMIT ?"
                args.append(limit + 1)
                found = cur.execute(sql, args).fetchall()
                rows = [{**json.loads(r[1]), "seq": r[0]} for r in found[:limit]]
                has_more = len(found) > limit
                next_after = rows[-1]["seq"] if has_more else upper
                payload = {"version": 1, "epoch": epoch, "scope": digest,
                           "after": next_after, "upper": upper if has_more else None}
                checkpoint = self._seal_cursor(payload)
                return {"schema_version": "harken.observations.v1", "store_epoch": epoch,
                        "scope_digest": digest, "scope": scope, "upper_seq": upper,
                        "retained_after_seq": floor, "rows": rows, "has_more": has_more,
                        "next_cursor": checkpoint if has_more else None, "checkpoint": checkpoint}
            finally:
                self._conn.rollback()

    def close(self) -> None:
        self._conn.close()

    def check(self) -> None:
        """Raise if SQLite cannot execute a read and a small write transaction."""
        with closing(self._conn.cursor()) as cur:
            cur.execute("SELECT 1")
            cur.execute("BEGIN IMMEDIATE")
            cur.execute("ROLLBACK")

    def backup(self, destination: str | Path, *, overwrite: bool = False) -> Path:
        """Create a transactionally consistent SQLite backup."""
        if self.path == ":memory:":
            source = None
        else:
            source = Path(self.path).resolve()
        target = Path(destination).expanduser().resolve()
        if source is not None and target == source:
            raise ValueError("backup destination must differ from the active database")
        if target.exists() and not overwrite:
            raise FileExistsError(f"backup already exists: {target}")
        target.parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(target)) as destination_conn:
            self._conn.backup(destination_conn)
        return target

    def count_before(self, cutoff: datetime, query: str | None = None) -> int:
        """Count mentions older than an absolute timestamp."""
        where, args = _before_filter(cutoff, query)
        with closing(self._conn.cursor()) as cur:
            cur.execute(f"SELECT COUNT(*) AS n FROM mentions{where}", args)
            return cur.fetchone()["n"]

    def delete_before(self, cutoff: datetime, query: str | None = None) -> int:
        """Delete old mentions and their alert state in one transaction."""
        where, args = _before_filter(cutoff, query)
        alert_where, alert_args = _before_filter(cutoff, query, table="m")
        with closing(self._conn.cursor()) as cur:
            cur.execute("BEGIN IMMEDIATE")
            affected = [r[0] for r in cur.execute(f"SELECT DISTINCT id FROM mentions{where}", args)]
            cur.execute(
                f"""
                DELETE FROM alert_outbox
                WHERE EXISTS (
                    SELECT 1 FROM mentions AS m{alert_where}
                    AND m.query = alert_outbox.query AND m.id = alert_outbox.mention_id
                )
                """,
                alert_args,
            )
            cur.execute(f"DELETE FROM mentions{where}", args)
            deleted = max(cur.rowcount, 0)
            for item_id in affected:
                self._sync_observation(cur, item_id, datetime.now(timezone.utc).isoformat())
            self._conn.commit()
        return deleted

    def __enter__(self) -> Store:
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # -- local accounts -----------------------------------------------------
    def create_user(self, username: str, password_hash: str, role: str = "viewer") -> dict:
        username = validate_username(username)
        role = validate_role(role)
        now = datetime.now(timezone.utc).isoformat()
        try:
            with closing(self._conn.cursor()) as cur:
                cur.execute("BEGIN IMMEDIATE")
                cur.execute("SELECT COUNT(*) AS n FROM users WHERE active = 1")
                if cur.fetchone()["n"] == 0 and role != "admin":
                    raise ValueError("the first active user must be an admin")
                cur.execute(
                    """
                    INSERT INTO users (username, password_hash, role, active, created_at, updated_at)
                    VALUES (?, ?, ?, 1, ?, ?)
                    """,
                    (username, password_hash, role, now, now),
                )
                user_id = cur.lastrowid
            self._conn.commit()
        except sqlite3.IntegrityError as exc:
            self._conn.rollback()
            raise ValueError(f"user already exists: {username}") from exc
        except Exception:
            self._conn.rollback()
            raise
        user = self.user(user_id)
        if user is None:  # pragma: no cover - defensive after committed insert
            raise RuntimeError("created user could not be read")
        return user

    def users(self) -> list[dict]:
        with closing(self._conn.cursor()) as cur:
            cur.execute(
                """
                SELECT id, username, role, active, created_at, updated_at, last_login_at
                FROM users ORDER BY username COLLATE NOCASE
                """
            )
            return [_user_view(row) for row in cur.fetchall()]

    def user(self, user_id: int) -> dict | None:
        with closing(self._conn.cursor()) as cur:
            cur.execute(
                """
                SELECT id, username, role, active, created_at, updated_at, last_login_at
                FROM users WHERE id = ?
                """,
                (user_id,),
            )
            row = cur.fetchone()
            return _user_view(row) if row else None

    def user_for_login(self, username: str) -> dict | None:
        with closing(self._conn.cursor()) as cur:
            cur.execute(
                """
                SELECT id, username, password_hash, role, active,
                       created_at, updated_at, last_login_at
                FROM users WHERE username = ? COLLATE NOCASE
                """,
                (username.strip(),),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def set_user_password(self, user_id: int, password_hash: str) -> bool:
        now = datetime.now(timezone.utc).isoformat()
        with closing(self._conn.cursor()) as cur:
            cur.execute(
                "UPDATE users SET password_hash = ?, updated_at = ? WHERE id = ?",
                (password_hash, now, user_id),
            )
            updated = bool(cur.rowcount)
            if updated:
                cur.execute("DELETE FROM auth_sessions WHERE user_id = ?", (user_id,))
        self._conn.commit()
        return updated

    def set_user_role(self, user_id: int, role: str) -> bool:
        role = validate_role(role)
        now = datetime.now(timezone.utc).isoformat()
        try:
            with closing(self._conn.cursor()) as cur:
                cur.execute("BEGIN IMMEDIATE")
                cur.execute("SELECT role, active FROM users WHERE id = ?", (user_id,))
                current = cur.fetchone()
                if current is None:
                    self._conn.rollback()
                    return False
                if current["role"] == "admin" and current["active"] and role != "admin":
                    self._ensure_another_admin(cur, user_id)
                cur.execute(
                    "UPDATE users SET role = ?, updated_at = ? WHERE id = ?",
                    (role, now, user_id),
                )
            self._conn.commit()
            return True
        except Exception:
            self._conn.rollback()
            raise

    def set_user_active(self, user_id: int, active: bool) -> bool:
        now = datetime.now(timezone.utc).isoformat()
        try:
            with closing(self._conn.cursor()) as cur:
                cur.execute("BEGIN IMMEDIATE")
                cur.execute("SELECT role, active FROM users WHERE id = ?", (user_id,))
                current = cur.fetchone()
                if current is None:
                    self._conn.rollback()
                    return False
                if current["role"] == "admin" and current["active"] and not active:
                    self._ensure_another_admin(cur, user_id)
                cur.execute(
                    "UPDATE users SET active = ?, updated_at = ? WHERE id = ?",
                    (int(active), now, user_id),
                )
                if not active:
                    cur.execute("DELETE FROM auth_sessions WHERE user_id = ?", (user_id,))
            self._conn.commit()
            return True
        except Exception:
            self._conn.rollback()
            raise

    def delete_user(self, user_id: int) -> bool:
        try:
            with closing(self._conn.cursor()) as cur:
                cur.execute("BEGIN IMMEDIATE")
                cur.execute("SELECT role, active FROM users WHERE id = ?", (user_id,))
                current = cur.fetchone()
                if current is None:
                    self._conn.rollback()
                    return False
                if current["role"] == "admin" and current["active"]:
                    self._ensure_another_admin(cur, user_id)
                cur.execute("DELETE FROM users WHERE id = ?", (user_id,))
            self._conn.commit()
            return True
        except Exception:
            self._conn.rollback()
            raise

    def create_session(
        self,
        user_id: int,
        token_hash: str,
        expires_at: datetime,
        *,
        now: datetime | None = None,
    ) -> None:
        created = (now or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat()
        expiry = expires_at.astimezone(timezone.utc).isoformat()
        with closing(self._conn.cursor()) as cur:
            cur.execute("DELETE FROM auth_sessions WHERE expires_at <= ?", (created,))
            cur.execute(
                """
                INSERT INTO auth_sessions
                    (token_hash, user_id, created_at, expires_at, last_seen_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (token_hash, user_id, created, expiry, created),
            )
            # Bound stolen/forgotten browser sessions per account.
            cur.execute(
                """
                DELETE FROM auth_sessions WHERE token_hash IN (
                    SELECT token_hash FROM auth_sessions WHERE user_id = ?
                    ORDER BY created_at DESC LIMIT -1 OFFSET 20
                )
                """,
                (user_id,),
            )
            cur.execute(
                "UPDATE users SET last_login_at = ?, updated_at = ? WHERE id = ?",
                (created, created, user_id),
            )
        self._conn.commit()

    def session_user(self, token_hash: str, *, now: datetime | None = None) -> dict | None:
        current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat()
        with closing(self._conn.cursor()) as cur:
            cur.execute(
                """
                SELECT u.id, u.username, u.role, u.active,
                       u.created_at, u.updated_at, u.last_login_at
                FROM auth_sessions AS s
                JOIN users AS u ON u.id = s.user_id
                WHERE s.token_hash = ? AND s.expires_at > ? AND u.active = 1
                """,
                (token_hash, current),
            )
            row = cur.fetchone()
        return _user_view(row) if row else None

    def delete_session(self, token_hash: str) -> None:
        with closing(self._conn.cursor()) as cur:
            cur.execute("DELETE FROM auth_sessions WHERE token_hash = ?", (token_hash,))
        self._conn.commit()

    @staticmethod
    def _ensure_another_admin(cur: sqlite3.Cursor, excluded_user_id: int) -> None:
        cur.execute(
            """
            SELECT COUNT(*) AS n FROM users
            WHERE role = 'admin' AND active = 1 AND id != ?
            """,
            (excluded_user_id,),
        )
        if cur.fetchone()["n"] == 0:
            raise ValueError("cannot remove or demote the last active admin")

    # -- writes --------------------------------------------------------------
    def upsert(
        self, mentions: list[Mention], *, update_theme: bool = True,
        attempts: list[dict] | None = None,
    ) -> int:
        """Insert or replace mentions. Returns count of *new* rows.

        ``update_theme=False`` leaves an existing row's ``theme`` untouched. Use
        it for the pre-cluster ingest of freshly fetched mentions (which carry no
        theme), so re-ingesting cannot transiently null an already-computed
        label. The post-cluster write uses the default so it can both set and
        *clear* labels (a mention that falls out of every cluster becomes NULL).
        """
        now = datetime.now(timezone.utc).isoformat()
        new = 0
        finalized_attempts: list[tuple[dict, dict]] = []
        landed: dict[tuple[str, str], set[str]] = {}
        with self._conn, closing(self._conn.cursor()) as cur:
            observed_sources: dict[str, list[str]] = {}
            for mention in mentions:
                sources = observed_sources.setdefault(mention.query, [])
                if mention.source not in sources:
                    sources.append(mention.source)
            for query, sources in observed_sources.items():
                cur.execute(
                    """
                    INSERT OR IGNORE INTO tracked_queries
                        (query, sources, created_at, updated_at, last_scanned_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (query, json.dumps(sources), now, now, now),
                )
                if cur.rowcount:
                    cur.execute(
                        """
                        INSERT OR IGNORE INTO project_queries (project_id, query, added_at)
                        VALUES (?, ?, ?)
                        """,
                        (DEFAULT_PROJECT_ID, query, now),
                    )
            # When update_theme is False the theme column is left out of the
            # UPDATE, preserving the stored label; otherwise it is set from the
            # incoming value (which may be NULL to clear a de-clustered label).
            theme_update = (
                "theme=CASE WHEN mentions.body_expired=1 THEN NULL ELSE excluded.theme END, "
                if update_theme else ""
            )
            upsert_sql = f"""
                INSERT INTO mentions
                    (id, source, query, author, title, text, url, created_at,
                     score, sentiment, sentiment_score, theme, fetched_at,
                     published_at, publication_provenance, source_updated_at, source_id,
                     source_item_id, author_id, indexed_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(id, query) DO UPDATE SET
                    source=excluded.source,
                    author=excluded.author,
                    title=CASE WHEN mentions.body_expired=1 THEN NULL ELSE excluded.title END,
                    text=CASE WHEN mentions.body_expired=1 THEN NULL ELSE excluded.text END,
                    url=excluded.url,
                    created_at=excluded.created_at,
                    sentiment=excluded.sentiment,
                    sentiment_score=excluded.sentiment_score,
                    {theme_update}score=excluded.score,
                    published_at=excluded.published_at,
                    publication_provenance=excluded.publication_provenance,
                    source_updated_at=excluded.source_updated_at,
                    source_id=excluded.source_id,
                    source_item_id=excluded.source_item_id,
                    author_id=excluded.author_id,
                    indexed_at=excluded.indexed_at
            """
            for m in mentions:
                self._resolve_mention(cur, m)
                head = cur.execute(
                    "SELECT reference FROM observation_heads WHERE id=?", (m.id,)
                ).fetchone()
                prior = json.loads(head[0]) if head else None
                if prior and prior["availability"] == "removed":
                    continue  # A retained tombstone cannot resurrect an expired identity.
                cur.execute("SELECT 1 FROM mentions WHERE id = ? AND query = ?", (m.id, m.query))
                existed = cur.fetchone() is not None
                cur.execute(
                    upsert_sql,
                    (
                        m.id,
                        m.source,
                        m.query,
                        m.author,
                        m.title,
                        m.text,
                        m.url,
                        m.created_at.isoformat(),
                        m.score,
                        m.sentiment.value if m.sentiment else None,
                        m.sentiment_score,
                        m.theme,
                        now,
                        m.published_at.isoformat() if m.published_at else None,
                        m.publication_provenance,
                        m.source_updated_at.isoformat() if m.source_updated_at else None,
                        m.source_id,
                        m.source_item_id,
                        m.author_id,
                        m.indexed_at.isoformat() if m.indexed_at else None,
                    ),
                )
                expired = cur.execute(
                    "SELECT 1 FROM mentions WHERE id=? AND body_expired=1", (m.id,)
                ).fetchone()
                if expired or (prior and prior["availability"] in {"expired", "removed"}):
                    cur.execute(
                        "UPDATE mentions SET text=NULL,title=NULL,theme=NULL,body_expired=1 WHERE id=?",
                        (m.id,),
                    )
                if prior:
                    cur.execute("UPDATE mentions SET fetched_at=? WHERE id=?",
                                (prior["fetched_at"], m.id))
                # An object edit applies to all query memberships. The query-specific
                # theme remains separate and is not part of the source revision.
                object_fields = (
                    "source", "source_id", "source_item_id", "author_id", "author", "url",
                    "title", "text", "created_at", "published_at", "publication_provenance",
                    "indexed_at", "source_updated_at", "score", "sentiment", "sentiment_score",
                )
                names = ",".join(object_fields)
                cur.execute(
                    f"UPDATE mentions SET ({names})=(SELECT {names} FROM mentions WHERE id=? AND query=?) "
                    "WHERE id=? AND query!=?", (m.id, m.query, m.id, m.query),
                )
                self._sync_observation(cur, m.id, now)
                landed.setdefault((m.source, m.query), set()).add(m.id)
                if not existed:
                    new += 1
            # The landing receipt commits with bodies, matches and changes. A
            # reader can never see a stored receipt without its durable evidence.
            for attempt in attempts or []:
                if attempt["landing"] == "pending":
                    final = self._insert_attempt(cur, {
                        **attempt, "landing": "stored", "landed_at": now,
                        "stored_count": len(landed.get((attempt["source"], attempt["query"]), set())),
                        "execution": attempt["acquisition"],
                    })
                    finalized_attempts.append((attempt, final))
        for attempt, final in finalized_attempts:
            attempt.update(final)
        self._conn.commit()
        return new

    def retention(
        self, *, now: datetime, source: str | None = None, apply: bool = False,
    ) -> dict[str, int]:
        """Preview or explicitly apply 30-day body / 90-day identity retention.

        Cutoffs use immutable first ingestion, not source publication. Expired
        bodies cannot be resurrected by a replay while the identity is retained.
        """
        if now.tzinfo is None:
            raise ValueError("Retention time must include a timezone")
        body_cutoff = (now - timedelta(days=30)).astimezone(timezone.utc).isoformat()
        identity_cutoff = (now - timedelta(days=90)).astimezone(timezone.utc).isoformat()
        scope = " AND source=?" if source is not None else ""
        source_args = [source] if source is not None else []
        with closing(self._conn.cursor()) as cur:
            if apply:
                cur.execute("BEGIN IMMEDIATE")
            try:
                counts = {
                    "bodies": cur.execute(
                        "SELECT COUNT(*) FROM mentions WHERE fetched_at<? AND fetched_at>=? "
                        "AND body_expired=0" + scope,
                        [body_cutoff, identity_cutoff, *source_args],
                    ).fetchone()[0],
                    "identities": cur.execute(
                        "SELECT COUNT(*) FROM mentions WHERE fetched_at<?" + scope,
                        [identity_cutoff, *source_args],
                    ).fetchone()[0],
                    "alert_payloads": cur.execute(
                        "SELECT COUNT(*) FROM threshold_alerts WHERE query IN "
                        "(SELECT query FROM mentions WHERE fetched_at<?" + scope + ")",
                        [body_cutoff, *source_args],
                    ).fetchone()[0],
                }
                if apply:
                    affected = [r[0] for r in cur.execute(
                        "SELECT DISTINCT id FROM mentions WHERE fetched_at<?" + scope,
                        [body_cutoff, *source_args],
                    )]
                    # Threshold payloads may quote multiple sources. They have
                    # no item-level provenance, so discard the affected query's
                    # payloads rather than retain an expired body indirectly.
                    cur.execute(
                        "DELETE FROM threshold_alerts WHERE query IN "
                        "(SELECT query FROM mentions WHERE fetched_at<?" + scope + ")",
                        [body_cutoff, *source_args],
                    )
                    cur.execute(
                        "DELETE FROM alert_outbox WHERE (mention_id,query) IN "
                        "(SELECT id,query FROM mentions WHERE fetched_at<?" + scope + ")",
                        [identity_cutoff, *source_args],
                    )
                    cur.execute("DELETE FROM mentions WHERE fetched_at<?" + scope,
                                [identity_cutoff, *source_args])
                    cur.execute(
                        "UPDATE mentions SET text=NULL,title=NULL,theme=NULL,body_expired=1 "
                        "WHERE fetched_at<? AND body_expired=0" + scope,
                        [body_cutoff, *source_args],
                    )
                    for item_id in affected:
                        if cur.execute("SELECT 1 FROM mentions WHERE id=? AND body_expired=1",
                                       (item_id,)).fetchone():
                            cur.execute("UPDATE mentions SET text=NULL,title=NULL,theme=NULL,body_expired=1 "
                                        "WHERE id=?", (item_id,))
                        self._sync_observation(cur, item_id, now.astimezone(timezone.utc).isoformat())
                    for stream, table, time_column in (
                        ("changes", "observation_changes", "changed_at"),
                        ("coverage", "collection_attempts", "finished_at"),
                    ):
                        # A source-scoped action must not erase another source's
                        # history or advance the global continuation floor.
                        if source is not None:
                            continue
                        floor = cur.execute(
                            f"SELECT COALESCE(MAX(seq),0) FROM {table} WHERE {time_column}<?",
                            (identity_cutoff,),
                        ).fetchone()[0]
                        if floor:
                            cur.execute(f"DELETE FROM {table} WHERE seq<=?", (floor,))
                            cur.execute(
                                "UPDATE observation_meta SET value=? WHERE key=?",
                                (str(floor), f"{stream}_floor"),
                            )
                    cur.execute(
                        "DELETE FROM observation_heads WHERE json_extract(reference,'$.removed_at')<?"
                        + (" AND json_extract(reference,'$.source')=?" if source else ""),
                        [identity_cutoff, *source_args],
                    )
                    self._conn.commit()
                return counts
            except Exception:
                if apply:
                    self._conn.rollback()
                raise

    def save_tracking(
        self, query: str, sources: list[str], *, project_id: int | None = None
    ) -> None:
        """Persist a keyword even when a scan returns no mentions."""
        query = query.strip()
        normalized = list(
            dict.fromkeys(source.strip().lower() for source in sources if source.strip())
        )
        if not query:
            raise ValueError("query must not be empty")
        if not normalized:
            raise ValueError("at least one source must be configured")
        now = datetime.now(timezone.utc).isoformat()
        with closing(self._conn.cursor()) as cur:
            if project_id is not None:
                cur.execute("SELECT 1 FROM projects WHERE id = ?", (project_id,))
                if cur.fetchone() is None:
                    raise ValueError(f"unknown project: {project_id}")
            cur.execute(
                """
                INSERT INTO tracked_queries (query, sources, created_at, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(query) DO UPDATE SET
                    sources = excluded.sources,
                    updated_at = excluded.updated_at
                """,
                (query, json.dumps(normalized), now, now),
            )
            selected_project_id = project_id
            if selected_project_id is None:
                cur.execute("SELECT 1 FROM project_queries WHERE query = ? LIMIT 1", (query,))
                if cur.fetchone() is None:
                    selected_project_id = DEFAULT_PROJECT_ID
            if selected_project_id is not None:
                cur.execute(
                    """
                    INSERT OR IGNORE INTO project_queries (project_id, query, added_at)
                    VALUES (?, ?, ?)
                    """,
                    (selected_project_id, query, now),
                )
        self._conn.commit()

    def create_project(self, name: str) -> dict:
        """Create a named keyword group and return its persisted representation."""
        normalized = " ".join(name.split())
        if not normalized:
            raise ValueError("project name must not be empty")
        if len(normalized) > 100:
            raise ValueError("project name must be at most 100 characters")
        now = datetime.now(timezone.utc).isoformat()
        try:
            with closing(self._conn.cursor()) as cur:
                cur.execute(
                    "INSERT INTO projects (name, created_at, updated_at) VALUES (?, ?, ?)",
                    (normalized, now, now),
                )
                project_id = cur.lastrowid
            self._conn.commit()
        except sqlite3.IntegrityError as exc:
            self._conn.rollback()
            raise ValueError(f'a project named "{normalized}" already exists') from exc
        return self.project(project_id) or {}

    def add_query_to_project(self, project_id: int, query: str) -> bool:
        """Associate an existing tracked keyword with a project."""
        normalized = query.strip()
        if not normalized:
            raise ValueError("query must not be empty")
        now = datetime.now(timezone.utc).isoformat()
        with closing(self._conn.cursor()) as cur:
            cur.execute("SELECT 1 FROM projects WHERE id = ?", (project_id,))
            if cur.fetchone() is None:
                raise ValueError(f"unknown project: {project_id}")
            cur.execute("SELECT 1 FROM tracked_queries WHERE query = ?", (normalized,))
            if cur.fetchone() is None:
                raise ValueError(f'unknown tracked query: "{normalized}"')
            cur.execute(
                """
                INSERT OR IGNORE INTO project_queries (project_id, query, added_at)
                VALUES (?, ?, ?)
                """,
                (project_id, normalized, now),
            )
            added = bool(cur.rowcount)
            if added:
                cur.execute("UPDATE projects SET updated_at = ? WHERE id = ?", (now, project_id))
        self._conn.commit()
        return added

    def remove_query_from_project(self, project_id: int, query: str) -> bool:
        """Remove only the grouping; the tracked keyword and mentions remain intact."""
        if project_id == DEFAULT_PROJECT_ID:
            raise ValueError("keywords cannot be removed from the Default project")
        now = datetime.now(timezone.utc).isoformat()
        with closing(self._conn.cursor()) as cur:
            cur.execute(
                "DELETE FROM project_queries WHERE project_id = ? AND query = ?",
                (project_id, query.strip()),
            )
            removed = bool(cur.rowcount)
            if removed:
                cur.execute("UPDATE projects SET updated_at = ? WHERE id = ?", (now, project_id))
        self._conn.commit()
        return removed

    def delete_project(self, project_id: int) -> bool:
        """Delete a named grouping without deleting its keywords or mentions."""
        if project_id == DEFAULT_PROJECT_ID:
            raise ValueError("the Default project cannot be deleted")
        with closing(self._conn.cursor()) as cur:
            cur.execute("DELETE FROM projects WHERE id = ?", (project_id,))
            deleted = bool(cur.rowcount)
        self._conn.commit()
        return deleted

    def record_source_success(
        self,
        query: str,
        source: str,
        mentions: list[Mention],
        *,
        mode: str,
        next_cursor: str | None,
        incremental_since: datetime | None = None,
    ) -> None:
        """Advance a source cursor after its fetched rows have been committed."""
        state = self.source_state(query, source)
        observed = [mention.created_at.astimezone(timezone.utc).isoformat() for mention in mentions]
        newest_at = _latest_timestamp(state.get("newest_at"), max(observed, default=None))
        oldest_at = _earliest_timestamp(state.get("oldest_at"), min(observed, default=None))

        incremental_cursor = state.get("incremental_cursor")
        incremental_since_value = state.get("incremental_since")
        backfill_cursor = state.get("backfill_cursor")
        backfill_complete = bool(state.get("backfill_complete"))

        if mode == "backfill":
            backfill_cursor = next_cursor
            backfill_complete = next_cursor is None
        elif state.get("newest_at") is None:
            # The first recent scan establishes both temporal bounds. Its next
            # page is historical, so it seeds the backfill cursor rather than
            # the forward/incremental continuation.
            backfill_cursor = next_cursor
            backfill_complete = next_cursor is None
            incremental_cursor = None
            incremental_since_value = None
        else:
            incremental_cursor = next_cursor
            incremental_since_value = (
                incremental_since.astimezone(timezone.utc).isoformat()
                if next_cursor and incremental_since
                else None
            )

        now = datetime.now(timezone.utc).isoformat()
        with closing(self._conn.cursor()) as cur:
            cur.execute(
                """
                INSERT INTO source_scan_state
                    (query, source, newest_at, oldest_at, incremental_cursor,
                     incremental_since, backfill_cursor, backfill_complete,
                     last_success_at, last_error)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
                ON CONFLICT(query, source) DO UPDATE SET
                    newest_at = excluded.newest_at,
                    oldest_at = excluded.oldest_at,
                    incremental_cursor = excluded.incremental_cursor,
                    incremental_since = excluded.incremental_since,
                    backfill_cursor = excluded.backfill_cursor,
                    backfill_complete = excluded.backfill_complete,
                    last_success_at = excluded.last_success_at,
                    last_error = NULL
                """,
                (
                    query,
                    source,
                    newest_at,
                    oldest_at,
                    incremental_cursor,
                    incremental_since_value,
                    backfill_cursor,
                    int(backfill_complete),
                    now,
                ),
            )
            cur.execute(
                "UPDATE tracked_queries SET last_scanned_at = ?, updated_at = ? WHERE query = ?",
                (now, now, query),
            )
        self._conn.commit()

    def record_source_error(self, query: str, source: str, error: str) -> None:
        """Persist a safe, inspectable source error without moving any cursor."""
        with closing(self._conn.cursor()) as cur:
            cur.execute(
                """
                INSERT INTO source_scan_state (query, source, last_error)
                VALUES (?, ?, ?)
                ON CONFLICT(query, source) DO UPDATE SET last_error = excluded.last_error
                """,
                (query, source, error[:500]),
            )
        self._conn.commit()

    def record_source_metric(
        self,
        source: str,
        *,
        duration_seconds: float,
        fetched: int,
        pages: int,
        retries: int,
        error: str | None = None,
        now: datetime | None = None,
    ) -> None:
        """Atomically accumulate low-cardinality source telemetry."""
        if duration_seconds < 0 or min(fetched, pages, retries) < 0:
            raise ValueError("source metric values must not be negative")
        observed_at = (now or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat()
        succeeded = error is None
        with closing(self._conn.cursor()) as cur:
            cur.execute(
                """
                INSERT INTO source_metrics
                    (source, scans_total, errors_total, fetched_total, pages_total,
                     retries_total, duration_seconds_total, last_duration_seconds,
                     last_fetched, last_pages, last_retries, last_success, last_scan_at,
                     last_success_at, last_error_at, last_error)
                VALUES (?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source) DO UPDATE SET
                    scans_total = source_metrics.scans_total + 1,
                    errors_total = source_metrics.errors_total + excluded.errors_total,
                    fetched_total = source_metrics.fetched_total + excluded.fetched_total,
                    pages_total = source_metrics.pages_total + excluded.pages_total,
                    retries_total = source_metrics.retries_total + excluded.retries_total,
                    duration_seconds_total =
                        source_metrics.duration_seconds_total + excluded.duration_seconds_total,
                    last_duration_seconds = excluded.last_duration_seconds,
                    last_fetched = excluded.last_fetched,
                    last_pages = excluded.last_pages,
                    last_retries = excluded.last_retries,
                    last_success = excluded.last_success,
                    last_scan_at = excluded.last_scan_at,
                    last_success_at = COALESCE(
                        excluded.last_success_at, source_metrics.last_success_at
                    ),
                    last_error_at = COALESCE(excluded.last_error_at, source_metrics.last_error_at),
                    last_error = excluded.last_error
                """,
                (
                    source,
                    int(not succeeded),
                    fetched,
                    pages,
                    retries,
                    duration_seconds,
                    duration_seconds,
                    fetched,
                    pages,
                    retries,
                    int(succeeded),
                    observed_at,
                    observed_at if succeeded else None,
                    observed_at if not succeeded else None,
                    error[:500] if error else None,
                ),
            )
        self._conn.commit()

    def source_metrics(self) -> list[dict]:
        """Return durable per-source counters in stable label order."""
        with closing(self._conn.cursor()) as cur:
            cur.execute("SELECT * FROM source_metrics ORDER BY source")
            return [dict(row) for row in cur.fetchall()]

    def existing_ids(self, query: str, ids: list[str]) -> set[str]:
        """Return IDs already stored for a query, chunked below SQLite's variable limit."""
        unique = list(dict.fromkeys(ids))
        found: set[str] = set()
        with closing(self._conn.cursor()) as cur:
            for start in range(0, len(unique), 900):
                chunk = unique[start : start + 900]
                if not chunk:
                    continue
                placeholders = ",".join("?" for _ in chunk)
                cur.execute(
                    f"SELECT id FROM mentions WHERE query = ? AND id IN ({placeholders})",
                    [query, *chunk],
                )
                found.update(row["id"] for row in cur.fetchall())
        return found

    def enqueue_alerts(self, mentions: list[Mention], target_key: str) -> int:
        """Add mentions to the durable delivery outbox. Returns newly queued rows."""
        now = datetime.now(timezone.utc).isoformat()
        queued = 0
        with closing(self._conn.cursor()) as cur:
            for mention in mentions:
                cur.execute(
                    """
                    INSERT OR IGNORE INTO alert_outbox
                        (query, mention_id, target_key, enqueued_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (mention.query, mention.id, target_key, now),
                )
                queued += max(cur.rowcount, 0)
        self._conn.commit()
        return queued

    def pending_alerts(self, query: str, target_key: str, limit: int = 100) -> list[Mention]:
        """Return undelivered mentions for a target, oldest enqueued first."""
        if limit < 1:
            raise ValueError("limit must be at least 1")
        with closing(self._conn.cursor()) as cur:
            cur.execute(
                """
                SELECT m.*
                FROM alert_outbox AS a
                JOIN mentions AS m ON m.query = a.query AND m.id = a.mention_id
                WHERE a.query = ? AND a.target_key = ? AND a.delivered_at IS NULL
                ORDER BY a.enqueued_at, a.mention_id
                LIMIT ?
                """,
                (query, target_key, limit),
            )
            return [_row_to_mention(row) for row in cur.fetchall()]

    def mark_alerts_delivered(self, query: str, ids: list[str], target_key: str) -> None:
        """Mark one successfully delivered batch."""
        self._update_alert_batch(query, ids, target_key, delivered=True)

    def mark_alerts_failed(self, query: str, ids: list[str], target_key: str, error: str) -> None:
        """Record a sanitized error while leaving the batch pending for retry."""
        self._update_alert_batch(query, ids, target_key, delivered=False, error=error[:500])

    def pending_alert_count(self, query: str, target_key: str) -> int:
        with closing(self._conn.cursor()) as cur:
            cur.execute(
                """
                SELECT COUNT(*) AS n FROM alert_outbox
                WHERE query = ? AND target_key = ? AND delivered_at IS NULL
                """,
                (query, target_key),
            )
            return cur.fetchone()["n"]

    def activate_threshold_alert(
        self,
        query: str,
        event_type: str,
        target_key: str,
        text: str,
        payload: dict,
        *,
        cooldown_hours: int,
        now: datetime | None = None,
    ) -> int | None:
        """Open one alert episode, or reuse the active episode without duplicating it."""
        current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        current_iso = current.isoformat()
        with closing(self._conn.cursor()) as cur:
            cur.execute(
                """
                SELECT id, delivered_at FROM threshold_alerts
                WHERE query = ? AND event_type = ? AND target_key = ? AND cleared_at IS NULL
                """,
                (query, event_type, target_key),
            )
            active = cur.fetchone()
            if active is not None:
                if active["delivered_at"] is None:
                    cur.execute(
                        "UPDATE threshold_alerts SET text = ?, payload = ? WHERE id = ?",
                        (text, json.dumps(payload), active["id"]),
                    )
                    self._conn.commit()
                return active["id"]

            if cooldown_hours:
                cur.execute(
                    """
                    SELECT MAX(delivered_at) AS delivered_at FROM threshold_alerts
                    WHERE query = ? AND event_type = ? AND target_key = ?
                    """,
                    (query, event_type, target_key),
                )
                delivered_at = cur.fetchone()["delivered_at"]
                if delivered_at:
                    last_delivery = datetime.fromisoformat(delivered_at)
                    if current - last_delivery < timedelta(hours=cooldown_hours):
                        return None

            cur.execute(
                """
                INSERT INTO threshold_alerts
                    (query, event_type, target_key, text, payload, triggered_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (query, event_type, target_key, text, json.dumps(payload), current_iso),
            )
            alert_id = cur.lastrowid
        self._conn.commit()
        return alert_id

    def clear_threshold_alert(
        self, query: str, event_type: str, target_key: str, *, now: datetime | None = None
    ) -> None:
        """Clear an active episode so a future crossing can re-arm it."""
        cleared_at = (now or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat()
        with closing(self._conn.cursor()) as cur:
            cur.execute(
                """
                UPDATE threshold_alerts SET cleared_at = ?
                WHERE query = ? AND event_type = ? AND target_key = ? AND cleared_at IS NULL
                """,
                (cleared_at, query, event_type, target_key),
            )
        self._conn.commit()

    def pending_threshold_alerts(self, query: str, target_key: str) -> list[dict]:
        with closing(self._conn.cursor()) as cur:
            cur.execute(
                """
                SELECT * FROM threshold_alerts
                WHERE query = ? AND target_key = ?
                  AND delivered_at IS NULL AND cleared_at IS NULL
                ORDER BY triggered_at, id
                """,
                (query, target_key),
            )
            rows = [dict(row) for row in cur.fetchall()]
        for row in rows:
            row["payload"] = json.loads(row["payload"])
        return rows

    def mark_threshold_alert_delivered(self, alert_id: int, *, now: datetime | None = None) -> None:
        self._mark_threshold_alert(alert_id, delivered=True, now=now)

    def mark_threshold_alert_failed(self, alert_id: int, error: str) -> None:
        self._mark_threshold_alert(alert_id, delivered=False, error=error[:500])

    def threshold_alert_pending_count(self, query: str, target_key: str) -> int:
        with closing(self._conn.cursor()) as cur:
            cur.execute(
                """
                SELECT COUNT(*) AS n FROM threshold_alerts
                WHERE query = ? AND target_key = ?
                  AND delivered_at IS NULL AND cleared_at IS NULL
                """,
                (query, target_key),
            )
            return cur.fetchone()["n"]

    def operational_stats(self) -> dict[str, int]:
        """Small persisted counters suitable for health dashboards and metrics."""
        with closing(self._conn.cursor()) as cur:
            cur.execute("SELECT COUNT(*) AS n FROM mentions")
            mentions = cur.fetchone()["n"]
            cur.execute("SELECT COUNT(*) AS n FROM tracked_queries")
            queries = cur.fetchone()["n"]
            cur.execute("SELECT COUNT(*) AS n FROM alert_outbox WHERE delivered_at IS NULL")
            alerts_pending = cur.fetchone()["n"]
            cur.execute("SELECT COUNT(*) AS n FROM alert_outbox WHERE delivered_at IS NOT NULL")
            alerts_delivered = cur.fetchone()["n"]
            cur.execute(
                """
                SELECT COUNT(*) AS n FROM threshold_alerts
                WHERE delivered_at IS NULL AND cleared_at IS NULL
                """
            )
            threshold_alerts_pending = cur.fetchone()["n"]
            cur.execute("SELECT COUNT(*) AS n FROM threshold_alerts WHERE delivered_at IS NOT NULL")
            threshold_alerts_delivered = cur.fetchone()["n"]
        return {
            "mentions": mentions,
            "queries": queries,
            "alerts_pending": alerts_pending,
            "alerts_delivered": alerts_delivered,
            "threshold_alerts_pending": threshold_alerts_pending,
            "threshold_alerts_delivered": threshold_alerts_delivered,
        }

    def _mark_threshold_alert(
        self,
        alert_id: int,
        *,
        delivered: bool,
        error: str | None = None,
        now: datetime | None = None,
    ) -> None:
        delivered_at = (
            (now or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat()
            if delivered
            else None
        )
        with closing(self._conn.cursor()) as cur:
            cur.execute(
                """
                UPDATE threshold_alerts
                SET attempts = attempts + 1, delivered_at = ?, last_error = ?
                WHERE id = ? AND delivered_at IS NULL AND cleared_at IS NULL
                """,
                (delivered_at, None if delivered else error, alert_id),
            )
        self._conn.commit()

    def _update_alert_batch(
        self,
        query: str,
        ids: list[str],
        target_key: str,
        *,
        delivered: bool,
        error: str | None = None,
    ) -> None:
        unique = list(dict.fromkeys(ids))
        now = datetime.now(timezone.utc).isoformat()
        with closing(self._conn.cursor()) as cur:
            for start in range(0, len(unique), 900):
                chunk = unique[start : start + 900]
                if not chunk:
                    continue
                placeholders = ",".join("?" for _ in chunk)
                delivered_at = now if delivered else None
                cur.execute(
                    f"""
                    UPDATE alert_outbox
                    SET attempts = attempts + 1, delivered_at = ?, last_error = ?
                    WHERE query = ? AND target_key = ? AND delivered_at IS NULL
                      AND mention_id IN ({placeholders})
                    """,
                    [delivered_at, None if delivered else error, query, target_key, *chunk],
                )
        self._conn.commit()

    # -- reads ---------------------------------------------------------------
    def projects(self) -> list[dict]:
        """List project groups with keyword and mention totals."""
        with closing(self._conn.cursor()) as cur:
            cur.execute(
                """
                SELECT p.id, p.name, p.created_at, p.updated_at,
                       COUNT(DISTINCT pq.query) AS query_count,
                       COUNT(m.id) AS mention_count
                FROM projects AS p
                LEFT JOIN project_queries AS pq ON pq.project_id = p.id
                LEFT JOIN mentions AS m ON m.query = pq.query
                GROUP BY p.id
                ORDER BY CASE WHEN p.id = ? THEN 0 ELSE 1 END,
                         p.name COLLATE NOCASE
                """,
                (DEFAULT_PROJECT_ID,),
            )
            return [dict(row) for row in cur.fetchall()]

    def project(self, project_id: int) -> dict | None:
        """Return one project and its ordered member keywords."""
        with closing(self._conn.cursor()) as cur:
            cur.execute(
                """
                SELECT p.id, p.name, p.created_at, p.updated_at,
                       COUNT(DISTINCT pq.query) AS query_count,
                       COUNT(m.id) AS mention_count
                FROM projects AS p
                LEFT JOIN project_queries AS pq ON pq.project_id = p.id
                LEFT JOIN mentions AS m ON m.query = pq.query
                WHERE p.id = ?
                GROUP BY p.id
                """,
                (project_id,),
            )
            row = cur.fetchone()
        if row is None:
            return None
        value = dict(row)
        value["queries"] = self.queries(project_id=project_id)
        return value

    def alert_metrics(
        self,
        query: str,
        *,
        now: datetime | None = None,
        window_hours: int = 24,
        baseline_windows: int = 7,
    ) -> dict:
        """Current-window volume/sentiment and the preceding complete-window baseline."""
        if window_hours < 1 or baseline_windows < 1:
            raise ValueError("alert windows must be at least 1")
        current_time = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        current_start = current_time - timedelta(hours=window_hours)
        baseline_start = current_start - timedelta(hours=window_hours * baseline_windows)
        with closing(self._conn.cursor()) as cur:
            cur.execute(
                """
                SELECT created_at, sentiment FROM mentions
                WHERE query = ? AND datetime(created_at) >= datetime(?)
                  AND datetime(created_at) <= datetime(?)
                """,
                (query, baseline_start.isoformat(), current_time.isoformat()),
            )
            rows = cur.fetchall()

        current_sentiments: list[str | None] = []
        baseline_sentiments: list[str | None] = []
        for row in rows:
            created_at = datetime.fromisoformat(row["created_at"])
            if created_at >= current_start:
                current_sentiments.append(row["sentiment"])
            else:
                baseline_sentiments.append(row["sentiment"])
        baseline_count = len(baseline_sentiments)
        return {
            "current_count": len(current_sentiments),
            "baseline_count": baseline_count,
            "baseline_average": baseline_count / baseline_windows,
            "current_net_sentiment": _net_sentiment(current_sentiments),
            "baseline_net_sentiment": _net_sentiment(baseline_sentiments),
        }

    def tracking(self, query: str) -> dict | None:
        with closing(self._conn.cursor()) as cur:
            cur.execute("SELECT * FROM tracked_queries WHERE query = ?", (query,))
            row = cur.fetchone()
        if row is None:
            return None
        value = dict(row)
        try:
            value["sources"] = json.loads(value["sources"])
        except (TypeError, json.JSONDecodeError):
            value["sources"] = []
        return value

    def source_state(self, query: str, source: str) -> dict:
        with closing(self._conn.cursor()) as cur:
            cur.execute(
                "SELECT * FROM source_scan_state WHERE query = ? AND source = ?",
                (query, source),
            )
            row = cur.fetchone()
        return dict(row) if row is not None else {}

    def source_states(self, query: str) -> list[dict]:
        with closing(self._conn.cursor()) as cur:
            cur.execute("SELECT * FROM source_scan_state WHERE query = ? ORDER BY source", (query,))
            return [dict(row) for row in cur.fetchall()]

    def mentions(
        self,
        query: str | None = None,
        project_id: int | None = None,
        source: str | None = None,
        sentiment: Sentiment | None = None,
        limit: int | None = 500,
    ) -> list[Mention]:
        if limit is not None and limit < 1:
            raise ValueError("limit must be at least 1")
        if query and project_id is not None:
            raise ValueError("query and project_id are mutually exclusive")
        sql = "SELECT * FROM mentions WHERE 1=1"
        args: list = []
        if query:
            sql += " AND query = ?"
            args.append(query)
        if project_id is not None:
            sql += " AND query IN (SELECT query FROM project_queries WHERE project_id = ?)"
            args.append(project_id)
        if source:
            sql += " AND source = ?"
            args.append(source)
        if sentiment:
            sql += " AND sentiment = ?"
            args.append(sentiment.value)
        sql += " ORDER BY created_at DESC"
        if limit is not None:
            sql += " LIMIT ?"
            args.append(limit)
        with closing(self._conn.cursor()) as cur:
            cur.execute(sql, args)
            return [_row_to_mention(r) for r in cur.fetchall()]

    def mention_page(
        self, *, query: str | None = None, source: str | None = None,
        since: datetime | None = None, after: tuple[str, str, str] | None = None,
        limit: int = 200,
    ) -> list[Mention]:
        """Read a bounded page in immutable first-ingestion order.

        Query is the third cursor component because (id, query) is the primary
        key: one item matching two terms can share both fetched_at and id.
        """
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        sql = "SELECT * FROM mentions WHERE 1=1"
        args: list = []
        if query is not None:
            sql += " AND query=?"
            args.append(query)
        if source is not None:
            sql += " AND source=?"
            args.append(source)
        if since is not None:
            if since.tzinfo is None:
                raise ValueError("since must include a timezone")
            sql += " AND fetched_at>=?"
            args.append(since.astimezone(timezone.utc).isoformat())
        if after is not None:
            sql += " AND (fetched_at,id,query)>(?,?,?)"
            args.extend(after)
        sql += " ORDER BY fetched_at,id,query LIMIT ?"
        args.append(limit)
        with closing(self._conn.cursor()) as cur:
            return [_row_to_mention(row) for row in cur.execute(sql, args)]

    def queries(self, project_id: int | None = None) -> list[str]:
        with closing(self._conn.cursor()) as cur:
            sql = """
                SELECT t.query
                FROM tracked_queries AS t
                LEFT JOIN mentions AS m ON m.query = t.query
                """
            args: list = []
            if project_id is not None:
                sql += " JOIN project_queries AS pq ON pq.query = t.query AND pq.project_id = ?"
                args.append(project_id)
            sql += """
                GROUP BY t.query
                ORDER BY COALESCE(MAX(m.created_at), t.updated_at) DESC,
                         t.query COLLATE NOCASE
                """
            cur.execute(sql, args)
            return [r["query"] for r in cur.fetchall()]

    def summary(self, query: str | None = None, project_id: int | None = None) -> dict:
        where, args = _scope_filter(query, project_id)
        with closing(self._conn.cursor()) as cur:
            cur.execute(f"SELECT COUNT(*) AS n FROM mentions{where}", args)
            total = cur.fetchone()["n"]
            cur.execute(
                f"SELECT COALESCE(sentiment, 'neutral') AS label, COUNT(*) AS n "
                f"FROM mentions{where} GROUP BY label ORDER BY n DESC, label",
                args,
            )
            by_sentiment = {row["label"]: row["n"] for row in cur.fetchall()}
            cur.execute(
                f"SELECT source, COUNT(*) AS n FROM mentions{where} "
                "GROUP BY source ORDER BY n DESC, source",
                args,
            )
            by_source = {row["source"]: row["n"] for row in cur.fetchall()}
            cur.execute(
                f"SELECT substr(created_at, 1, 10) AS day, COUNT(*) AS n "
                f"FROM mentions{where} GROUP BY day ORDER BY day",
                args,
            )
            by_day = {row["day"]: row["n"] for row in cur.fetchall()}
        return {
            "total": total,
            "by_sentiment": by_sentiment,
            "by_source": by_source,
            "by_day": by_day,
        }

    def timeseries(self, query: str | None = None, project_id: int | None = None) -> list[dict]:
        """Per-day sentiment breakdown, oldest first, for the trend chart.

        Each entry: ``{"date": "YYYY-MM-DD", "positive": n, "neutral": n,
        "negative": n, "total": n}``.
        """
        where, args = _scope_filter(query, project_id)
        with closing(self._conn.cursor()) as cur:
            cur.execute(
                f"""
                SELECT substr(created_at, 1, 10) AS date,
                       COALESCE(SUM(sentiment = 'positive'), 0) AS positive,
                       COALESCE(SUM(sentiment = 'negative'), 0) AS negative,
                       COALESCE(SUM(sentiment = 'neutral' OR sentiment IS NULL), 0) AS neutral,
                       COUNT(*) AS total
                FROM mentions{where}
                GROUP BY date
                ORDER BY date
                """,
                args,
            )
            return [dict(row) for row in cur.fetchall()]

    def themes(
        self,
        query: str | None = None,
        project_id: int | None = None,
        limit: int = 8,
    ) -> list[dict]:
        """Theme counts over the complete result set, not only the visible feed page."""
        if limit < 1:
            raise ValueError("limit must be at least 1")
        where, args = _scope_filter(query, project_id)
        condition = "theme IS NOT NULL AND theme != ''"
        sql = f"SELECT theme AS label, COUNT(*) AS count FROM mentions{where}"
        sql += (" AND " if where else " WHERE ") + condition
        sql += " GROUP BY theme ORDER BY count DESC, theme LIMIT ?"
        with closing(self._conn.cursor()) as cur:
            cur.execute(sql, [*args, limit])
            return [dict(row) for row in cur.fetchall()]

    def net_sentiment(self, query: str | None = None, project_id: int | None = None) -> float:
        """Net sentiment in [-1, 1]: (positive - negative) / total."""
        s = self.summary(query=query, project_id=project_id)
        bs = s["by_sentiment"]
        pos, neg = bs.get("positive", 0), bs.get("negative", 0)
        total = s["total"] or 1
        return round((pos - neg) / total, 3)


def _scope_filter(query: str | None, project_id: int | None) -> tuple[str, list]:
    if query and project_id is not None:
        raise ValueError("query and project_id are mutually exclusive")
    if query:
        return " WHERE query = ?", [query]
    if project_id is not None:
        return (
            " WHERE query IN (SELECT query FROM project_queries WHERE project_id = ?)",
            [project_id],
        )
    return "", []


def _before_filter(
    cutoff: datetime, query: str | None, table: str | None = None
) -> tuple[str, list[str]]:
    prefix = f"{table}." if table else ""
    where = f" WHERE datetime({prefix}created_at) < datetime(?)"
    args = [cutoff.isoformat()]
    if query:
        where += f" AND {prefix}query = ?"
        args.append(query)
    return where, args


def _row_to_mention(r: sqlite3.Row) -> Mention:
    return Mention(
        id=r["id"],
        source=r["source"],
        query=r["query"],
        author=r["author"],
        title=r["title"],
        text=r["text"] or "",
        url=r["url"],
        created_at=datetime.fromisoformat(r["created_at"]),
        published_at=datetime.fromisoformat(r["published_at"]) if r["published_at"] else None,
        publication_provenance=r["publication_provenance"],
        source_updated_at=(datetime.fromisoformat(r["source_updated_at"])
                           if r["source_updated_at"] else None),
        fetched_at=datetime.fromisoformat(r["fetched_at"]),
        body_expired=bool(r["body_expired"]),
        source_id=r["source_id"],
        source_item_id=r["source_item_id"],
        author_id=r["author_id"],
        indexed_at=datetime.fromisoformat(r["indexed_at"]) if r["indexed_at"] else None,
        score=r["score"],
        sentiment=Sentiment(r["sentiment"]) if r["sentiment"] else None,
        sentiment_score=r["sentiment_score"],
        theme=r["theme"],
    )


def _user_view(row: sqlite3.Row) -> dict:
    return {
        "id": row["id"],
        "username": row["username"],
        "role": row["role"],
        "active": bool(row["active"]),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "last_login_at": row["last_login_at"],
    }


def _latest_timestamp(left: str | None, right: str | None) -> str | None:
    values = [value for value in (left, right) if value]
    return max(values, key=datetime.fromisoformat) if values else None


def _earliest_timestamp(left: str | None, right: str | None) -> str | None:
    values = [value for value in (left, right) if value]
    return min(values, key=datetime.fromisoformat) if values else None


def _net_sentiment(sentiments: list[str | None]) -> float | None:
    if not sentiments:
        return None
    positive = sum(value == Sentiment.POSITIVE.value for value in sentiments)
    negative = sum(value == Sentiment.NEGATIVE.value for value in sentiments)
    return round((positive - negative) / len(sentiments), 3)
