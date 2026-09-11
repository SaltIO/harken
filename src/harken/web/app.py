"""FastAPI app serving the dashboard + a small JSON API over the store."""

from __future__ import annotations

import hashlib
import math
import secrets
import time
from base64 import b64decode
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from fastapi import FastAPI, HTTPException, Query
from fastapi import Path as PathParam
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field
from starlette.requests import Request

from harken import __version__
from harken.auth import (
    DUMMY_PASSWORD_HASH,
    MUTATING_ROLES,
    new_session_token,
    session_token_hash,
    verify_password,
)
from harken.config import Config
from harken.models import Mention, Sentiment
from harken.observability import configure_logging
from harken.pipeline import Pipeline
from harken.sources import REGISTRY
from harken.store import CursorError, Store

_HERE = Path(__file__).parent
_SESSION_COOKIE = "harken_session"
# Largest value SQLite binds as an INTEGER; anything larger overflows the bind
# and raises, so reject oversized ids at the query-param boundary (422) rather
# than let them reach the store and surface as an unhandled 500.
_MAX_ID = 2**63 - 1

# Per-source display metadata: a short glyph badge + accent colour, kept offline
# (no icon fonts / CDNs). Matches the product's "no telemetry" premise.
SOURCE_META = {
    "hackernews": {"label": "Hacker News", "glyph": "Y", "color": "#ff6a3d"},
    "reddit": {"label": "Reddit", "glyph": "r/", "color": "#ff4f3f"},
    "mastodon": {"label": "Mastodon", "glyph": "@", "color": "#7c7fff"},
    "bluesky": {"label": "Bluesky", "glyph": "◈", "color": "#3aa8ff"},
    "rss": {"label": "RSS", "glyph": "∿", "color": "#e0a23a"},
    "stackoverflow": {"label": "Stack Overflow", "glyph": "<>", "color": "#f48024"},
    "x": {"label": "X / Twitter", "glyph": "X", "color": "#d8dce5"},
    "youtube": {"label": "YouTube", "glyph": "▶", "color": "#ff3d3d"},
}

_MONTH_NAMES = (
    "",
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
)


def _date_range_label(start: date, end: date) -> str:
    """Format a compact, unambiguous date or date range."""
    start_label = f"{_MONTH_NAMES[start.month]} {start.day}"
    if start == end:
        return f"{start_label}, {start.year}"
    if start.year != end.year:
        return f"{start_label}, {start.year} – {_MONTH_NAMES[end.month]} {end.day}, {end.year}"
    if start.month == end.month:
        return f"{start_label}–{end.day}, {start.year}"
    return f"{start_label} – {_MONTH_NAMES[end.month]} {end.day}, {start.year}"


def _chart_series(rows: list[dict], max_points: int = 48, max_ticks: int = 5) -> dict:
    """Build an evenly spaced, bounded series for the dashboard chart.

    Store timeseries contain active dates only. The chart needs empty calendar
    periods too, otherwise adjacent bars can represent very different amounts
    of time. Long histories are aggregated into equal-duration buckets so the
    DOM and labels stay readable.
    """
    if not rows:
        return {
            "points": [],
            "range_label": "No data yet",
            "cadence_label": "",
            "aria_label": "No mention history is available.",
        }
    if max_points < 1 or max_ticks < 1:
        raise ValueError("max_points and max_ticks must be positive")

    dated_rows = sorted((date.fromisoformat(row["date"]), row) for row in rows)
    first, last = dated_rows[0][0], dated_rows[-1][0]
    day_count = (last - first).days + 1
    bucket_days = max(1, math.ceil(day_count / max_points))
    point_count = math.ceil(day_count / bucket_days)
    points = []
    for index in range(point_count):
        bucket_start = first + timedelta(days=index * bucket_days)
        bucket_end = min(bucket_start + timedelta(days=bucket_days - 1), last)
        points.append(
            {
                "date": bucket_start.isoformat(),
                "end_date": bucket_end.isoformat(),
                "period_label": _date_range_label(bucket_start, bucket_end),
                "positive": 0,
                "neutral": 0,
                "negative": 0,
                "total": 0,
            }
        )

    for row_date, row in dated_rows:
        point = points[(row_date - first).days // bucket_days]
        for key in ("positive", "neutral", "negative", "total"):
            point[key] += int(row.get(key) or 0)

    visible_tick_count = min(max_ticks, point_count)
    if visible_tick_count == 1:
        tick_indices = {0}
    else:
        tick_indices = {
            round(index * (point_count - 1) / (visible_tick_count - 1))
            for index in range(visible_tick_count)
        }
    include_tick_year = first.year != last.year or day_count > 330
    for index, point in enumerate(points):
        point_date = date.fromisoformat(point["date"])
        label_date = last if index == point_count - 1 else point_date
        tick_label = f"{_MONTH_NAMES[label_date.month]} {label_date.day}"
        if include_tick_year:
            tick_label += f" ’{str(label_date.year)[2:]}"
        point.update(
            {
                "show_tick": index in tick_indices,
                "first_tick": index == min(tick_indices),
                "last_tick": index == max(tick_indices),
                "tick_label": tick_label,
            }
        )

    range_label = _date_range_label(first, last)
    cadence_label = "Daily" if bucket_days == 1 else f"{bucket_days}-day buckets"
    cadence_aria = "daily" if bucket_days == 1 else f"in {bucket_days}-day buckets"
    return {
        "points": points,
        "range_label": range_label,
        "cadence_label": cadence_label,
        "aria_label": (f"Mention volume and sentiment from {range_label}, shown {cadence_aria}."),
    }


class ScanRequest(BaseModel):
    query: str = Field(min_length=1, max_length=200)
    sources: list[str] = Field(min_length=1, max_length=20)
    mode: str = "incremental"
    pages: int = Field(default=3, ge=1, le=20)
    project_id: int | None = Field(default=None, ge=1, le=_MAX_ID)


class ProjectRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)


class ProjectQueryRequest(BaseModel):
    query: str = Field(min_length=1, max_length=200)


def create_app(
    db_path: str = "harken.db",
    auth_username: str | None = None,
    auth_password: str | None = None,
    config: Config | None = None,
) -> FastAPI:
    app = FastAPI(title="Harken", docs_url=None, redoc_url=None)
    templates = Jinja2Templates(directory=str(_HERE / "templates"))
    templates.env.filters["reltime"] = _reltime
    templates.env.globals["asset_version"] = _asset_version()
    app.mount("/static", StaticFiles(directory=str(_HERE / "static")), name="static")
    runtime_config = replace(config or Config(), db_path=db_path)
    basic_username = auth_username or runtime_config.auth_username
    basic_password = auth_password or runtime_config.auth_password
    if bool(basic_username) != bool(basic_password):
        raise ValueError("HARKEN_AUTH_USERNAME and HARKEN_AUTH_PASSWORD must be set together")
    auth_mode = "basic" if basic_username and basic_password else runtime_config.auth_mode
    if auth_mode == "basic" and not (basic_username and basic_password):
        raise ValueError("basic auth mode requires a username and password")
    configure_logging(runtime_config.log_format, runtime_config.log_level)
    csrf_token = secrets.token_urlsafe(32)
    login_failures: dict[tuple[str, str], list[float]] = {}

    def store() -> Store:
        return Store(db_path)

    @app.exception_handler(CursorError)
    async def cursor_error(request: Request, exc: CursorError):
        status = 400 if exc.code == "invalid_cursor" else 410 if exc.code == "cursor_expired" else 409
        return JSONResponse({"schema_version": "harken.observations.v1",
                             "code": exc.code, "detail": str(exc)}, status_code=status)

    @app.get("/api/observations/changes")
    def observation_changes(
        cursor: str | None = None, source: str | None = None, query: str | None = None,
        profile_id: str | None = None, profile_version: str | None = None,
        limit: int = Query(200, ge=1, le=1000),
    ):
        if (profile_id is None) != (profile_version is None):
            raise HTTPException(422, detail="Supply profile_id and profile_version together")
        with store() as db:
            return db.observation_changes(cursor=cursor, source=source, query=query,
                                          profile_id=profile_id, profile_version=profile_version,
                                          limit=limit)

    @app.get("/api/observations/{item_id}")
    def observation_detail(item_id: str):
        with store() as db:
            value = db.observation(item_id)
            if value is None:
                return JSONResponse({"schema_version": "harken.observations.v1",
                                     "code": "observation_not_found", "detail": "Identity is not retained."},
                                    status_code=404)
            return {"schema_version": "harken.observations.v1", "store_epoch": db.store_epoch,
                    "observation": value}

    @app.get("/api/coverage")
    def coverage(
        cursor: str | None = None, source: str | None = None, query: str | None = None,
        profile_id: str | None = None, profile_version: str | None = None,
        limit: int = Query(200, ge=1, le=1000),
    ):
        if (profile_id is None) != (profile_version is None):
            raise HTTPException(422, detail="Supply profile_id and profile_version together")
        with store() as db:
            return db.coverage_page(cursor=cursor, source=source, query=query,
                                    profile_id=profile_id, profile_version=profile_version, limit=limit)

    def _csrf_valid(supplied: str) -> bool:
        # Compare as bytes so a non-ASCII submission returns False instead of
        # raising TypeError (secrets.compare_digest rejects non-ASCII str),
        # which would otherwise surface as an unhandled HTTP 500.
        return secrets.compare_digest(supplied.encode("utf-8"), csrf_token.encode("utf-8"))

    def require_csrf(request: Request) -> None:
        if not _csrf_valid(request.headers.get("x-harken-csrf", "")):
            raise HTTPException(status_code=403, detail="Invalid CSRF token")

    def require_operator(request: Request) -> None:
        if (
            auth_mode == "accounts"
            and getattr(request.state, "user", {}).get("role") not in MUTATING_ROLES
        ):
            raise HTTPException(status_code=403, detail="Operator role required")

    @app.middleware("http")
    async def security_headers(request: Request, call_next):
        request.state.user = None
        path = request.url.path
        if runtime_config.read_only and request.method not in {"GET", "HEAD"}:
            response = JSONResponse(
                {"detail": "Read-only API"}, status_code=405, headers={"Allow": "GET, HEAD"}
            )
        elif (
            auth_mode == "basic"
            and path != "/health"
            and not _valid_basic_auth(
                request.headers.get("authorization"), basic_username or "", basic_password or ""
            )
        ):
            response = JSONResponse(
                {"detail": "Authentication required"},
                status_code=401,
                headers={"WWW-Authenticate": 'Basic realm="Harken", charset="UTF-8"'},
            )
        elif auth_mode == "accounts" and not _account_public_path(path):
            raw_token = request.cookies.get(_SESSION_COOKIE, "")
            account = None
            if raw_token:
                db = store()
                try:
                    account = db.session_user(session_token_hash(raw_token))
                finally:
                    db.close()
            if account is None:
                if request.method == "GET" and not path.startswith("/api/") and path != "/metrics":
                    response = RedirectResponse("/login", status_code=303)
                else:
                    response = JSONResponse({"detail": "Authentication required"}, status_code=401)
            else:
                request.state.user = account
                response = await call_next(request)
        else:
            response = await call_next(request)
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; img-src 'self' data:; style-src 'self' 'unsafe-inline'; "
            "font-src 'self'; script-src 'self'; connect-src 'self'; base-uri 'none'; "
            "frame-ancestors 'none'; form-action 'self'"
        )
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        if auth_mode != "none":
            response.headers["Cache-Control"] = "no-store"
        return response

    @app.get("/login", response_class=HTMLResponse)
    def login_page(request: Request):
        if auth_mode != "accounts":
            return RedirectResponse("/", status_code=303)
        return templates.TemplateResponse(
            request,
            "login.html",
            {"csrf_token": csrf_token, "error": None, "no_users": _user_count(store) == 0},
        )

    @app.post("/login")
    async def login(request: Request):
        if auth_mode != "accounts":
            raise HTTPException(status_code=404, detail="Not found")
        raw_body = await request.body()
        if len(raw_body) > 4096:
            raise HTTPException(status_code=413, detail="Login request is too large")
        form = parse_qs(raw_body.decode("utf-8", errors="replace"), keep_blank_values=True)
        username = (form.get("username") or [""])[0].strip()
        password = (form.get("password") or [""])[0]
        supplied_csrf = (form.get("csrf") or [""])[0]
        # The CSRF token is served on the public /login page, so it cannot by
        # itself stop a cross-site login POST (login CSRF / session fixation).
        # An Origin/Referer check that does not depend on token secrecy closes
        # that gap; the token check still guards same-origin form integrity.
        if not _same_origin(request):
            raise HTTPException(status_code=403, detail="Cross-origin request rejected")
        if not _csrf_valid(supplied_csrf):
            raise HTTPException(status_code=403, detail="Invalid CSRF token")

        client_host = request.client.host if request.client else "unknown"
        failure_key = (client_host, f"user:{username.casefold()}")
        client_failure_key = (client_host, "client:*")
        now_monotonic = time.monotonic()
        for key, failures in list(login_failures.items()):
            recent = [seen for seen in failures if now_monotonic - seen < 300]
            if recent:
                login_failures[key] = recent
            else:
                login_failures.pop(key, None)
        recent_failures = login_failures.get(failure_key, [])
        client_failures = login_failures.get(client_failure_key, [])
        if len(recent_failures) >= 5 or len(client_failures) >= 20:
            return templates.TemplateResponse(
                request,
                "login.html",
                {
                    "csrf_token": csrf_token,
                    "error": "Too many attempts. Try again later.",
                    "no_users": False,
                },
                status_code=429,
                headers={"Retry-After": "300"},
            )

        db = store()
        try:
            account = db.user_for_login(username)
            password_hash = (
                account["password_hash"] if account and account["active"] else DUMMY_PASSWORD_HASH
            )
            authenticated = verify_password(password, password_hash)
            if not account or not account["active"] or not authenticated:
                recent_failures.append(now_monotonic)
                login_failures[failure_key] = recent_failures
                client_failures.append(now_monotonic)
                login_failures[client_failure_key] = client_failures
                return templates.TemplateResponse(
                    request,
                    "login.html",
                    {
                        "csrf_token": csrf_token,
                        "error": "Invalid username or password.",
                        "no_users": False,
                    },
                    status_code=401,
                )
            login_failures.pop(failure_key, None)
            token = new_session_token()
            expires = datetime.now(timezone.utc) + timedelta(hours=runtime_config.session_hours)
            db.create_session(account["id"], session_token_hash(token), expires)
        finally:
            db.close()

        response = RedirectResponse("/", status_code=303)
        # Mark the cookie Secure whenever the request is actually served over
        # HTTPS (directly or via a TLS-terminating proxy), so the token is not
        # exposed on the wire even if the operator forgets HARKEN_SESSION_SECURE.
        # The config flag stays an explicit override; the default stays off so a
        # plain-HTTP localhost deployment still receives a usable cookie.
        response.set_cookie(
            _SESSION_COOKIE,
            token,
            max_age=runtime_config.session_hours * 3600,
            httponly=True,
            secure=runtime_config.session_secure or _is_https(request),
            samesite="strict",
            path="/",
        )
        return response

    @app.post("/logout")
    async def logout(request: Request):
        if auth_mode != "accounts":
            raise HTTPException(status_code=404, detail="Not found")
        raw_body = await request.body()
        form = parse_qs(raw_body.decode("utf-8", errors="replace"), keep_blank_values=True)
        supplied_csrf = (form.get("csrf") or [""])[0]
        if not _same_origin(request):
            raise HTTPException(status_code=403, detail="Cross-origin request rejected")
        if not _csrf_valid(supplied_csrf):
            raise HTTPException(status_code=403, detail="Invalid CSRF token")
        raw_token = request.cookies.get(_SESSION_COOKIE, "")
        if raw_token:
            db = store()
            try:
                db.delete_session(session_token_hash(raw_token))
            finally:
                db.close()
        response = RedirectResponse("/login", status_code=303)
        response.delete_cookie(_SESSION_COOKIE, path="/", samesite="strict")
        return response

    @app.get("/health")
    def health():
        db = store()
        try:
            db.check()
            return {"status": "ok", "database": "ok"}
        finally:
            db.close()

    @app.get("/metrics", response_class=PlainTextResponse)
    def metrics():
        db = store()
        try:
            stats = db.operational_stats()
            source_stats = db.source_metrics()
        finally:
            db.close()
        lines = [
            "# HELP harken_mentions Stored mentions.",
            "# TYPE harken_mentions gauge",
            f"harken_mentions {stats['mentions']}",
            "# HELP harken_queries Stored tracked queries.",
            "# TYPE harken_queries gauge",
            f"harken_queries {stats['queries']}",
            "# HELP harken_alerts_pending Undelivered mention alerts across all transports.",
            "# TYPE harken_alerts_pending gauge",
            f"harken_alerts_pending {stats['alerts_pending']}",
            "# HELP harken_alerts_delivered Delivered mention alerts retained for deduplication.",
            "# TYPE harken_alerts_delivered gauge",
            f"harken_alerts_delivered {stats['alerts_delivered']}",
            "# HELP harken_threshold_alerts_pending Undelivered active threshold alerts.",
            "# TYPE harken_threshold_alerts_pending gauge",
            f"harken_threshold_alerts_pending {stats['threshold_alerts_pending']}",
            "# HELP harken_threshold_alerts_delivered Delivered threshold alert episodes.",
            "# TYPE harken_threshold_alerts_delivered counter",
            f"harken_threshold_alerts_delivered {stats['threshold_alerts_delivered']}",
        ]
        source_metrics = [
            (
                "harken_source_scans_total",
                "Completed source scans.",
                "counter",
                "scans_total",
            ),
            (
                "harken_source_errors_total",
                "Failed source scans.",
                "counter",
                "errors_total",
            ),
            (
                "harken_source_mentions_fetched_total",
                "Mentions fetched before de-duplication.",
                "counter",
                "fetched_total",
            ),
            (
                "harken_source_pages_fetched_total",
                "Source result pages fetched.",
                "counter",
                "pages_total",
            ),
            (
                "harken_source_retries_total",
                "Retried source requests.",
                "counter",
                "retries_total",
            ),
            (
                "harken_source_scan_duration_seconds_total",
                "Cumulative source scan duration in seconds.",
                "counter",
                "duration_seconds_total",
            ),
            (
                "harken_source_last_scan_duration_seconds",
                "Most recent source scan duration in seconds.",
                "gauge",
                "last_duration_seconds",
            ),
            (
                "harken_source_last_scan_success",
                "Whether the most recent source scan succeeded (1 or 0).",
                "gauge",
                "last_success",
            ),
        ]
        for metric_name, help_text, metric_type, key in source_metrics:
            lines.extend(
                [f"# HELP {metric_name} {help_text}", f"# TYPE {metric_name} {metric_type}"]
            )
            lines.extend(
                f'{metric_name}{{source="{_prometheus_label(row["source"])}"}} '
                f"{_metric_number(row[key])}"
                for row in source_stats
            )
        return "\n".join(lines) + "\n"

    @app.get("/", response_class=HTMLResponse)
    def dashboard(
        request: Request,
        q: str | None = None,
        p: int | None = Query(default=None, ge=1, le=_MAX_ID),
    ):
        db = store()
        try:
            projects = db.projects()
            all_queries = db.queries()
            project = db.project(p) if p is not None else None
            if p is not None and project is None:
                raise HTTPException(status_code=404, detail="Project not found")
            queries = project["queries"] if project else all_queries
            if project:
                query = q if q in queries else None
            else:
                query = q if q in queries else (queries[0] if queries else None)
            project_scope = project["id"] if project and query is None else None
            has_scope = bool(query or project_scope is not None)
            summary = (
                db.summary(query=query, project_id=project_scope)
                if has_scope
                else {"total": 0, "by_sentiment": {}, "by_source": {}, "by_day": {}}
            )
            mentions = (
                db.mentions(query=query, project_id=project_scope, limit=200) if has_scope else []
            )
            timeseries = db.timeseries(query=query, project_id=project_scope) if has_scope else []
            chart = _chart_series(timeseries)
            themes = db.themes(query=query, project_id=project_scope) if has_scope else []
            tracking = db.tracking(query) if query else None
            tracking_sources = (
                tracking["sources"] if tracking and tracking["sources"] else runtime_config.sources
            )
            bs = summary["by_sentiment"]
            ctx = {
                "queries": queries,
                "all_queries": all_queries,
                "query": query,
                "project": project,
                "projects": projects,
                "project_scope": project_scope,
                "unassigned_queries": [value for value in all_queries if value not in queries],
                "summary": summary,
                "mentions": [_view(m) for m in mentions],
                "themes": themes,
                "timeseries": timeseries,
                "chart_series": chart["points"],
                "chart_range": chart["range_label"],
                "chart_cadence": chart["cadence_label"],
                "chart_aria": chart["aria_label"],
                "max_day": max((d["total"] for d in chart["points"]), default=0),
                "pos": bs.get("positive", 0),
                "neu": bs.get("neutral", 0),
                "neg": bs.get("negative", 0),
                "net": (
                    db.net_sentiment(query=query, project_id=project_scope) if has_scope else 0.0
                ),
                "top_theme": themes[0]["label"] if themes else None,
                "source_meta": SOURCE_META,
                "available_sources": [
                    {
                        "name": name,
                        "label": source_cls.label,
                        "needs_config": source_cls.needs_config,
                        "configured": _source_configured(name, runtime_config),
                    }
                    for name, source_cls in REGISTRY.items()
                ],
                "tracking_sources": tracking_sources,
                "source_states": {state["source"]: state for state in db.source_states(query)}
                if query
                else {},
                "csrf_token": csrf_token,
                "current_user": request.state.user,
                "can_mutate": auth_mode != "accounts"
                or request.state.user["role"] in MUTATING_ROLES,
                "auth_mode": auth_mode,
            }
            return templates.TemplateResponse(request, "dashboard.html", ctx)
        finally:
            db.close()

    @app.get("/api/mentions/page")
    def api_mention_page(
        q: str | None = None,
        source: str | None = None,
        since: datetime | None = None,
        after_fetched_at: datetime | None = None,
        after_id: str | None = None,
        after_query: str | None = None,
        limit: int = Query(200, ge=1, le=1000),
    ):
        cursor_parts = (after_fetched_at, after_id, after_query)
        if any(part is not None for part in cursor_parts) and not all(
            part is not None for part in cursor_parts
        ):
            raise HTTPException(status_code=422, detail="Supply every after cursor field")
        if any(value is not None and value.tzinfo is None
               for value in (since, after_fetched_at)):
            raise HTTPException(status_code=422, detail="Cursor dates must include a timezone")
        after = (
            (after_fetched_at.astimezone(timezone.utc).isoformat(), after_id, after_query)
            if after_fetched_at is not None else None
        )
        with store() as db:
            rows = db.mention_page(query=q, source=source, since=since, after=after, limit=limit)
        next_cursor = None
        if len(rows) == limit:
            last = rows[-1]
            next_cursor = {"after_fetched_at": last.fetched_at.isoformat(),
                           "after_id": last.id, "after_query": last.query}
        return JSONResponse({"rows": [_view(m) for m in rows], "next_cursor": next_cursor})

    @app.get("/api/mentions")
    def api_mentions(
        q: str | None = None,
        p: int | None = Query(default=None, ge=1, le=_MAX_ID),
        source: str | None = None,
        sentiment: Sentiment | None = None,
        limit: int = Query(200, ge=1, le=1000),
    ):
        db = store()
        try:
            if q and p is not None:
                raise HTTPException(status_code=422, detail="Choose a query or a project, not both")
            if p is not None and db.project(p) is None:
                raise HTTPException(status_code=404, detail="Project not found")
            rows = db.mentions(
                query=q,
                project_id=p,
                source=source,
                sentiment=sentiment,
                limit=limit,
            )
            return JSONResponse([_view(m) for m in rows])
        finally:
            db.close()

    @app.get("/api/summary")
    def api_summary(q: str | None = None, p: int | None = Query(default=None, ge=1, le=_MAX_ID)):
        db = store()
        try:
            if q and p is not None:
                raise HTTPException(status_code=422, detail="Choose a query or a project, not both")
            if p is not None and db.project(p) is None:
                raise HTTPException(status_code=404, detail="Project not found")
            return JSONResponse(
                {
                    "summary": db.summary(query=q, project_id=p),
                    "timeseries": db.timeseries(query=q, project_id=p),
                    "net": db.net_sentiment(query=q, project_id=p),
                }
            )
        finally:
            db.close()

    @app.post("/api/track")
    def api_track(payload: ScanRequest, request: Request):
        require_operator(request)
        require_csrf(request)
        query = payload.query.strip()
        if not query:
            raise HTTPException(status_code=422, detail="Query must not be empty")
        source_names = list(
            dict.fromkeys(name.strip().lower() for name in payload.sources if name.strip())
        )
        if not source_names:
            raise HTTPException(status_code=422, detail="Select at least one source")
        unknown = [name for name in source_names if name not in REGISTRY]
        if unknown:
            raise HTTPException(status_code=422, detail=f"Unknown source(s): {', '.join(unknown)}")
        if payload.mode not in {"incremental", "backfill"}:
            raise HTTPException(status_code=422, detail="Mode must be incremental or backfill")

        if payload.project_id is not None:
            db = store()
            try:
                if db.project(payload.project_id) is None:
                    raise HTTPException(status_code=404, detail="Project not found")
            finally:
                db.close()

        scan_config = replace(runtime_config, sources=source_names)
        pipeline = Pipeline(scan_config)
        try:
            result = pipeline.track(
                query,
                backfill=payload.mode == "backfill",
                pages=payload.pages,
                project_id=payload.project_id,
            )
        finally:
            pipeline.close()
        return {
            "query": result.query,
            "project_id": result.project_id,
            "mode": result.mode,
            "fetched": result.fetched,
            "new": result.new,
            "by_source": result.by_source,
            "pages_by_source": result.pages_by_source,
            "backfill_complete": result.backfill_complete,
            "errors": result.errors,
            "sentiment_error": result.sentiment_error,
            "analysis_error": result.analysis_error,
            "alerted": result.alerted,
            "alert_pending": result.alert_pending,
            "alert_error": result.alert_error,
            "threshold_alerted": result.threshold_alerted,
            "threshold_pending": result.threshold_pending,
            "threshold_events": result.threshold_events,
        }

    @app.post("/api/projects", status_code=201)
    def api_create_project(payload: ProjectRequest, request: Request):
        require_operator(request)
        require_csrf(request)
        db = store()
        try:
            try:
                return db.create_project(payload.name)
            except ValueError as exc:
                raise HTTPException(status_code=409, detail=str(exc)) from exc
        finally:
            db.close()

    @app.post("/api/projects/{project_id}/queries")
    def api_add_project_query(
        payload: ProjectQueryRequest,
        request: Request,
        project_id: int = PathParam(ge=1, le=_MAX_ID),
    ):
        require_operator(request)
        require_csrf(request)
        db = store()
        try:
            try:
                added = db.add_query_to_project(project_id, payload.query)
            except ValueError as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from exc
            return {"added": added, "project": db.project(project_id)}
        finally:
            db.close()

    @app.delete("/api/projects/{project_id}/queries")
    def api_remove_project_query(
        payload: ProjectQueryRequest,
        request: Request,
        project_id: int = PathParam(ge=1, le=_MAX_ID),
    ):
        require_operator(request)
        require_csrf(request)
        db = store()
        try:
            try:
                removed = db.remove_query_from_project(project_id, payload.query)
            except ValueError as exc:
                raise HTTPException(status_code=409, detail=str(exc)) from exc
            if not removed:
                raise HTTPException(status_code=404, detail="Keyword is not in this project")
            return {"removed": True, "project": db.project(project_id)}
        finally:
            db.close()

    @app.delete("/api/projects/{project_id}")
    def api_delete_project(request: Request, project_id: int = PathParam(ge=1, le=_MAX_ID)):
        require_operator(request)
        require_csrf(request)
        db = store()
        try:
            try:
                deleted = db.delete_project(project_id)
            except ValueError as exc:
                raise HTTPException(status_code=409, detail=str(exc)) from exc
            if not deleted:
                raise HTTPException(status_code=404, detail="Project not found")
            return {"deleted": True}
        finally:
            db.close()

    return app


def _asset_version() -> str:
    """Content-address static URLs so upgrades cannot reuse stale CSS or JS."""
    digest = hashlib.sha256()
    for name in ("style.css", "app.js"):
        digest.update((_HERE / "static" / name).read_bytes())
    return f"{__version__}-{digest.hexdigest()[:10]}"


def _account_public_path(path: str) -> bool:
    return path in {"/health", "/login"} or path.startswith("/static/")


def _user_count(store_factory) -> int:
    db = store_factory()
    try:
        return len(db.users())
    finally:
        db.close()


def _source_configured(name: str, config: Config) -> bool:
    if name == "reddit":
        return bool(
            config.reddit_access_token or (config.reddit_client_id and config.reddit_client_secret)
        )
    if name == "mastodon":
        return bool(config.mastodon_access_token)
    if name == "rss":
        return bool(config.rss_feeds)
    if name == "x":
        return bool(config.x_bearer_token)
    if name == "youtube":
        return bool(config.youtube_api_key)
    return True


def _is_https(request: Request) -> bool:
    """True when the client-facing connection is HTTPS, trusting the standard
    X-Forwarded-Proto set by a TLS-terminating reverse proxy. Spoofing it can
    only make a cookie *more* restrictive (Secure), so trusting it is safe."""
    forwarded = request.headers.get("x-forwarded-proto", "").split(",")[0].strip().lower()
    return request.url.scheme == "https" or forwarded == "https"


def _same_origin(request: Request) -> bool:
    """Reject cross-site state-changing POSTs. Absent both Origin and Referer
    (non-browser clients: the CLI, curl, tests) the request is allowed; a
    browser always sends Origin on a cross-site POST, so genuine CSRF is
    caught while same-origin form submissions pass.

    The submitted Origin is matched against both Host and X-Forwarded-Host, so a
    TLS-terminating reverse proxy that rewrites Host to the internal upstream
    (but forwards the original as X-Forwarded-Host) does not 403 real logins.
    Trusting X-Forwarded-Host is safe here: the check only passes when it equals
    the browser-supplied Origin, which an attacker's cross-site form cannot forge.
    """
    source = request.headers.get("origin") or request.headers.get("referer")
    if not source:
        return True
    try:
        netloc = urlsplit(source).netloc
    except ValueError:
        return False
    if not netloc:
        return False
    allowed = {request.headers.get("host")}
    forwarded_host = request.headers.get("x-forwarded-host")
    if forwarded_host:
        allowed.add(forwarded_host.split(",")[0].strip())
    return netloc in allowed


def _valid_basic_auth(header: str | None, username: str, password: str) -> bool:
    if not header or not header.lower().startswith("basic "):
        return False
    try:
        decoded = b64decode(header.split(" ", 1)[1], validate=True).decode("utf-8")
        supplied_username, supplied_password = decoded.split(":", 1)
    except (ValueError, UnicodeDecodeError):
        return False
    # Evaluate both comparisons unconditionally (assign before `and`) so a wrong
    # username does not skip the password check — that short-circuit would leak,
    # via response timing, whether the username matched. Compare as bytes to
    # avoid TypeError on non-ASCII credentials.
    username_ok = secrets.compare_digest(
        supplied_username.encode("utf-8"), username.encode("utf-8")
    )
    password_ok = secrets.compare_digest(
        supplied_password.encode("utf-8"), password.encode("utf-8")
    )
    return username_ok and password_ok


def _view(m: Mention) -> dict:
    meta = SOURCE_META.get(m.source, {"label": m.source, "glyph": "•", "color": "#8b93a1"})
    return {
        "id": m.id,
        "query": m.query,
        "source": m.source,
        "source_label": meta["label"],
        "source_glyph": meta["glyph"],
        "source_color": meta["color"],
        "author": m.author,
        "title": m.title,
        "text": m.text,
        "url": _safe_url(m.url),
        "source_id": m.source_id,
        "source_item_id": m.source_item_id,
        "author_id": m.author_id,
        "indexed_at": m.indexed_at.isoformat() if m.indexed_at else None,
        "created_at": m.created_at.isoformat(),
        "published_at": m.published_at.isoformat() if m.published_at else None,
        "publication_provenance": m.publication_provenance,
        "source_updated_at": m.source_updated_at.isoformat() if m.source_updated_at else None,
        "fetched_at": m.fetched_at.isoformat() if m.fetched_at else None,
        "body_expired": m.body_expired,
        "reltime": _reltime(m.created_at),
        "score": m.score,
        "sentiment": m.sentiment.value if m.sentiment else "neutral",
        "sentiment_score": m.sentiment_score,
        "theme": m.theme,
    }


def _safe_url(value: str | None) -> str | None:
    """Only render navigable web links; source payloads are untrusted input."""
    if not value:
        return None
    try:
        parsed = urlsplit(value)
    except ValueError:
        return None
    return value if parsed.scheme.lower() in {"http", "https"} and parsed.netloc else None


def _prometheus_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _metric_number(value: int | float) -> str:
    return str(value) if isinstance(value, int) else format(value, ".15g")


def _reltime(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta = datetime.now(timezone.utc) - dt
    secs = max(0, int(delta.total_seconds()))
    if secs < 60:
        return "now"
    mins = secs // 60
    if mins < 60:
        return f"{mins}m"
    hrs = mins // 60
    if hrs < 24:
        return f"{hrs}h"
    days = hrs // 24
    if days < 7:
        return f"{days}d"
    weeks = days // 7
    if weeks < 5:
        return f"{weeks}w"
    return f"{days // 30}mo"
