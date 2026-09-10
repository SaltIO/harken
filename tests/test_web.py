"""Web dashboard / JSON API tests — cover the surface the review found at 0% coverage."""

import re
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from harken import __version__
from harken.auth import hash_password
from harken.config import Config
from harken.models import Mention, Sentiment
from harken.store import Store
from harken.web.app import _chart_series, create_app


def mk(text, sentiment=Sentiment.NEUTRAL, source="hackernews", query="acme", url=None):
    return Mention(
        source=source,
        query=query,
        text=text,
        url=url,
        created_at=datetime(2026, 6, 1, tzinfo=timezone.utc),
        sentiment=sentiment,
    )


def seeded_db(tmp_path):
    db_path = tmp_path / "t.db"
    store = Store(db_path)
    store.upsert(
        [
            mk("great stuff", Sentiment.POSITIVE, url="u1"),
            mk("terrible bug", Sentiment.NEGATIVE, url="u2"),
            mk("it exists", Sentiment.NEUTRAL, url="u3"),
        ]
    )
    store.close()
    return str(db_path)


def test_read_only_api_rejects_every_mutating_method_before_pipeline(tmp_path, monkeypatch):
    monkeypatch.setenv("HARKEN_READ_ONLY", "true")
    calls = []

    def forbidden_pipeline(*args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("Read-only API must not construct a collector")

    monkeypatch.setattr("harken.web.app.Pipeline", forbidden_pipeline)
    client = TestClient(create_app(db_path=seeded_db(tmp_path)))
    assert client.get("/api/mentions/page").status_code == 200
    for method in ("POST", "PUT", "PATCH", "DELETE", "OPTIONS"):
        response = client.request(method, "/api/track", json={"query": "CMBS"})
        assert response.status_code == 405
        assert response.headers["Allow"] == "GET, HEAD"
    assert client.post("/api/projects", json={"name": "bypass"}).status_code == 405
    assert calls == []


def test_mention_page_stable_ties_filters_and_provenance(tmp_path):
    path = str(tmp_path / "pages.db")
    with Store(path) as db:
        db.upsert([mk("shared", query=q, source=s, url=f"https://{s}/1")
                   for s in ("rss", "hackernews") for q in ("CMBS", "EDGAR")])
    client = TestClient(create_app(path))
    params = {"limit": 1, "source": "rss"}
    collected = []
    while True:
        response = client.get("/api/mentions/page", params=params)
        assert response.status_code == 200, response.text
        page = response.json()
        collected.extend(page["rows"])
        if page["next_cursor"] is None:
            break
        params.update(page["next_cursor"])
    assert len(collected) == 2
    assert {r["query"] for r in collected} == {"CMBS", "EDGAR"}
    assert all(r["published_at"] is None and r["fetched_at"] for r in collected)
    for invalid in ({"after_id": "partial"}, {"since": "2026-01-01T00:00:00"},
                    {"limit": 1001}):
        assert client.get("/api/mentions/page", params=invalid).status_code == 422



def account_client(tmp_path, role="admin"):
    db_path = str(tmp_path / f"accounts-{role}.db")
    with Store(db_path) as store:
        store.create_user("admin", hash_password("admin password 123", iterations=100_000), "admin")
        username = "admin"
        password = "admin password 123"
        if role != "admin":
            username = role
            password = f"{role} password 123"
            store.create_user(
                username,
                hash_password(password, iterations=100_000),
                role,
            )
    cfg = Config(db_path=db_path, auth_mode="accounts", session_hours=1)
    client = TestClient(create_app(db_path, config=cfg), follow_redirects=False)
    login_page = client.get("/login")
    csrf = re.search(r'name="csrf" value="([^"]+)"', login_page.text).group(1)
    response = client.post(
        "/login", data={"username": username, "password": password, "csrf": csrf}
    )
    assert response.status_code == 303, response.text
    return client, db_path


def test_dashboard_renders_with_no_data(tmp_path):
    client = TestClient(create_app(str(tmp_path / "empty.db")))
    r = client.get("/")
    assert r.status_code == 200
    assert r.headers["x-frame-options"] == "DENY"
    assert "default-src 'self'" in r.headers["content-security-policy"]
    assert 'id="scan-form"' in r.text
    assert "scan latest" in r.text
    assert "X / Twitter" in r.text
    assert "YouTube" in r.text
    assert r.text.count("setup needed") >= 5


def test_account_mode_redirects_to_login_and_keeps_health_public(tmp_path):
    db_path = str(tmp_path / "account-gate.db")
    cfg = Config(db_path=db_path, auth_mode="accounts")
    client = TestClient(create_app(db_path, config=cfg), follow_redirects=False)
    assert client.get("/").status_code == 303
    assert client.get("/").headers["location"] == "/login"
    assert client.get("/api/summary").status_code == 401
    assert client.get("/metrics").status_code == 401
    assert client.get("/health").json() == {"status": "ok", "database": "ok"}
    login = client.get("/login")
    assert login.status_code == 200
    assert "No account exists yet" in login.text
    assert "no-store" in login.headers["cache-control"]


def test_account_login_sets_hardened_cookie_and_logout_revokes_it(tmp_path):
    client, _ = account_client(tmp_path)
    cookie = client.cookies.get("harken_session")
    assert cookie
    dashboard = client.get("/")
    assert dashboard.status_code == 200
    assert "admin" in dashboard.text
    assert "sign out" in dashboard.text
    csrf = re.search(r'name="csrf" value="([^"]+)"', dashboard.text).group(1)
    logged_out = client.post("/logout", data={"csrf": csrf})
    assert logged_out.status_code == 303
    assert client.get("/").status_code == 303


def test_account_cookie_can_be_marked_secure(tmp_path):
    db_path = str(tmp_path / "secure-cookie.db")
    with Store(db_path) as store:
        store.create_user("admin", hash_password("admin password 123", iterations=100_000), "admin")
    cfg = Config(db_path=db_path, auth_mode="accounts", session_secure=True)
    client = TestClient(create_app(db_path, config=cfg), follow_redirects=False)
    page = client.get("/login")
    csrf = re.search(r'name="csrf" value="([^"]+)"', page.text).group(1)
    response = client.post(
        "/login",
        data={"username": "admin", "password": "admin password 123", "csrf": csrf},
    )
    set_cookie = response.headers["set-cookie"].lower()
    assert "httponly" in set_cookie
    assert "samesite=strict" in set_cookie
    assert "secure" in set_cookie


def test_viewer_can_read_but_cannot_mutate(tmp_path):
    client, _ = account_client(tmp_path, "viewer")
    dashboard = client.get("/")
    assert dashboard.status_code == 200
    assert "viewer · read only" in dashboard.text
    assert "disabled" in dashboard.text
    assert client.get("/api/summary").status_code == 200
    csrf = re.search(r'data-csrf="([^"]+)"', dashboard.text).group(1)
    response = client.post(
        "/api/projects",
        json={"name": "Forbidden"},
        headers={"X-Harken-CSRF": csrf},
    )
    assert response.status_code == 403
    assert response.json()["detail"] == "Operator role required"


def test_operator_can_mutate_projects(tmp_path):
    client, _ = account_client(tmp_path, "operator")
    dashboard = client.get("/")
    csrf = re.search(r'data-csrf="([^"]+)"', dashboard.text).group(1)
    response = client.post(
        "/api/projects",
        json={"name": "Allowed"},
        headers={"X-Harken-CSRF": csrf},
    )
    assert response.status_code == 201
    assert response.json()["name"] == "Allowed"


def test_account_login_has_generic_errors_and_rate_limit(tmp_path):
    db_path = str(tmp_path / "rate-limit.db")
    with Store(db_path) as store:
        store.create_user("admin", hash_password("admin password 123", iterations=100_000), "admin")
    cfg = Config(db_path=db_path, auth_mode="accounts")
    client = TestClient(create_app(db_path, config=cfg), follow_redirects=False)
    page = client.get("/login")
    csrf = re.search(r'name="csrf" value="([^"]+)"', page.text).group(1)
    for _ in range(5):
        response = client.post(
            "/login", data={"username": "nobody", "password": "wrong", "csrf": csrf}
        )
        assert response.status_code == 401
        assert "Invalid username or password" in response.text
    limited = client.post("/login", data={"username": "nobody", "password": "wrong", "csrf": csrf})
    assert limited.status_code == 429
    assert limited.headers["retry-after"] == "300"


def test_keyed_sources_are_enabled_in_dashboard_when_configured(tmp_path):
    cfg = Config(
        db_path=str(tmp_path / "keys.db"),
        x_bearer_token="x-token",
        youtube_api_key="youtube-key",
    )
    text = TestClient(create_app(cfg.db_path, config=cfg)).get("/").text
    label = r'<label class="source-option[^\"]*">(?:(?!</label>).)*?value="{}"(?:(?!</label>).)*?</label>'
    x_option = re.search(label.format("x"), text, re.S)
    youtube_option = re.search(label.format("youtube"), text, re.S)
    assert x_option and "setup needed" not in x_option.group(0)
    assert youtube_option and "setup needed" not in youtube_option.group(0)


def test_dashboard_renders_with_data(tmp_path):
    client = TestClient(create_app(seeded_db(tmp_path)))
    r = client.get("/")
    assert r.status_code == 200
    assert "acme" in r.text
    assert f"/static/app.js?v={__version__}-" in r.text
    assert f"/static/style.css?v={__version__}-" in r.text


def test_chart_series_fills_calendar_gaps_and_limits_axis_ticks():
    chart = _chart_series(
        [
            {
                "date": "2026-06-01",
                "positive": 2,
                "neutral": 0,
                "negative": 0,
                "total": 2,
            },
            {
                "date": "2026-06-03",
                "positive": 0,
                "neutral": 0,
                "negative": 1,
                "total": 1,
            },
        ]
    )

    assert [point["date"] for point in chart["points"]] == [
        "2026-06-01",
        "2026-06-02",
        "2026-06-03",
    ]
    assert chart["points"][1]["total"] == 0
    assert chart["range_label"] == "Jun 1–3, 2026"
    assert chart["cadence_label"] == "Daily"
    assert sum(point["show_tick"] for point in chart["points"]) == 3


def test_chart_series_aggregates_long_histories_without_losing_counts():
    chart = _chart_series(
        [
            {
                "date": "2025-01-01",
                "positive": 1,
                "neutral": 0,
                "negative": 0,
                "total": 1,
            },
            {
                "date": "2025-07-04",
                "positive": 0,
                "neutral": 3,
                "negative": 0,
                "total": 3,
            },
            {
                "date": "2025-12-31",
                "positive": 0,
                "neutral": 0,
                "negative": 2,
                "total": 2,
            },
        ]
    )

    assert len(chart["points"]) <= 48
    assert sum(point["total"] for point in chart["points"]) == 6
    assert sum(point["positive"] for point in chart["points"]) == 1
    assert sum(point["neutral"] for point in chart["points"]) == 3
    assert sum(point["negative"] for point in chart["points"]) == 2
    assert sum(point["show_tick"] for point in chart["points"]) == 5
    assert chart["points"][0]["first_tick"] is True
    assert chart["points"][-1]["last_tick"] is True
    assert chart["points"][-1]["tick_label"] == "Dec 31 ’25"
    assert chart["cadence_label"] == "8-day buckets"
    assert chart["range_label"] == "Jan 1 – Dec 31, 2025"


def test_dense_dashboard_renders_sparse_readable_date_ticks(tmp_path):
    db_path = tmp_path / "dense.db"
    start = datetime(2026, 6, 1, tzinfo=timezone.utc)
    with Store(db_path) as store:
        store.upsert(
            [
                Mention(
                    source="hackernews",
                    query="dense",
                    text=f"mention {index}",
                    url=f"https://example.test/{index}",
                    created_at=start + timedelta(days=index),
                    sentiment=(Sentiment.POSITIVE if index % 3 == 0 else Sentiment.NEUTRAL),
                )
                for index in range(40)
            ]
        )

    response = TestClient(create_app(str(db_path))).get("/")
    axis = re.search(r'<div class="xaxis"[^>]*>(.*?)</div>', response.text, re.S)
    assert response.status_code == 200
    assert axis
    assert len(re.findall(r"<b>.*?</b>", axis.group(1))) == 5
    assert "Jun 1 – Jul 10, 2026 · Daily" in response.text
    assert response.text.count('class="col"') == 40


def test_api_mentions_returns_rows(tmp_path):
    client = TestClient(create_app(seeded_db(tmp_path)))
    r = client.get("/api/mentions", params={"q": "acme"})
    assert r.status_code == 200
    assert len(r.json()) == 3


def test_api_mentions_filters_by_sentiment(tmp_path):
    client = TestClient(create_app(seeded_db(tmp_path)))
    r = client.get("/api/mentions", params={"q": "acme", "sentiment": "positive"})
    assert r.status_code == 200
    rows = r.json()
    assert len(rows) == 1
    assert rows[0]["sentiment"] == "positive"


def test_api_mentions_rejects_unknown_sentiment(tmp_path):
    client = TestClient(create_app(seeded_db(tmp_path)))
    r = client.get("/api/mentions", params={"sentiment": "mixed"})
    assert r.status_code == 422


def test_api_mentions_rejects_negative_limit(tmp_path):
    # regression test: ?limit=-1 used to pass FastAPI validation (only le=1000
    # was set) and SQLite treats `LIMIT -1` as "no limit", dumping the table.
    client = TestClient(create_app(seeded_db(tmp_path)))
    r = client.get("/api/mentions", params={"q": "acme", "limit": -1})
    assert r.status_code == 422


def test_api_mentions_rejects_zero_limit(tmp_path):
    client = TestClient(create_app(seeded_db(tmp_path)))
    r = client.get("/api/mentions", params={"q": "acme", "limit": 0})
    assert r.status_code == 422


def test_api_summary(tmp_path):
    client = TestClient(create_app(seeded_db(tmp_path)))
    r = client.get("/api/summary", params={"q": "acme"})
    assert r.status_code == 200
    body = r.json()
    assert body["summary"]["total"] == 3
    assert "net" in body


def test_untrusted_link_schemes_are_not_rendered(tmp_path):
    db_path = tmp_path / "unsafe.db"
    with Store(db_path) as store:
        store.upsert([mk("click me", url="javascript:alert(1)")])
    client = TestClient(create_app(str(db_path)))
    api_row = client.get("/api/mentions").json()[0]
    assert api_row["url"] is None
    assert "javascript:" not in client.get("/").text


def test_unknown_dashboard_query_falls_back_to_stored_data(tmp_path):
    client = TestClient(create_app(seeded_db(tmp_path)))
    r = client.get("/", params={"q": "not-tracked"})
    assert r.status_code == 200
    assert '<h1 class="subject" title="acme">acme</h1>' in r.text


def test_dashboard_uses_singular_mention_labels(tmp_path):
    db_path = tmp_path / "singular.db"
    with Store(db_path) as store:
        store.upsert([mk("one result")])
    page = TestClient(create_app(str(db_path))).get("/")
    assert '<span class="big-label">mention<br>tracked</span>' in page.text
    assert "Showing 1 mention." in page.text


def test_health_endpoint(tmp_path):
    client = TestClient(create_app(str(tmp_path / "health.db")))
    assert client.get("/health").json() == {"status": "ok", "database": "ok"}


def test_metrics_endpoint_reports_persisted_counts(tmp_path):
    db_path = seeded_db(tmp_path)
    with Store(db_path) as store:
        store.record_source_metric(
            'source"with\\escapes',
            duration_seconds=0.125,
            fetched=3,
            pages=1,
            retries=2,
            error="upstream unavailable",
        )
    client = TestClient(create_app(db_path))
    response = client.get("/metrics")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    assert "harken_mentions 3" in response.text
    assert "harken_queries 1" in response.text
    assert "harken_alerts_pending 0" in response.text
    assert "harken_threshold_alerts_pending 0" in response.text
    assert "harken_threshold_alerts_delivered 0" in response.text
    assert 'harken_source_scans_total{source="source\\"with\\\\escapes"} 1' in response.text
    assert 'harken_source_errors_total{source="source\\"with\\\\escapes"} 1' in response.text
    assert 'harken_source_retries_total{source="source\\"with\\\\escapes"} 2' in response.text
    assert (
        'harken_source_last_scan_duration_seconds{source="source\\"with\\\\escapes"} 0.125'
        in response.text
    )
    assert 'harken_source_last_scan_success{source="source\\"with\\\\escapes"} 0' in response.text


def test_project_dashboard_and_apis_roll_up_multiple_keywords(tmp_path):
    db_path = seeded_db(tmp_path)
    with Store(db_path) as store:
        store.upsert(
            [
                mk("beta works", Sentiment.POSITIVE, query="beta", url="b1"),
                mk("beta failed", Sentiment.NEGATIVE, query="beta", url="b2"),
            ]
        )
        project = store.create_project("Product Suite")
        store.add_query_to_project(project["id"], "acme")
        store.add_query_to_project(project["id"], "beta")

    client = TestClient(create_app(db_path))
    page = client.get("/", params={"p": project["id"]})
    assert page.status_code == 200
    assert "project rollup" in page.text
    assert '<h1 class="subject" title="Product Suite">Product Suite</h1>' in page.text
    assert "5" in page.text
    assert "all keywords" in page.text
    assert "acme" in page.text and "beta" in page.text
    assert 'id="project-delete-dialog"' in page.text
    assert f'data-confirm-project-delete="{project["id"]}"' in page.text

    summary = client.get("/api/summary", params={"p": project["id"]}).json()
    assert summary["summary"]["total"] == 5
    assert summary["summary"]["by_sentiment"] == {
        "negative": 2,
        "positive": 2,
        "neutral": 1,
    }
    mentions = client.get(
        "/api/mentions", params={"p": project["id"], "sentiment": "negative"}
    ).json()
    assert {row["query"] for row in mentions} == {"acme", "beta"}
    assert client.get("/api/summary", params={"p": 999}).status_code == 404
    assert client.get("/api/mentions", params={"p": project["id"], "q": "acme"}).status_code == 422


def test_project_mutation_apis_require_csrf_and_preserve_keyword_data(tmp_path):
    db_path = seeded_db(tmp_path)
    client = TestClient(create_app(db_path))
    page = client.get("/")
    token = re.search(r'data-csrf="([^"]+)"', page.text).group(1)

    assert client.post("/api/projects", json={"name": "Suite"}).status_code == 403
    created = client.post("/api/projects", json={"name": "Suite"}, headers={"X-Harken-CSRF": token})
    assert created.status_code == 201
    project_id = created.json()["id"]
    duplicate = client.post(
        "/api/projects", json={"name": "suite"}, headers={"X-Harken-CSRF": token}
    )
    assert duplicate.status_code == 409

    added = client.post(
        f"/api/projects/{project_id}/queries",
        json={"query": "acme"},
        headers={"X-Harken-CSRF": token},
    )
    assert added.status_code == 200
    assert added.json()["project"]["queries"] == ["acme"]
    removed = client.request(
        "DELETE",
        f"/api/projects/{project_id}/queries",
        json={"query": "acme"},
        headers={"X-Harken-CSRF": token},
    )
    assert removed.status_code == 200
    deleted = client.delete(f"/api/projects/{project_id}", headers={"X-Harken-CSRF": token})
    assert deleted.status_code == 200
    assert client.delete("/api/projects/1", headers={"X-Harken-CSRF": token}).status_code == 409
    with Store(db_path) as store:
        assert store.summary("acme")["total"] == 3


def test_dashboard_scan_can_create_keyword_inside_project(tmp_path, monkeypatch):
    from harken.sources import REGISTRY
    from harken.sources.base import FetchPage

    class EmptyProjectSource:
        label = "Empty Project Source"
        needs_config = False

        def __init__(self, **options):
            pass

        def fetch_page(self, query, limit=50, *, cursor=None, since=None):
            return FetchPage([])

    monkeypatch.setitem(REGISTRY, "empty-project", EmptyProjectSource)
    db_path = str(tmp_path / "project-scan.db")
    with Store(db_path) as store:
        project = store.create_project("Focused")
    client = TestClient(
        create_app(db_path, config=Config(db_path=db_path, sources=["empty-project"]))
    )
    token = re.search(r'data-csrf="([^"]+)"', client.get("/").text).group(1)
    response = client.post(
        "/api/track",
        json={
            "query": "project-only",
            "sources": ["empty-project"],
            "project_id": project["id"],
        },
        headers={"X-Harken-CSRF": token},
    )
    assert response.status_code == 200, response.text
    assert response.json()["project_id"] == project["id"]
    with Store(db_path) as store:
        assert store.queries(project_id=project["id"]) == ["project-only"]
        assert "project-only" not in store.queries(project_id=1)


def test_optional_basic_auth_protects_everything_except_health(tmp_path):
    client = TestClient(
        create_app(
            seeded_db(tmp_path),
            auth_username="admin",
            auth_password="correct horse battery staple",
        )
    )
    unauthorized = client.get("/")
    assert unauthorized.status_code == 401
    assert unauthorized.headers["www-authenticate"].startswith("Basic")
    assert unauthorized.headers["x-frame-options"] == "DENY"
    assert client.get("/metrics").status_code == 401
    assert client.get("/static/style.css").status_code == 401
    assert client.get("/health").status_code == 200
    assert client.get("/", auth=("admin", "wrong")).status_code == 401
    assert client.get("/", auth=("admin", "correct horse battery staple")).status_code == 200


def test_partial_basic_auth_configuration_is_rejected(tmp_path):
    with pytest.raises(ValueError, match="must be set together"):
        create_app(str(tmp_path / "auth.db"), auth_username="admin")


def test_dashboard_scan_api_persists_empty_keyword_and_requires_csrf(tmp_path, monkeypatch):
    from harken.sources import REGISTRY
    from harken.sources.base import FetchPage

    class EmptySource:
        label = "Empty Source"
        needs_config = False

        def __init__(self, **options):
            pass

        def fetch_page(self, query, limit=50, *, cursor=None, since=None):
            return FetchPage([])

    monkeypatch.setitem(REGISTRY, "empty", EmptySource)
    db_path = str(tmp_path / "scan.db")
    client = TestClient(create_app(db_path, config=Config(db_path=db_path, sources=["empty"])))
    page = client.get("/")
    token = re.search(r'data-csrf="([^"]+)"', page.text).group(1)
    payload = {"query": "No Hits Yet", "sources": ["empty"], "mode": "incremental"}

    assert client.post("/api/track", json=payload).status_code == 403
    response = client.post("/api/track", json=payload, headers={"X-Harken-CSRF": token})
    assert response.status_code == 200, response.text
    assert response.json()["fetched"] == 0
    with Store(db_path) as store:
        assert store.queries() == ["No Hits Yet"]
        assert store.tracking("No Hits Yet")["sources"] == ["empty"]
    empty_page = client.get("/", params={"q": "No Hits Yet"}).text
    assert "No Hits Yet" in empty_page
    assert empty_page.count('<span class="cell-v">—</span>') == 2
    assert 'class="filters-div"' not in empty_page


def test_dashboard_scan_api_rejects_unknown_source(tmp_path):
    client = TestClient(create_app(str(tmp_path / "scan.db")))
    page = client.get("/")
    token = re.search(r'data-csrf="([^"]+)"', page.text).group(1)
    response = client.post(
        "/api/track",
        json={"query": "acme", "sources": ["not-real"]},
        headers={"X-Harken-CSRF": token},
    )
    assert response.status_code == 422
    assert "Unknown source" in response.json()["detail"]


def test_oversized_project_id_is_rejected_not_500(tmp_path):
    # A project id past SQLite's INTEGER range must be a clean 422, never a 500.
    client = TestClient(create_app(seeded_db(tmp_path)))
    huge = "9" * 26
    for path in (f"/?p={huge}", f"/api/summary?p={huge}", f"/api/mentions?p={huge}"):
        assert client.get(path).status_code == 422, path
    assert client.get("/?p=0").status_code == 422


def test_login_rejects_non_ascii_csrf_without_500(tmp_path):
    db_path = str(tmp_path / "nonascii.db")
    cfg = Config(db_path=db_path, auth_mode="accounts")
    client = TestClient(create_app(db_path, config=cfg), follow_redirects=False)
    response = client.post(
        "/login",
        content=b"csrf=%C3%A9&username=x&password=y",
        headers={"content-type": "application/x-www-form-urlencoded"},
    )
    assert response.status_code == 403


def test_oversized_project_id_on_mutations_is_rejected_not_500(tmp_path):
    client = TestClient(create_app(seeded_db(tmp_path)))
    token = re.search(r'data-csrf="([^"]+)"', client.get("/").text).group(1)
    huge = "9" * 26
    assert (
        client.request(
            "DELETE", f"/api/projects/{huge}", headers={"X-Harken-CSRF": token}
        ).status_code
        == 422
    )
    assert (
        client.post(
            f"/api/projects/{huge}/queries",
            json={"query": "acme"},
            headers={"X-Harken-CSRF": token},
        ).status_code
        == 422
    )
    assert (
        client.post(
            "/api/track",
            json={"query": "acme", "sources": ["hackernews"], "project_id": int(huge)},
            headers={"X-Harken-CSRF": token},
        ).status_code
        == 422
    )


def test_chart_series_tolerates_null_sentiment_days(tmp_path):
    # A day whose mentions all have NULL sentiment makes the SUM() columns NULL;
    # the chart must render it as zeros, not crash with int(None).
    db_path = str(tmp_path / "nullsent.db")
    with Store(db_path) as store:
        store.upsert(
            [
                Mention(
                    source="hackernews",
                    query="acme",
                    text="no sentiment yet",
                    url="n1",
                    created_at=datetime(2026, 6, 1, tzinfo=timezone.utc),
                    sentiment=None,
                )
            ]
        )
    client = TestClient(create_app(db_path))
    assert client.get("/?q=acme").status_code == 200
    assert client.get("/api/summary?q=acme").status_code == 200


def test_login_rejects_cross_origin_post(tmp_path):
    db_path = str(tmp_path / "xorigin.db")
    with Store(db_path) as store:
        store.create_user("admin", hash_password("admin password 123", iterations=100_000), "admin")
    cfg = Config(db_path=db_path, auth_mode="accounts")
    client = TestClient(create_app(db_path, config=cfg), follow_redirects=False)
    csrf = re.search(r'name="csrf" value="([^"]+)"', client.get("/login").text).group(1)
    # Even with a valid token, a cross-site Origin must be refused (login CSRF).
    response = client.post(
        "/login",
        data={"username": "admin", "password": "admin password 123", "csrf": csrf},
        headers={"origin": "https://evil.example"},
    )
    assert response.status_code == 403
    assert "harken_session" not in client.cookies
