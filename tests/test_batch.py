"""Synthetic HTTP fixtures exercise real adapters, Pipeline and SQLite batch writes."""

import json

import httpx
import pytest
import respx

from harken.batch import collect, main
from harken.store import Store

TERMS = ["CMBS", "EDGAR", "ABS-EE"]
HN = "https://hn.algolia.com/api/v1/search_by_date"
BSKY = "https://api.bsky.app/xrpc/app.bsky.feed.searchPosts"
FEED = "https://example.test/feed"
RSS = """<?xml version="1.0"?><rss version="2.0"><channel><title>Fixture</title>
<item><title>CMBS EDGAR ABS-EE extraction</title><link>https://example.test/post</link>
<description>Useful servicing data</description><pubDate>Thu, 10 Sep 2026 12:00:00 GMT</pubDate>
</item><item><title>Unrelated post</title><link>https://example.test/other</link></item>
</channel></rss>"""


@pytest.fixture
def routes():
    with respx.mock(assert_all_called=False) as mock:
        def hn_response(request):
            term = request.url.params["query"]
            return httpx.Response(200, json={"hits": [{
                "objectID": term, "title": f"{term} data", "created_at_i": 1700000000,
            }]})
        hn = mock.get(HN).mock(side_effect=hn_response)
        bluesky = mock.get(BSKY).respond(200, json={"posts": []})
        rss = mock.get(FEED).respond(200, text=RSS)
        yield mock, hn, bluesky, rss


def test_sequential_batch_fetches_feed_once_and_stores_each_query(tmp_path, routes):
    mock, hn, bluesky, rss = routes
    path = tmp_path / "batch.db"
    results = collect(TERMS, sources=["hackernews", "bluesky", "rss"],
                      db_path=str(path), rss_feeds=[FEED])
    assert [result.query for result in results] == TERMS
    assert all(not result.errors and result.new == 2 for result in results)
    assert hn.call_count == bluesky.call_count == 3
    assert rss.call_count == 1
    assert [call.request.url.host for call in mock.calls] == [
        "hn.algolia.com", "api.bsky.app", "example.test", "hn.algolia.com",
        "api.bsky.app", "hn.algolia.com", "api.bsky.app",
    ]
    store = Store(str(path))
    try:
        rss_ids = []
        for term in TERMS:
            mentions = store.mentions(query=term)
            assert len(mentions) == 2
            assert {mention.query for mention in mentions} == {term}
            rss_ids.append(next(mention.id for mention in mentions if mention.source == "rss"))
        assert len(set(rss_ids)) == 1  # Store associates the same item with all matching terms.
        metrics = {item["source"]: item for item in store.source_metrics()}
        assert metrics["rss"]["scans_total"] == 3
        assert metrics["bluesky"]["fetched_total"] == 0  # A genuine empty parsed fixture result.
    finally:
        store.close()


@pytest.mark.parametrize("status", [200, 401])
@respx.mock
def test_batch_shares_bluesky_login_success_or_failure(tmp_path, monkeypatch, status):
    monkeypatch.setenv("HARKEN_BLUESKY_HANDLE", "test.bsky.social")
    monkeypatch.setenv("HARKEN_BLUESKY_APP_PASSWORD", "secret")
    login = respx.post("https://bsky.social/xrpc/com.atproto.server.createSession").respond(
        status, json={"accessJwt": "secret-token", "did": "did:plc:test", "didDoc": {
            "id": "did:plc:test", "service": [{"id": "#atproto_pds",
                "type": "AtprotoPersonalDataServer",
                "serviceEndpoint": "https://test.host.bsky.network"}]}})
    if status == 200:
        search = respx.get(
            "https://test.host.bsky.network/xrpc/app.bsky.feed.searchPosts"
        ).respond(200, json={"posts": []})
    results = collect(TERMS, sources=["bluesky"], db_path=str(tmp_path / "auth.db"))
    assert login.call_count == 1
    assert all(bool(result.errors) == (status != 200) for result in results)
    if status == 200:
        assert search.call_count == 3


@pytest.mark.parametrize("failure", ["http", "network", "malformed"])
def test_feed_failure_is_cached_without_retry_other_sources_still_land(tmp_path, routes, failure):
    _, hn, _, rss = routes
    if failure == "http":
        rss.respond(429, headers={"Retry-After": "1"})
    elif failure == "network":
        rss.mock(side_effect=httpx.ConnectError("fixture network failure"))
    else:
        rss.respond(200, text="not a feed")
    path = tmp_path / "failed.db"
    results = collect(TERMS, sources=["hackernews", "rss"], db_path=str(path), rss_feeds=[FEED])
    assert hn.call_count == 3
    assert rss.call_count == 1
    assert all(set(result.errors) == {"rss"} and result.new == 1 for result in results)
    store = Store(str(path))
    try:
        for term in TERMS:
            assert len(store.mentions(query=term)) == 1
            assert store.source_state(term, "rss")["last_error"]
    finally:
        store.close()


def test_feed_cache_does_not_outlive_batch_and_repeats_deduplicate(tmp_path, routes):
    _, _, _, rss = routes
    path = str(tmp_path / "repeat.db")
    assert sum(result.new for result in collect(TERMS, sources=["rss"], db_path=path,
                                               rss_feeds=[FEED])) == 3
    assert sum(result.new for result in collect(TERMS, sources=["rss"], db_path=path,
                                               rss_feeds=[FEED])) == 0
    assert rss.call_count == 2


def test_cli_disables_ambient_alerts_models_and_reports_partial_failure(
    tmp_path, routes, monkeypatch, capsys,
):
    _, _, _, rss = routes
    rss.respond(401)
    monkeypatch.setenv("HARKEN_RSS_FEEDS", FEED)
    monkeypatch.setenv("HARKEN_WEBHOOK_URL", "https://unexpected.test/hook")
    monkeypatch.setenv("HARKEN_EMAIL_TO", "nobody@example.test")
    monkeypatch.setenv("HARKEN_SMTP_HOST", "unexpected.test")
    monkeypatch.setenv("HARKEN_LLM_PROVIDER", "openai")
    monkeypatch.setenv("HARKEN_SENTIMENT_ANALYZER", "llm")
    code = main(["--terms", *TERMS, "--sources", "hackernews,rss",
                 "--db", str(tmp_path / "cli.db")])
    assert code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["event"] == "batch_complete"
    assert all(item["errors"] for item in payload["results"])
    assert rss.call_count == 1


def test_long_retry_after_receipt_preserves_server_delay(tmp_path, routes, monkeypatch, capsys):
    _, _, _, rss = routes
    rss.respond(429, headers={"Retry-After": "7200"})
    monkeypatch.setenv("HARKEN_RSS_FEEDS", FEED)
    receipt_path = tmp_path / "receipt.json"
    assert main(["--terms", *TERMS, "--sources", "rss", "--db", str(tmp_path / "retry.db"),
                 "--receipt", str(receipt_path)]) == 1
    receipt = json.loads(receipt_path.read_text())
    assert receipt["retry_after_seconds"] == 7200
    assert receipt["source_error_count"] == 3
    assert receipt["started_at"] <= receipt["finished_at"]
    assert set(receipt) == {"started_at", "finished_at", "retry_after_seconds", "source_error_count"}
    assert FEED not in receipt_path.read_text()
    assert json.loads(capsys.readouterr().out)["retry_after_seconds"] == 7200
    assert rss.call_count == 1


@pytest.mark.parametrize("login_status", [200, 429])
@respx.mock
def test_bluesky_cooldown_reaches_receipt_once_per_login(
    tmp_path, monkeypatch, login_status,
):
    monkeypatch.setenv("HARKEN_BLUESKY_HANDLE", "test.bsky.social")
    monkeypatch.setenv("HARKEN_BLUESKY_APP_PASSWORD", "secret")
    monkeypatch.setenv("HARKEN_RSS_FEEDS", FEED)
    login = respx.post("https://bsky.social/xrpc/com.atproto.server.createSession").respond(
        login_status, headers={"Retry-After": "7200"},
        json={"accessJwt": "secret-token", "did": "did:plc:test", "didDoc": {
            "id": "did:plc:test", "service": [{"id": "#atproto_pds",
                "type": "AtprotoPersonalDataServer",
                "serviceEndpoint": "https://test.host.bsky.network"}]}})
    if login_status == 200:
        respx.get("https://test.host.bsky.network/xrpc/app.bsky.feed.searchPosts").respond(
            429, headers={"Retry-After": "7200"}, text="secret-token")
    respx.get(FEED).respond(429, headers={"Retry-After": "3600"})
    receipt = tmp_path / "cooldown.json"
    assert main(["--terms", *TERMS, "--sources", "bluesky,rss",
                 "--db", str(tmp_path / "cooldown.db"), "--receipt", str(receipt)]) == 1
    assert login.call_count == 1
    assert json.loads(receipt.read_text())["retry_after_seconds"] == 7200
    assert "secret" not in receipt.read_text()


@pytest.mark.parametrize("header", ["-1", "NaN", "invalid"])
def test_invalid_retry_after_does_not_change_receipt_delay(tmp_path, routes, monkeypatch, header):
    _, _, _, rss = routes
    rss.respond(429, headers={"Retry-After": header})
    monkeypatch.setenv("HARKEN_RSS_FEEDS", FEED)
    receipt_path = tmp_path / "receipt.json"
    assert main(["--terms", "CMBS", "--sources", "rss", "--db", str(tmp_path / "retry.db"),
                 "--receipt", str(receipt_path)]) == 1
    assert json.loads(receipt_path.read_text())["retry_after_seconds"] == 0
    assert rss.call_count == 1


def test_successful_batch_replaces_old_failure_receipt(tmp_path, routes, monkeypatch):
    _, _, _, rss = routes
    monkeypatch.setenv("HARKEN_RSS_FEEDS", FEED)
    receipt_path = tmp_path / "receipt.json"
    receipt_path.write_text(json.dumps({"started_at": 1, "retry_after_seconds": 7200,
                                        "source_error_count": 3}))
    assert main(["--terms", "CMBS", "--sources", "rss", "--db", str(tmp_path / "success.db"),
                 "--receipt", str(receipt_path)]) == 0
    receipt = json.loads(receipt_path.read_text())
    assert receipt["started_at"] > 1
    assert receipt["retry_after_seconds"] == receipt["source_error_count"] == 0
    assert rss.call_count == 1


@pytest.mark.parametrize("terms,sources,feeds", [
    ([], ["rss"], [FEED]), (TERMS + ["fourth"], ["rss"], [FEED]),
    (["CMBS", "cmbs"], ["rss"], [FEED]), ([" "], ["rss"], [FEED]),
    (TERMS, ["youtube"], []), (TERMS, ["rss", "rss"], [FEED]),
    (TERMS, ["rss"], [FEED, FEED]),
])
def test_bounds_rejected_before_opening_database(tmp_path, terms, sources, feeds):
    path = tmp_path / "not-created.db"
    with pytest.raises(ValueError):
        collect(terms, sources=sources, db_path=str(path), rss_feeds=feeds)
    assert not path.exists()
