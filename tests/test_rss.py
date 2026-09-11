"""RSS source tests — HTTP is mocked, so these run offline and deterministically."""

from datetime import datetime, timezone
from time import struct_time

import httpx
import pytest
import respx

from harken.config import Config
from harken.pipeline import Pipeline
from harken.sources.base import retry_after_seconds
from harken.sources.rss import RSSSource, _entry_time

_FEED_A = """<?xml version="1.0"?>
<rss version="2.0"><channel>
<item><title>Acme one</title><description>people are talking</description>
<link>https://example.com/a1</link><author>alice</author>
<pubDate>Mon, 01 Jan 2024 00:00:00 GMT</pubDate></item>
<item><title>Acme two</title><description>more chatter</description>
<link>https://example.com/a2</link><author>alice</author>
<pubDate>Mon, 01 Jan 2024 00:00:00 GMT</pubDate></item>
</channel></rss>"""

_FEED_B = """<?xml version="1.0"?>
<rss version="2.0"><channel>
<item><title>Acme three</title><description>from another feed entirely</description>
<link>https://example.com/b1</link><author>bob</author>
<pubDate>Mon, 01 Jan 2024 00:00:00 GMT</pubDate></item>
</channel></rss>"""


@respx.mock
def test_fetches_every_configured_feed_even_once_limit_is_hit():
    feed_a = respx.get("https://feeds.example/a.xml").mock(
        return_value=httpx.Response(200, content=_FEED_A)
    )
    feed_b = respx.get("https://feeds.example/b.xml").mock(
        return_value=httpx.Response(200, content=_FEED_B)
    )
    src = RSSSource(feeds=["https://feeds.example/a.xml", "https://feeds.example/b.xml"])
    out = src.fetch("acme", limit=1)

    assert feed_a.called
    assert feed_b.called  # regression: used to never be requested once feed A hit `limit`
    assert len(out) == 1  # the limit is still respected in the returned result


@respx.mock
def test_isolates_a_single_bad_feed():
    respx.get("https://feeds.example/broken.xml").mock(return_value=httpx.Response(500))
    respx.get("https://feeds.example/b.xml").mock(return_value=httpx.Response(200, content=_FEED_B))
    src = RSSSource(feeds=["https://feeds.example/broken.xml", "https://feeds.example/b.xml"])
    out = src.fetch("acme", limit=50)

    assert len(out) == 1
    assert out[0].author == "bob"


@respx.mock
def test_filters_entries_by_query():
    respx.get("https://feeds.example/a.xml").mock(return_value=httpx.Response(200, content=_FEED_A))
    src = RSSSource(feeds=["https://feeds.example/a.xml"])
    out = src.fetch("nonexistent-brand", limit=50)
    assert out == []


def test_uses_the_shared_timeout_client():
    # regression: fetch() used to call feedparser.parse(url) directly, which
    # bypasses Source._client()'s 15s timeout entirely (unbounded hang risk).
    src = RSSSource(feeds=["https://feeds.example/a.xml"])
    client = src._client()
    try:
        assert client.timeout == httpx.Timeout(15.0)
    finally:
        client.close()


def test_requires_at_least_one_feed():
    with pytest.raises(RuntimeError, match="HARKEN_RSS_FEEDS"):
        RSSSource().fetch("acme")


def test_feed_struct_time_is_interpreted_as_utc():
    entry = {"published_parsed": struct_time((2024, 1, 1, 0, 0, 0, 0, 1, 0))}
    assert _entry_time(entry) == datetime(2024, 1, 1, tzinfo=timezone.utc)


@respx.mock
@pytest.mark.parametrize("successful_feed", [False, True])
def test_pipeline_records_rss_failure_without_discarding_good_entries(tmp_path, successful_feed):
    feeds = ["https://feeds.example/broken.xml?token=private-value"]
    respx.get(feeds[0]).mock(return_value=httpx.Response(503))
    if successful_feed:
        feeds.append("https://feeds.example/b.xml")
        respx.get(feeds[1]).mock(return_value=httpx.Response(200, content=_FEED_B))
    pipe = Pipeline(Config(db_path=str(tmp_path / "test.db"), sources=["rss"],
                           rss_feeds=feeds, source_retries=2))
    result = pipe.track("acme")
    assert "HTTP 503" in result.errors["rss"]
    assert "private-value" not in result.errors["rss"]
    assert result.new == (1 if successful_feed else 0)
    assert len(pipe.store.mentions(query="acme")) == result.new
    metrics = pipe.store.source_metrics()[0]
    assert metrics["errors_total"] == 1 and metrics["last_success"] == 0
    assert metrics["fetched_total"] == result.new
    state = pipe.store.source_state("acme", "rss")
    assert state["last_error"]
    assert not state.get("last_success_at")
    # A page reporting partial failure is not retried as a whole: successful
    # feeds are retained without a tight retry loop over every URL.
    assert len(respx.calls) == len(feeds)


@respx.mock
def test_pipeline_empty_feed_is_success_and_contact_user_agent_reaches_wire(tmp_path, monkeypatch):
    monkeypatch.setenv("HARKEN_USER_AGENT", "example-listener/1.0 (+https://example.com/contact)")
    feed = respx.get("https://feeds.example/empty.xml").mock(return_value=httpx.Response(
        200, content='<rss version="2.0"><channel><title>Empty</title></channel></rss>'
    ))
    pipe = Pipeline(Config(db_path=str(tmp_path / "test.db"), sources=["rss"],
                           rss_feeds=["https://feeds.example/empty.xml"]))
    result = pipe.track("acme")
    assert result.errors == {} and result.fetched == 0
    assert pipe.store.source_metrics()[0]["last_success"] == 1
    assert feed.calls[0].request.headers["User-Agent"] == (
        "example-listener/1.0 (+https://example.com/contact)"
    )


@respx.mock
def test_http_200_non_feed_is_not_healthy(tmp_path):
    respx.get("https://feeds.example/a.xml").mock(return_value=httpx.Response(
        200, text="<html><body>Gateway login</body></html>"
    ))
    pipe = Pipeline(Config(db_path=str(tmp_path / "test.db"), sources=["rss"],
                           rss_feeds=["https://feeds.example/a.xml"]))
    result = pipe.track("acme")
    assert "invalid RSS/Atom" in result.errors["rss"]
    assert pipe.store.source_metrics()[0]["last_success"] == 0


def test_user_agent_rejects_header_control_characters(monkeypatch):
    monkeypatch.setenv("HARKEN_USER_AGENT", "listener\r\nX-Extra: value")
    with pytest.raises(ValueError, match="printable ASCII"):
        Config()


@respx.mock
def test_partial_failure_recovers_without_duplicate_mentions(tmp_path):
    broken = respx.get("https://feeds.example/a.xml").mock(return_value=httpx.Response(500))
    respx.get("https://feeds.example/b.xml").mock(return_value=httpx.Response(200, content=_FEED_B))
    pipe = Pipeline(Config(db_path=str(tmp_path / "test.db"), sources=["rss"],
                           rss_feeds=["https://feeds.example/a.xml", "https://feeds.example/b.xml"]))
    assert pipe.track("acme").new == 1
    broken.mock(return_value=httpx.Response(200, content=_FEED_A))
    recovered = pipe.track("acme")
    assert recovered.errors == {} and recovered.new == 2
    assert len(pipe.store.mentions(query="acme")) == 3
    assert pipe.store.source_state("acme", "rss")["last_error"] is None
    metrics = pipe.store.source_metrics()[0]
    assert metrics["last_success"] == 1 and metrics["errors_total"] == 1


@respx.mock
@pytest.mark.parametrize("status", [200, 429])
def test_batch_cache_fetches_each_url_once_including_failures(status):
    route = respx.get("https://feeds.example/b.xml").mock(return_value=httpx.Response(
        status, content=_FEED_B
    ))
    cache = {}
    first = RSSSource(feeds=["https://feeds.example/b.xml"], batch_cache=cache).fetch_page("acme")
    second = RSSSource(feeds=["https://feeds.example/b.xml"], batch_cache=cache).fetch_page("another")
    assert route.call_count == 1
    assert len(first.mentions) == len(second.mentions) == (1 if status == 200 else 0)
    assert bool(first.errors) == bool(second.errors) == (status != 200)
    if status == 200:
        assert first.mentions[0].query == "acme"
        assert second.mentions[0].query == "another"
    RSSSource(feeds=["https://feeds.example/b.xml"], batch_cache={}).fetch_page("acme")
    assert route.call_count == 2  # a new batch must refresh the upstream response


@respx.mock
@pytest.mark.parametrize("date_xml, published, updated", [
    ("", False, False),
    ("<updated>2024-01-02T00:00:00Z</updated>", False, True),
    ("<published>2024-01-01T00:00:00Z</published>", True, False),
    ("<published>2024-01-01T00:00:00Z</published>"
     "<updated>2024-01-02T00:00:00Z</updated>", True, True),
])
def test_rss_publication_provenance_does_not_invent_dates(date_xml, published, updated):
    atom = ('<feed xmlns="http://www.w3.org/2005/Atom"><title>Feed</title>'
            '<entry><title>Acme</title><id>https://example.com/post</id>'
            f'{date_xml}</entry></feed>')
    respx.get("https://feeds.example/atom.xml").mock(return_value=httpx.Response(200, text=atom))
    mention = RSSSource(feeds=["https://feeds.example/atom.xml"]).fetch("acme")[0]
    assert (mention.published_at is not None) is published
    assert (mention.source_updated_at is not None) is updated
    assert mention.publication_provenance == ("source_published" if published else "unknown")
    if published:
        assert mention.published_at == datetime(2024, 1, 1, tzinfo=timezone.utc)
    if updated:
        assert mention.source_updated_at == datetime(2024, 1, 2, tzinfo=timezone.utc)


@respx.mock
def test_rss_source_identity_is_stable_per_feed_without_url_credentials():
    feed_a = "https://feeds.example/a.xml?token=private-a"
    feed_b = "https://feeds.example/b.xml?token=private-b"
    respx.get(feed_a).mock(return_value=httpx.Response(200, content=_FEED_A))
    respx.get(feed_b).mock(return_value=httpx.Response(200, content=_FEED_B))
    mentions = RSSSource(feeds=[feed_a, feed_b]).fetch("acme")
    assert mentions[0].source_id == mentions[1].source_id
    assert mentions[0].source_id != mentions[2].source_id
    assert all(mention.source_id.startswith("rss:") and len(mention.source_id) == 68
               for mention in mentions)
    assert "private-" not in " ".join(mention.source_id for mention in mentions)
    repeated = RSSSource(feeds=[feed_a]).fetch("acme")
    assert repeated[0].source_id == mentions[0].source_id


@pytest.mark.parametrize("value, expected", [
    ("7200", 7200), ("7200.01", 7201), ("0", 0), ("-1", None),
    ("NaN", None), ("Infinity", None), ("invalid", None), (None, None),
    ("1e5000", None),
    ("Thu, 10 Sep 2026 14:00:00 GMT", 7200),
    ("Thu, 10 Sep 2026 10:00:00 GMT", 0),
    ("999999999999999999999", 999999999999999999999),
])
def test_retry_after_preserves_long_delays_and_rejects_invalid(value, expected):
    assert retry_after_seconds(value, now=datetime(2026, 9, 10, 12, tzinfo=timezone.utc)) == expected


@respx.mock
def test_retry_after_marker_survives_cached_feed_failure():
    route = respx.get("https://feeds.example/retry.xml").respond(
        429, headers={"Retry-After": "7200.5"},
    )
    cache = {}
    for term in ("first", "second"):
        page = RSSSource(feeds=["https://feeds.example/retry.xml"], batch_cache=cache).fetch_page(term)
        assert page.errors == ["feed[1] HTTPStatusError HTTP 429 retry_after_seconds=7201"]
    assert route.call_count == 1
