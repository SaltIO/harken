"""Source adapter tests — HTTP is mocked, so these run offline and deterministically."""

from datetime import datetime, timedelta, timezone

import httpx
import pytest
import respx

from harken.sources.bluesky import BlueskySource
from harken.sources.hackernews import HackerNewsSource
from harken.sources.reddit import RedditSource
from harken.sources.stackoverflow import StackOverflowSource
from harken.sources.x import XSource
from harken.sources.youtube import YouTubeSource


@respx.mock
def test_hackernews_parses_hits():
    payload = {
        "hits": [
            {
                "objectID": "111",
                "title": "Acme is great",
                "author": "alice",
                "points": 42,
                "created_at_i": 1_700_000_000,
            },
            {
                "objectID": "222",
                "comment_text": "I tried <b>Acme</b> and it&#x27;s fast",
                "story_title": "Show HN: Acme",
                "author": "bob",
                "created_at_i": 1_700_000_500,
            },
        ]
    }
    respx.get("https://hn.algolia.com/api/v1/search_by_date").mock(
        return_value=httpx.Response(200, json=payload)
    )
    out = HackerNewsSource().fetch("acme", limit=10)
    assert len(out) == 2
    assert out[0].source == "hackernews"
    assert out[0].author == "alice"
    assert out[0].url == "https://news.ycombinator.com/item?id=111"
    # html stripped + entities decoded
    assert "fast" in out[1].text
    assert "<b>" not in out[1].text
    assert "it's" in out[1].text


@respx.mock
def test_hackernews_page_uses_stable_time_boundaries():
    route = respx.get("https://hn.algolia.com/api/v1/search_by_date").mock(
        return_value=httpx.Response(
            200,
            json={
                "hits": [{"objectID": "1", "created_at_i": 1_700_000_000}],
                "page": 0,
                "nbPages": 2,
            },
        )
    )
    since = datetime(2023, 1, 1, tzinfo=timezone.utc)
    page = HackerNewsSource().fetch_page("acme", limit=1, cursor="1700000100", since=since)
    params = route.calls[0].request.url.params
    # Inclusive lower bound so items sharing the boundary second are not skipped
    # when a page cap splits that group (store dedup absorbs the overlap).
    assert "created_at_i<=1700000100" in params["numericFilters"]
    assert f"created_at_i>{int(since.timestamp())}" in params["numericFilters"]
    assert page.next_cursor == "1700000000"
    assert params["typoTolerance"] == "false"


@respx.mock
def test_hackernews_drops_typo_tolerant_false_positives():
    respx.get("https://hn.algolia.com/api/v1/search_by_date").mock(
        return_value=httpx.Response(
            200,
            json={
                "hits": [
                    {"objectID": "1", "title": "Harken release", "created_at_i": 1},
                    {"objectID": "2", "title": "A hardened service", "created_at_i": 2},
                ]
            },
        )
    )
    out = HackerNewsSource().fetch("harken")
    assert [mention.title for mention in out] == ["Harken release"]


@respx.mock
def test_reddit_parses_children():
    payload = {
        "data": {
            "children": [
                {
                    "data": {
                        "title": "Thoughts on Acme?",
                        "selftext": "is it any good",
                        "author": "carol",
                        "score": 7,
                        "permalink": "/r/test/comments/1/thoughts",
                        "created_utc": 1_700_000_000,
                    }
                }
            ]
        }
    }
    respx.get("https://oauth.reddit.com/search").mock(
        return_value=httpx.Response(200, json=payload)
    )
    out = RedditSource(access_token="test-token").fetch("acme")
    assert len(out) == 1
    assert out[0].source == "reddit"
    assert out[0].url == "https://www.reddit.com/r/test/comments/1/thoughts"
    assert out[0].score == 7


@respx.mock
def test_bluesky_parses_posts():
    payload = {
        "posts": [
            {
                "uri": "at://did:plc:abc/app.bsky.feed.post/xyz",
                "author": {"handle": "dave.bsky.social"},
                "record": {"text": "acme rocks", "createdAt": "2026-06-01T12:00:00Z"},
                "likeCount": 3,
            }
        ]
    }
    respx.get("https://api.bsky.app/xrpc/app.bsky.feed.searchPosts").mock(
        return_value=httpx.Response(200, json=payload)
    )
    out = BlueskySource().fetch("acme")
    assert len(out) == 1
    assert out[0].author == "dave.bsky.social"
    assert out[0].url == "https://bsky.app/profile/dave.bsky.social/post/xyz"


@respx.mock
def test_bluesky_page_preserves_api_cursor_and_since_boundary():
    route = respx.get("https://api.bsky.app/xrpc/app.bsky.feed.searchPosts").mock(
        return_value=httpx.Response(200, json={"posts": [], "cursor": "next-page"})
    )
    since = datetime(2026, 6, 1, tzinfo=timezone.utc)
    page = BlueskySource().fetch_page("acme", cursor="current-page", since=since)
    params = route.calls[0].request.url.params
    assert params["cursor"] == "current-page"
    assert params["since"] == "2026-06-01T00:00:00Z"
    assert page.next_cursor == "next-page"


def _bluesky_session(endpoint="https://test.host.bsky.network"):
    return {"accessJwt": "session-secret", "did": "did:plc:test", "didDoc": {
        "id": "did:plc:test", "service": [{"id": "#atproto_pds",
            "type": "AtprotoPersonalDataServer", "serviceEndpoint": endpoint}]}}


@respx.mock
def test_bluesky_authenticates_once_and_proxies_three_terms():
    login = respx.post("https://bsky.social/xrpc/com.atproto.server.createSession").respond(
        200, json=_bluesky_session())
    search = respx.get("https://test.host.bsky.network/xrpc/app.bsky.feed.searchPosts").respond(
        200, json={"posts": [], "cursor": "next"})
    source = BlueskySource(handle="test.bsky.social", app_password="password-secret")
    for term in ["CMBS", "EDGAR", "ABS-EE"]:
        assert source.fetch_page(term).next_cursor == "next"
    assert login.call_count == 1
    assert search.call_count == 3
    for call in search.calls:
        assert call.request.headers["authorization"] == "Bearer session-secret"
        assert call.request.headers["atproto-proxy"] == "did:web:api.bsky.app#bsky_appview"
    assert "authorization" not in login.calls[0].request.headers


@pytest.mark.parametrize("endpoint", ["http://test.host.bsky.network", "https://localhost",
    "https://bsky.social.evil.example", "https://user:secret@bsky.social",
    "https://test.host.bsky.network/path", "https://bsky.social?secret=value",
    "https://bsky.social:bad", "https://bsky.social\n", None])
@respx.mock
def test_bluesky_rejects_unsafe_pds_without_echoing_session(endpoint):
    respx.post("https://bsky.social/xrpc/com.atproto.server.createSession").respond(
        200, json=_bluesky_session(endpoint))
    source = BlueskySource(handle="test.bsky.social", app_password="password-secret")
    with pytest.raises(RuntimeError) as caught:
        source.fetch("CMBS")
    assert str(caught.value) == "Bluesky login failed; check the handle and app password"
    assert len(respx.calls) == 1


@pytest.mark.parametrize("payload", [{}, [], {"accessJwt": "bad\ntoken"},
                                      {"accessJwt": "secret", "did": None, "didDoc": {}}])
@respx.mock
def test_bluesky_rejects_malformed_session(payload):
    respx.post("https://bsky.social/xrpc/com.atproto.server.createSession").respond(
        200, json=payload)
    with pytest.raises(RuntimeError, match="Bluesky login failed"):
        BlueskySource(handle="test.bsky.social", app_password="secret").fetch("CMBS")


@respx.mock
def test_bluesky_failed_login_is_cached_and_redacted():
    login = respx.post("https://bsky.social/xrpc/com.atproto.server.createSession").respond(
        401, text="password-secret session-secret")
    source = BlueskySource(handle="test.bsky.social", app_password="password-secret")
    for term in ["CMBS", "EDGAR", "ABS-EE"]:
        with pytest.raises(RuntimeError) as caught:
            source.fetch(term)
        assert str(caught.value) == "Bluesky login failed (HTTP 401)"
    assert login.call_count == 1


@respx.mock
def test_bluesky_search_error_drops_credentials_but_keeps_backoff():
    respx.post("https://bsky.social/xrpc/com.atproto.server.createSession").respond(
        200, json=_bluesky_session())
    respx.get("https://test.host.bsky.network/xrpc/app.bsky.feed.searchPosts").respond(
        429, text="session-secret", headers={"Retry-After": "60"})
    source = BlueskySource(handle="test.bsky.social", app_password="password-secret")
    with pytest.raises(httpx.HTTPStatusError) as caught:
        source.fetch("CMBS")
    assert str(caught.value) == "Bluesky search failed (HTTP 429) retry_after_seconds=60"
    assert "authorization" not in caught.value.request.headers
    assert caught.value.response.text == ""
    assert caught.value.response.headers["Retry-After"] == "60"


@respx.mock
def test_reddit_can_get_an_app_only_oauth_token():
    token = respx.post("https://www.reddit.com/api/v1/access_token").mock(
        return_value=httpx.Response(200, json={"access_token": "oauth-token"})
    )
    search = respx.get("https://oauth.reddit.com/search").mock(
        return_value=httpx.Response(200, json={"data": {"children": []}})
    )
    out = RedditSource(client_id="client", client_secret="secret").fetch("acme")
    assert out == []
    assert token.called and search.called
    assert search.calls[0].request.headers["authorization"] == "Bearer oauth-token"


def test_reddit_requires_oauth_configuration():
    with pytest.raises(RuntimeError, match="requires OAuth"):
        RedditSource().fetch("acme")


@respx.mock
def test_stackoverflow_parses_questions_and_preserves_backoff(monkeypatch):
    StackOverflowSource._backoff_until = 0
    StackOverflowSource._quota_remaining = None
    monkeypatch.setattr("harken.sources.stackoverflow.time.monotonic", lambda: 100.0)
    route = respx.get("https://api.stackexchange.com/2.3/search/advanced").mock(
        return_value=httpx.Response(
            200,
            json={
                "items": [
                    {
                        "question_id": 123,
                        "title": "Using Acme &amp; Python",
                        "body": "<p>Acme is <strong>broken</strong></p>",
                        "link": "https://stackoverflow.com/questions/123/acme",
                        "owner": {"display_name": "Alice &amp; Bob"},
                        "creation_date": 1_700_000_000,
                        "score": 4,
                    }
                ],
                "backoff": 30,
                "quota_remaining": 99,
            },
        )
    )
    out = StackOverflowSource().fetch("acme", limit=5)
    assert len(out) == 1
    assert out[0].title == "Using Acme & Python"
    assert out[0].text == "Acme is broken"
    assert out[0].author == "Alice & Bob"
    assert out[0].score == 4
    with pytest.raises(RuntimeError, match="backoff"):
        StackOverflowSource().fetch("acme", limit=5)
    assert len(route.calls) == 1
    StackOverflowSource._backoff_until = 0
    StackOverflowSource._quota_remaining = None


@respx.mock
def test_stackoverflow_centers_long_body_on_the_match():
    StackOverflowSource._backoff_until = 0
    StackOverflowSource._quota_remaining = None
    respx.get("https://api.stackexchange.com/2.3/search/advanced").mock(
        return_value=httpx.Response(
            200,
            json={
                "items": [
                    {
                        "question_id": 1,
                        "title": "A long question",
                        "body": f"<p>{'before ' * 300}AcmeMatch {'after ' * 300}</p>",
                        "creation_date": 1_700_000_000,
                    }
                ],
                "quota_remaining": 99,
            },
        )
    )
    mention = StackOverflowSource().fetch("acmematch")[0]
    assert "AcmeMatch" in mention.text
    assert len(mention.text) <= 802


@respx.mock
def test_youtube_parses_video_search_and_pagination():
    route = respx.get("https://www.googleapis.com/youtube/v3/search").mock(
        return_value=httpx.Response(
            200,
            json={
                "nextPageToken": "older-videos",
                "items": [
                    {
                        "id": {"videoId": "abc123"},
                        "snippet": {
                            "publishedAt": "2026-07-20T12:30:00Z",
                            "channelTitle": "Alice &amp; Bob",
                            "title": "An &lt;Acme&gt; review",
                            "description": "Acme is fast &amp; friendly",
                        },
                    }
                ],
            },
        )
    )
    since = datetime(2026, 7, 1, tzinfo=timezone.utc)
    page = YouTubeSource(api_key="secret-key").fetch_page(
        "acme", limit=25, cursor="current", since=since
    )
    params = route.calls[0].request.url.params
    assert "key" not in params
    assert route.calls[0].request.headers["x-goog-api-key"] == "secret-key"
    assert params["type"] == "video"
    assert params["order"] == "date"
    assert params["pageToken"] == "current"
    assert params["publishedAfter"] == "2026-07-01T00:00:00Z"
    assert page.next_cursor == "older-videos"
    assert len(page.mentions) == 1
    assert page.mentions[0].title == "An <Acme> review"
    assert page.mentions[0].author == "Alice & Bob"
    assert page.mentions[0].url == "https://www.youtube.com/watch?v=abc123"


@respx.mock
def test_x_parses_posts_authors_metrics_and_pagination():
    route = respx.get("https://api.x.com/2/tweets/search/recent").mock(
        return_value=httpx.Response(
            200,
            json={
                "data": [
                    {
                        "id": "123",
                        "author_id": "42",
                        "text": "Acme shipped today",
                        "created_at": "2026-07-20T12:30:00Z",
                        "public_metrics": {"like_count": 17},
                    }
                ],
                "includes": {"users": [{"id": "42", "username": "alice", "name": "Alice"}]},
                "meta": {"next_token": "older-posts"},
            },
        )
    )
    since = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(days=1)
    page = XSource(bearer_token="secret-token").fetch_page(
        "acme", limit=5, cursor="current", since=since
    )
    request = route.calls[0].request
    params = request.url.params
    assert request.headers["authorization"] == "Bearer secret-token"
    assert params["max_results"] == "10"
    assert params["next_token"] == "current"
    assert params["start_time"] == since.isoformat().replace("+00:00", "Z")
    assert page.next_cursor == "older-posts"
    assert len(page.mentions) == 1
    assert page.mentions[0].author == "alice"
    assert page.mentions[0].score == 17
    assert page.mentions[0].url == "https://x.com/alice/status/123"


@pytest.mark.parametrize(
    ("source", "message"),
    [(YouTubeSource(), "HARKEN_YOUTUBE_API_KEY"), (XSource(), "HARKEN_X_BEARER_TOKEN")],
)
def test_keyed_sources_fail_before_network_without_credentials(source, message):
    with pytest.raises(RuntimeError, match=message):
        source.fetch("acme")


@respx.mock
def test_youtube_http_error_does_not_expose_api_key():
    respx.get("https://www.googleapis.com/youtube/v3/search").mock(
        return_value=httpx.Response(403, json={"error": {"message": "denied"}})
    )
    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        YouTubeSource(api_key="do-not-leak").fetch("acme")
    assert "do-not-leak" not in str(exc_info.value)


@respx.mock
def test_x_omits_stale_or_too_recent_time_boundary():
    route = respx.get("https://api.x.com/2/tweets/search/recent").mock(
        return_value=httpx.Response(200, json={"data": [], "meta": {}})
    )
    source = XSource(bearer_token="token")
    source.fetch_page("acme", since=datetime.now(timezone.utc) - timedelta(days=8))
    source.fetch_page("acme", since=datetime.now(timezone.utc) - timedelta(seconds=5))
    assert all("start_time" not in call.request.url.params for call in route.calls)


def test_mention_id_is_stable_and_dedupes():
    from datetime import datetime, timezone

    from harken.models import Mention

    kw = dict(source="hackernews", query="acme", created_at=datetime.now(timezone.utc))
    a = Mention(url="https://x/1", text="hello", **kw)
    b = Mention(url="https://x/1", text="hello", **kw)
    c = Mention(url="https://x/2", text="hello", **kw)
    assert a.id == b.id  # same url -> same id
    assert a.id != c.id
