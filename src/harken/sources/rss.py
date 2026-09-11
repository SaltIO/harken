"""RSS/Atom source — point it at any feed (blogs, news, Google Alerts RSS).

Filters feed entries to those mentioning the query. Configure feeds via
``feeds=[...]`` or the ``HARKEN_RSS_FEEDS`` env var (comma-separated).
"""

from __future__ import annotations

from calendar import timegm
from datetime import datetime, timezone
from hashlib import sha256

import feedparser
import httpx

from harken.models import Mention
from harken.sources.base import FetchPage, Source, retry_after_seconds, strip_html


class RSSSource(Source):
    name = "rss"
    label = "RSS"
    needs_config = True  # needs at least one feed URL

    def __init__(self, feeds: list[str] | None = None, batch_cache: dict | None = None, **options):
        super().__init__(**options)
        self.feeds = feeds or []
        # Owned by one batch invocation; never retain feed responses globally.
        self.batch_cache = batch_cache

    def fetch(self, query: str, limit: int = 50) -> list[Mention]:
        page = self.fetch_page(query, limit)
        if page.errors and not page.mentions:
            raise RuntimeError("; ".join(page.errors))
        return page.mentions

    def fetch_page(
        self, query: str, limit: int = 50, *, cursor: str | None = None,
        since: datetime | None = None,
    ) -> FetchPage:
        if not self.feeds:
            raise RuntimeError("RSS requires at least one URL in HARKEN_RSS_FEEDS")
        q = query.casefold()
        mentions: list[Mention] = []
        errors: list[str] = []
        with self._client() as client:
            for feed_number, feed_url in enumerate(self.feeds, start=1):
                cached = self.batch_cache.get(feed_url) if self.batch_cache is not None else None
                if cached is None:
                    cached = self._fetch_feed(client, feed_url)
                    if self.batch_cache is not None:
                        self.batch_cache[feed_url] = cached
                parsed, error = cached
                if error:
                    errors.append(f"feed[{feed_number}] {error}")
                if parsed is None:
                    continue
                for entry in parsed.entries:
                    title = entry.get("title", "")
                    summary = entry.get("summary", "")
                    blob = f"{title} {summary}".casefold()
                    if q not in blob:
                        continue
                    created = _entry_time(entry)
                    published = _entry_date(entry, "published_parsed")
                    mentions.append(
                        Mention(
                            source=self.name,
                            source_id="rss:" + sha256(feed_url.encode()).hexdigest(),
                            source_item_id=entry.get("id") or entry.get("guid"),
                            query=query,
                            author=entry.get("author"),
                            title=title or None,
                            text=strip_html(summary),
                            url=entry.get("link"),
                            created_at=created,
                            published_at=published,
                            publication_provenance="source_published" if published else "unknown",
                            source_updated_at=_entry_date(entry, "updated_parsed"),
                        )
                    )
        return FetchPage(mentions[:limit], errors=errors, truncated=len(mentions) > limit)

    @staticmethod
    def _fetch_feed(
        client: httpx.Client, feed_url: str,
    ) -> tuple[feedparser.FeedParserDict | None, str | None]:
        try:
            resp = client.get(feed_url)
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            # Cache only a sanitized descriptor: feed URLs may contain secrets.
            status = f" HTTP {exc.response.status_code}" if isinstance(
                exc, httpx.HTTPStatusError
            ) else ""
            retry = None
            if isinstance(exc, httpx.HTTPStatusError):
                retry = retry_after_seconds(exc.response.headers.get("Retry-After"))
            marker = f" retry_after_seconds={retry}" if retry is not None else ""
            return None, f"{type(exc).__name__}{status}{marker}"
        parsed = feedparser.parse(resp.content)
        if not parsed.version:
            return None, "invalid RSS/Atom document"
        return parsed, "malformed RSS/Atom document" if parsed.bozo else None


def _entry_time(entry) -> datetime:
    """Legacy operational ordering time; not evidence of source publication."""
    for key in ("published_parsed", "updated_parsed"):
        parsed = _entry_date(entry, key)
        if parsed is not None:
            return parsed
    return datetime.now(timezone.utc)


def _entry_date(entry, key: str) -> datetime | None:
    # FeedParserDict.get aliases absent updated_parsed to published_parsed;
    # membership distinguishes an actual source update from that fallback.
    value = entry.get(key) if key in entry else None
    if value:
        # feedparser's struct_time is UTC, not the host's local timezone.
        return datetime.fromtimestamp(timegm(value), tz=timezone.utc)
    return None
