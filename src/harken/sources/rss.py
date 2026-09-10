"""RSS/Atom source — point it at any feed (blogs, news, Google Alerts RSS).

Filters feed entries to those mentioning the query. Configure feeds via
``feeds=[...]`` or the ``HARKEN_RSS_FEEDS`` env var (comma-separated).
"""

from __future__ import annotations

from calendar import timegm
from datetime import datetime, timezone

import feedparser
import httpx

from harken.models import Mention
from harken.sources.base import FetchPage, Source, strip_html


class RSSSource(Source):
    name = "rss"
    label = "RSS"
    needs_config = True  # needs at least one feed URL

    def __init__(self, feeds: list[str] | None = None, **options):
        super().__init__(**options)
        self.feeds = feeds or []

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
                try:
                    resp = client.get(feed_url)
                    resp.raise_for_status()
                except httpx.HTTPError as exc:
                    # Feed URLs can contain credentials; identify the configured
                    # position and error class without serializing the raw URL.
                    status = f" HTTP {exc.response.status_code}" if isinstance(
                        exc, httpx.HTTPStatusError
                    ) else ""
                    errors.append(f"feed[{feed_number}] {type(exc).__name__}{status}")
                    continue
                parsed = feedparser.parse(resp.content)
                if not parsed.version:
                    errors.append(f"feed[{feed_number}] invalid RSS/Atom document")
                    continue
                if parsed.bozo:
                    errors.append(f"feed[{feed_number}] malformed RSS/Atom document")
                for entry in parsed.entries:
                    title = entry.get("title", "")
                    summary = entry.get("summary", "")
                    blob = f"{title} {summary}".casefold()
                    if q not in blob:
                        continue
                    created = _entry_time(entry)
                    mentions.append(
                        Mention(
                            source=self.name,
                            query=query,
                            author=entry.get("author"),
                            title=title or None,
                            text=strip_html(summary),
                            url=entry.get("link"),
                            created_at=created,
                        )
                    )
        return FetchPage(mentions[:limit], errors=errors)


def _entry_time(entry) -> datetime:
    for key in ("published_parsed", "updated_parsed"):
        t = entry.get(key)
        if t:
            # feedparser's struct_time is UTC; time.mktime() interprets it as
            # local time and shifts timestamps on non-UTC hosts.
            return datetime.fromtimestamp(timegm(t), tz=timezone.utc)
    return datetime.now(timezone.utc)
