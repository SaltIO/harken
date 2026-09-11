"""Hacker News source via the public Algolia API — no key required.

This is the zero-config workhorse: it works on a clean clone with nothing set up.
"""

from __future__ import annotations

from datetime import datetime, timezone

from harken.models import Mention
from harken.sources.base import FetchPage, Source, strip_html

_API = "https://hn.algolia.com/api/v1/search_by_date"


class HackerNewsSource(Source):
    name = "hackernews"
    label = "Hacker News"
    needs_config = False

    def fetch(self, query: str, limit: int = 50) -> list[Mention]:
        return self.fetch_page(query, limit=limit).mentions

    def fetch_page(
        self,
        query: str,
        limit: int = 50,
        *,
        cursor: str | None = None,
        since: datetime | None = None,
    ) -> FetchPage:
        params = {
            "query": query,
            "tags": "(story,comment)",
            "hitsPerPage": min(limit, 100),
            "typoTolerance": "false",
        }
        boundaries = []
        if cursor:
            # Inclusive (<=) so items sharing the previous page's oldest second
            # are not skipped when the page cap splits that group; the store
            # de-duplicates the one-second overlap and the pipeline's bounded
            # page loop stops the (pathological) all-same-second case.
            boundaries.append(f"created_at_i<={int(cursor)}")
        if since:
            boundaries.append(f"created_at_i>{int(since.timestamp())}")
        if boundaries:
            params["numericFilters"] = ",".join(boundaries)
        with self._client() as client:
            resp = client.get(_API, params=params)
            resp.raise_for_status()
            data = resp.json()

        mentions: list[Mention] = []
        hits = data.get("hits", [])
        timestamps: list[int] = []
        for hit in hits:
            ts = hit.get("created_at_i")
            published = None
            if isinstance(ts, (int, float)) and not isinstance(ts, bool):
                try:
                    published = datetime.fromtimestamp(ts, tz=timezone.utc)
                    timestamps.append(int(ts))
                except (ValueError, OverflowError, OSError):
                    pass
            text = hit.get("title") or hit.get("story_title") or ""
            body = hit.get("comment_text") or hit.get("story_text") or ""
            normalized_body = strip_html(body)
            if query.casefold() not in f"{text} {normalized_body}".casefold():
                continue
            object_id = hit.get("objectID")
            created = published or datetime.now(timezone.utc)
            mentions.append(
                Mention(
                    source=self.name,
                    source_item_id=str(object_id) if object_id is not None else None,
                    query=query,
                    author=hit.get("author"),
                    title=text or None,
                    text=normalized_body,
                    url=f"https://news.ycombinator.com/item?id={object_id}"
                    if object_id
                    else hit.get("url"),
                    created_at=created,
                    published_at=published,
                    publication_provenance="created_at_i" if published else "unknown",
                    score=hit.get("points"),
                )
            )
        has_more = data.get("page", 0) + 1 < data.get("nbPages", 1)
        if "nbPages" not in data:
            has_more = len(hits) >= min(limit, 100)
        next_cursor = str(min(timestamps)) if has_more and timestamps else None
        return FetchPage(mentions, next_cursor, truncated=has_more)
