"""Core data models for Harken.

Everything flowing through the pipeline is normalised into a :class:`Mention`.
Sources produce them, analyzers annotate them, the store persists them, and the
web dashboard renders them.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from enum import Enum

from pydantic import BaseModel, field_validator


class Sentiment(str, Enum):
    """Coarse sentiment label. Kept deliberately small — useful, not precise."""

    POSITIVE = "positive"
    NEUTRAL = "neutral"
    NEGATIVE = "negative"


class Mention(BaseModel):
    """A single thing someone said, somewhere, that matched a tracked query.

    ``id`` fingerprints source-scoped native identity, falling back to URL or
    content. The store preserves an older canonical ID when identity can be
    safely reconciled, and associates each matching query separately.
    """

    id: str = ""
    source_id: str = ""
    source_item_id: str | None = None
    author_id: str | None = None
    source: str  # e.g. "hackernews", "reddit", "mastodon"
    query: str  # the tracked term this mention matched
    author: str | None = None
    title: str | None = None
    text: str = ""
    url: str | None = None
    created_at: datetime
    # created_at remains the operational ordering timestamp. Only published_at
    # with provenance establishes publication; undated RSS must stay unknown.
    published_at: datetime | None = None
    publication_provenance: str = "unknown"
    indexed_at: datetime | None = None
    source_updated_at: datetime | None = None
    fetched_at: datetime | None = None
    body_expired: bool = False
    score: int | None = None  # upvotes / points / favourites, source-dependent

    # Populated by analyzers (None until analysed).
    sentiment: Sentiment | None = None
    sentiment_score: float | None = None  # signed, roughly [-1, 1]
    theme: str | None = None

    @field_validator("created_at")
    @classmethod
    def _ensure_tz_aware(cls, value: datetime) -> datetime:
        """Treat a tz-naive source timestamp as UTC.

        Some source APIs (or non-conformant federated servers) return
        offset-less timestamps. Left naive, they later blow up when compared
        against an aware ``datetime`` (alert bucketing) or get silently shifted
        by the host's local offset when normalised for cursor bounds. Anchoring
        them to UTC here keeps every ``created_at`` aware and comparable.
        """
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value

    def model_post_init(self, __context) -> None:  # noqa: D401
        if not self.source_id:
            self.source_id = self.source
        if not self.id:
            self.id = self.compute_id()

    @property
    def content(self) -> str:
        """Title + body, the text analyzers actually read."""
        parts = [p for p in (self.title, self.text) if p]
        return "\n".join(parts).strip()

    def compute_id(self) -> str:
        basis = (f"{self.source_id}:{self.source_item_id}" if self.source_item_id
                 else f"{self.source_id}:{self.url or self.content}")
        return hashlib.sha256(basis.encode("utf-8")).hexdigest()


def utcnow() -> datetime:
    return datetime.now(timezone.utc)
