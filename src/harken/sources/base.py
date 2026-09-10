"""Source interface and shared HTTP helper."""

from __future__ import annotations

import html
import re
from dataclasses import dataclass, field
from datetime import datetime

import httpx

from harken.models import Mention

USER_AGENT = "harken/0.1 (+https://github.com/VladUZH/harken)"


@dataclass
class FetchPage:
    """One source page, next cursor, and any partial collection failures.

    Failed pages can retain useful mentions, but must not advance the source's
    success cursor. Errors should identify failures without exposing credentials.
    """

    mentions: list[Mention]
    next_cursor: str | None = None
    errors: list[str] = field(default_factory=list)


class Source:
    """Base class for a mention source.

    Subclasses set :attr:`name` and implement :meth:`fetch`. Sources should be
    resilient: a network error for one source must never sink the whole run
    (the pipeline isolates failures), but raising here is fine — the caller
    catches it.
    """

    name: str = "base"
    #: human-readable, shown in the dashboard
    label: str = "Base"
    #: whether the source needs configuration to do anything useful
    needs_config: bool = False

    def __init__(self, **options):
        self.options = options

    def fetch(self, query: str, limit: int = 50) -> list[Mention]:
        raise NotImplementedError

    def fetch_page(
        self,
        query: str,
        limit: int = 50,
        *,
        cursor: str | None = None,
        since: datetime | None = None,
    ) -> FetchPage:
        """Fetch one page.

        The default keeps older source plugins working. Cursor-aware adapters
        override this method; ``since`` is a best-effort incremental boundary.
        """
        return FetchPage(self.fetch(query, limit=limit))

    # -- helpers -------------------------------------------------------------
    def _client(self, **kwargs) -> httpx.Client:
        headers = {"User-Agent": self.options.get("user_agent") or USER_AGENT,
                   **kwargs.pop("headers", {})}
        return httpx.Client(headers=headers, timeout=15.0, **kwargs)


def strip_html(value: str) -> str:
    """Turn the small HTML fragments returned by feeds into readable text."""
    without_tags = re.sub(r"<[^>]+>", " ", value or "")
    return re.sub(r"\s+", " ", html.unescape(without_tags)).strip()
