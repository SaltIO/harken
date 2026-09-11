"""Sequential, bounded free-source collection with one RSS fetch per batch.

Run with ``python -m harken.batch --terms CMBS EDGAR ABS-EE --db harken.db``.
Scheduling and locking belong to the caller; each invocation performs one batch.
"""

from __future__ import annotations

import argparse
import json
import re
import time
from dataclasses import dataclass, field
from pathlib import Path

from harken.config import Config
from harken.observability import configure_logging
from harken.pipeline import Pipeline, TrackResult

ALLOWED_SOURCES = {"hackernews", "bluesky", "rss"}


@dataclass
class _BatchConfig(Config):
    # Shared by the fresh RSS adapter constructed for each Pipeline.track call.
    # The cache is scoped to this invocation, including failed feed responses.
    _rss_cache: dict = field(default_factory=dict, init=False, repr=False)
    _bluesky_cache: dict = field(default_factory=dict, init=False, repr=False)

    def source_options(self, name: str) -> dict:
        options = super().source_options(name)
        if name == "rss":
            options["batch_cache"] = self._rss_cache
        if name == "bluesky":
            options["batch_cache"] = self._bluesky_cache
        return options


def collect(
    terms: list[str], *, sources: list[str], db_path: str,
    rss_feeds: list[str] | None = None,
    profile_id: str | None = None, profile_version: str | None = None,
) -> list[TrackResult]:
    """Track one to three terms, preserving normal pipeline writes and errors."""
    terms = [term.strip() for term in terms]
    if not 1 <= len(terms) <= 3 or any(not term for term in terms):
        raise ValueError("Provide one to three nonempty terms")
    if len({term.casefold() for term in terms}) != len(terms):
        raise ValueError("Batch terms must be distinct")
    sources = [source.strip().lower() for source in sources]
    if not sources or not set(sources) <= ALLOWED_SOURCES or len(set(sources)) != len(sources):
        raise ValueError("Choose distinct sources from hackernews, bluesky, rss")
    # This entrypoint is collection-only even when invoked outside the controller.
    config = _BatchConfig(
        db_path=db_path, sources=sources, source_retries=0, per_source_limit=50,
        llm_provider="none", sentiment_analyzer="lexicon",
        webhook_url=None, email_to=[], email_from=None, smtp_host=None,
        smtp_username=None, smtp_password=None,
    )
    if rss_feeds is not None:
        config.rss_feeds = rss_feeds
    if "rss" in sources and (len(config.rss_feeds) != 1 or not config.rss_feeds[0].strip()):
        raise ValueError("RSS batches require exactly one feed URL")
    configure_logging(config.log_format, config.log_level)
    pipeline = Pipeline(config)
    try:
        return [pipeline.track(term, pages=1, profile_id=profile_id,
                               profile_version=profile_version) for term in terms]
    finally:
        pipeline.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--terms", nargs="+", required=True)
    parser.add_argument("--sources", default="hackernews,bluesky,rss")
    parser.add_argument("--db", required=True)
    parser.add_argument("--profile-id")
    parser.add_argument("--profile-version")
    parser.add_argument("--receipt", type=Path, help="Write completed batch timing and retry deadline input")
    args = parser.parse_args(argv)
    started_at = time.time()
    try:
        results = collect(args.terms, sources=args.sources.split(","), db_path=args.db,
                          profile_id=args.profile_id, profile_version=args.profile_version)
        # Adapters supply sanitized numeric delay markers. Honor the longest
        # requested delay across every source and term in the batch.
        retry_after = max((
            int(match.group(1)) for result in results
            for error in result.errors.values()
            for match in re.finditer(
                r"\bretry_after_seconds=(\d+)(?=$|[\s;])", error
            )
        ), default=0)
        receipt = {"started_at": started_at, "finished_at": time.time(),
                   "retry_after_seconds": retry_after,
                   "source_error_count": sum(len(result.errors) for result in results)}
        if args.receipt:
            args.receipt.parent.mkdir(parents=True, exist_ok=True)
            temporary = args.receipt.with_name(args.receipt.name + ".tmp")
            temporary.write_text(json.dumps(receipt) + "\n")
            temporary.replace(args.receipt)
    except (ValueError, OSError) as exc:
        parser.exit(2, f"harken batch: {exc}\n")
    print(json.dumps({"event": "batch_complete", **receipt, "results": [
        {"query": result.query, "fetched": result.fetched, "new": result.new,
         "by_source": result.by_source, "errors": result.errors}
        for result in results
    ]}))
    return int(any(result.errors or result.sentiment_error or result.analysis_error
                   for result in results))


if __name__ == "__main__":
    raise SystemExit(main())
