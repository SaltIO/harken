"""SQLite store tests — temp DB, no network."""

import json
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from harken.models import Mention, Sentiment
from harken.store import CursorError, Store


def test_object_changes_keep_native_identity_separate_from_matches(tmp_path):
    path = tmp_path / "objects.db"
    first = Mention(source="bluesky", source_item_id="at://did:plc:a/app.bsky.feed.post/1",
                    author_id="did:plc:a", author="old.test", query="one", text="body",
                    created_at=datetime(2020, 1, 1, tzinfo=timezone.utc))
    other_match = first.model_copy(update={"query": "two"})
    with Store(path) as db:
        db.upsert([first, other_match])
        page = db.observation_changes()
        assert len({r["id"] for r in page["rows"]}) == 1
        assert len({r["revision"] for r in page["rows"]}) == 1
        assert page["rows"][-1]["queries"] == ["one", "two"]
        assert all("text" not in row and "title" not in row for row in page["rows"])
        original = db.observation(first.id)
        db.upsert([other_match])
        assert db.observation_changes(cursor=page["checkpoint"])["rows"] == []
        edited = first.model_copy(update={"text": "changed", "author": "new.test"})
        db.upsert([edited])
        change = db.observation_changes(cursor=page["checkpoint"])["rows"]
        assert len(change) == 1 and change[0]["id"] == first.id
        assert change[0]["revision"] != original["revision"]
        assert change[0]["fetched_at"] == original["fetched_at"]
        assert {m.text for m in db.mentions()} == {"changed"}
    with Store(path) as db:
        assert db.observation(first.id)["queries"] == ["one", "two"]
        assert db.observation_changes(cursor=page["checkpoint"])["rows"] == change


def test_changes_finite_upper_bound_scope_epoch_and_empty_resume(tmp_path):
    with Store(tmp_path / "finite.db") as db:
        baseline = db.observation_changes()
        assert baseline["rows"] == [] and baseline["next_cursor"] is None
        items = [mk(str(i), url=f"https://example.test/{i}") for i in range(3)]
        db.upsert(items)
        first = db.observation_changes(cursor=baseline["checkpoint"], limit=1)
        assert first["has_more"]
        db.upsert([mk("arrives after snapshot", url="https://example.test/new")])
        second = db.observation_changes(cursor=first["next_cursor"], limit=100)
        assert len(second["rows"]) == 2 and second["upper_seq"] == first["upper_seq"]
        assert second["next_cursor"] is None
        next_run = db.observation_changes(cursor=second["checkpoint"])
        assert len(next_run["rows"]) == 1
        with pytest.raises(CursorError, match="scope"):
            db.observation_changes(cursor=first["checkpoint"], source="rss")
        with pytest.raises(ValueError, match="together"):
            db.observation_changes(profile_id="optional")
        bound = db.observation_changes(profile_id="optional", profile_version="1")
        assert len(bound["rows"]) == 4  # Context binding does not silently filter objects.
        with pytest.raises(CursorError, match="scope"):
            db.observation_changes(cursor=bound["checkpoint"], profile_id="optional", profile_version="2")
        with Store(tmp_path / "other.db") as other:
            with pytest.raises(CursorError) as mismatch:
                other.observation_changes(cursor=first["checkpoint"])
            assert mismatch.value.code == "cursor_epoch_mismatch"
        for token in ("not-a-cursor", "!", "x" * 5000):
            with pytest.raises(CursorError):
                db.observation_changes(cursor=token)


def test_failed_upsert_rolls_back_objects_matches_and_changes(tmp_path, monkeypatch):
    with Store(tmp_path / "atomic.db") as db:
        old = mk("old", url="https://example.test/old")
        db.upsert([old])
        checkpoint = db.observation_changes()["checkpoint"]
        original = db._sync_observation
        def fail_after_event(cur, item_id, now):
            original(cur, item_id, now)
            raise RuntimeError("injected failure before commit")
        monkeypatch.setattr(db, "_sync_observation", fail_after_event)
        with pytest.raises(RuntimeError, match="before commit"):
            db.upsert([mk("new", url="https://example.test/new")])
        assert len(db.mentions()) == 1
        assert db.observation_changes(cursor=checkpoint)["rows"] == []
        monkeypatch.setattr(db, "_sync_observation", original)
        db.upsert([mk("new", url="https://example.test/new")])
        assert len(db.observation_changes(cursor=checkpoint)["rows"]) == 1


def test_expiry_and_removal_emit_scoped_body_free_changes(tmp_path):
    with Store(tmp_path / "expiry.db") as db:
        item = mk("sensitive body", source="rss", url="https://example.test/post")
        db.upsert([item])
        initial = db.observation(item.id)
        checkpoint = db.observation_changes(query="acme")["checkpoint"]
        first_seen = datetime.fromisoformat(initial["fetched_at"])
        db.retention(now=first_seen + timedelta(days=31), apply=True)
        expired = db.observation_changes(cursor=checkpoint, query="acme")
        assert [r["availability"] for r in expired["rows"]] == ["expired"]
        assert db.observation(item.id)["text"] is None
        db.upsert([item.model_copy(update={"query": "new-query"})])
        assert all(m.body_expired and not m.text for m in db.mentions())
        latest_checkpoint = db.observation_changes(query="acme")["checkpoint"]
        # Unprocessed history expired; an already-consumed boundary can still resume.
        db.retention(now=first_seen + timedelta(days=122), apply=True)
        with pytest.raises(CursorError) as expired_cursor:
            db.observation_changes(cursor=checkpoint, query="acme")
        assert expired_cursor.value.code == "cursor_expired"
        assert db.observation_changes(cursor=latest_checkpoint, query="acme")["rows"][-1]["kind"] == "removed"
        retained = db.observation_changes(query="acme")
        assert retained["rows"][-1]["kind"] == "removed"
        assert retained["rows"][-1]["url"] is None
        assert db.observation(item.id)["availability"] == "removed"
        assert "sensitive body" not in str(retained)


def test_query_scoped_prune_delivers_membership_removal(tmp_path):
    with Store(tmp_path / "prune.db") as db:
        item = mk("body", url="https://example.test/1")
        db.upsert([item, item.model_copy(update={"query": "other"})])
        checkpoint = db.observation_changes(query="acme")["checkpoint"]
        db.delete_before(datetime(2027, 1, 1, tzinfo=timezone.utc), query="acme")
        rows = db.observation_changes(query="acme", cursor=checkpoint)["rows"]
        assert len(rows) == 1 and rows[0]["queries"] == ["other"]
        assert rows[0]["availability"] == "available"


def test_scoped_retention_preserves_unselected_source_history(tmp_path):
    with Store(tmp_path / "scoped.db") as db:
        db.upsert([mk("rss body", source="rss"), mk("hn body")])
        first = db.observation_changes(limit=1)
        db.retention(now=datetime.now(timezone.utc) + timedelta(days=100), source="rss", apply=True)
        remaining = db.observation_changes(cursor=first["checkpoint"])
        assert any(r["source"] == "hackernews" for r in remaining["rows"])
        assert len(db.mentions(source="hackernews")) == 1


def test_explicit_epoch_rotation_invalidates_restored_history_cursors(tmp_path):
    with Store(tmp_path / "epoch.db") as db:
        db.upsert([mk("retained body")])
        old = db.observation_changes()
        with pytest.raises(CursorError):
            db.rotate_epoch("stale expectation")
        assert db.store_epoch == old["store_epoch"]
        new_epoch = db.rotate_epoch(old["store_epoch"])
        assert new_epoch != old["store_epoch"]
        assert len(db.mentions()) == 1
        with pytest.raises(CursorError) as mismatch:
            db.observation_changes(cursor=old["checkpoint"])
        assert mismatch.value.code == "cursor_epoch_mismatch"


@pytest.mark.parametrize("source,source_id,url,native_id", [
    ("hackernews", "hackernews", "https://news.ycombinator.com/item?id=123", "123"),
    ("rss", "rss:known-feed", "https://example.test/post", "urn:example:guid"),
])
def test_legacy_migration_and_native_refetch_preserve_canonical_id(
    tmp_path, source, source_id, url, native_id,
):
    path = tmp_path / "native-migration.db"
    with sqlite3.connect(path) as old:
        old.execute("""CREATE TABLE mentions (
            id TEXT NOT NULL, source TEXT NOT NULL, query TEXT NOT NULL,
            author TEXT, title TEXT, text TEXT, url TEXT, created_at TEXT NOT NULL,
            score INTEGER, sentiment TEXT, sentiment_score REAL, theme TEXT,
            fetched_at TEXT NOT NULL, source_id TEXT, PRIMARY KEY(id,query))""")
        old.execute("INSERT INTO mentions(id,source,source_id,query,text,url,created_at,fetched_at) "
                    "VALUES(?,?,?,?,?,?,?,?)", ("retained-legacy-id", source, source_id, "acme",
                    "old body", url, "2026-01-01T00:00:00+00:00", "2026-09-01T00:00:00+00:00"))
    with Store(path) as db:
        old_ref = db.observation("retained-legacy-id")
        assert old_ref["published_at"] is None
        if source == "hackernews":
            assert old_ref["source_item_id"] == native_id
        fresh = Mention(source=source, source_id=source_id, source_item_id=native_id,
                        url=url, query="acme", text="new body", created_at=datetime.now(timezone.utc))
        assert fresh.id != "retained-legacy-id"
        db.resolve_mentions([fresh])
        assert fresh.id == "retained-legacy-id"
        assert db.upsert([fresh]) == 0
        # The native linkage survives a display URL change and another query.
        fresh = Mention(source=source, source_id=source_id, source_item_id=native_id,
                        url="https://example.test/changed-display", query="second", text="edited",
                        created_at=datetime.now(timezone.utc))
        assert db.upsert([fresh]) == 1
        assert {m.id for m in db.mentions()} == {"retained-legacy-id"}
        assert db.observation(fresh.id)["fetched_at"] == old_ref["fetched_at"]
        assert len({r["id"] for r in db.observation_changes()["rows"]}) == 1


def test_legacy_bluesky_handle_and_unknown_rss_feed_are_not_guessed(tmp_path):
    with Store(tmp_path / "uncertain.db") as db:
        for source in ("bluesky", "rss"):
            legacy = mk("old", source=source, url="https://example.test/post")
            db.upsert([legacy])
            fresh = Mention(source=source, source_id="rss:known-feed" if source == "rss" else source,
                            source_item_id="native-id", url=legacy.url, query="acme", text="fresh",
                            created_at=datetime.now(timezone.utc))
            assert db.upsert([fresh]) == 1
            assert db.observation(legacy.id)["identity_provenance"] == "legacy_uncertain"
            assert fresh.id != legacy.id


def test_identity_expiry_scrubs_recent_journal_revisions_and_keeps_cursor_removal(tmp_path):
    with Store(tmp_path / "scrub.db") as db:
        item = Mention(source="rss", source_id="rss:feed", source_item_id="native-private-id",
                       query="acme", author="private-author", author_id="private-author-id",
                       url="https://private.example/post", text="private-body",
                       created_at=datetime.now(timezone.utc))
        db.upsert([item])
        first_seen = datetime.fromisoformat(db.observation(item.id)["fetched_at"])
        db.upsert([item.model_copy(update={"text": "edited"})])
        db.retention(now=first_seen + timedelta(days=31), source="rss", apply=True)
        checkpoint = db.observation_changes(query="acme")["checkpoint"]
        db.retention(now=first_seen + timedelta(days=91), source="rss", apply=True)
        rows = db.observation_changes()["rows"]
        assert len(rows) == 4  # Scoped retention preserves sequence history.
        assert all(r["availability"] == "removed" for r in rows)
        assert all(r["url"] is None and r["author"] is None and r["source_item_id"] is None for r in rows)
        assert "private" not in json.dumps(rows)
        assert db.observation_changes(query="acme", cursor=checkpoint)["rows"][0]["kind"] == "removed"
        assert db.upsert([item]) == 0
        assert db.observation(item.id)["availability"] == "removed"
        assert db.mentions() == []


@pytest.mark.parametrize("changed", [
    {"source_id": "rss:other-feed", "source_ids": ["rss:other-feed"]},
    {"profile_id": "profile", "profile_version": "2"},
    {"method": "backfill"}, {"effective_since": "2026-01-01T00:00:00+00:00"},
    {"row_limit_per_page": 2},
])
def test_attempt_last_success_requires_the_actual_scope(tmp_path, changed):
    now = datetime.now(timezone.utc).isoformat()
    value = {"source": "rss", "source_id": "rss:feed", "source_ids": ["rss:feed"],
             "query": "acme", "profile_id": "profile", "profile_version": "1",
             "method": "rss_feed", "effective_since": None, "row_limit_per_page": 1,
             "finished_at": now, "landed_at": now, "acquisition": "completed",
             "landing": "stored", "execution": "completed"}
    with Store(tmp_path / "attempt-scope.db") as db:
        good = db.record_attempt(value)
        assert good["last_success_at"] == now
        failed = {**value, "acquisition": "failed", "landing": "not_applicable", "execution": "failed"}
        assert db.record_attempt(failed)["last_success_at"] == now
        different = db.record_attempt({**failed, **changed})
        assert different["last_success_at"] is None
        assert different["attempt_scope_digest"] != good["attempt_scope_digest"]


def mk(text, sentiment=None, source="hackernews", query="acme", url=None):
    return Mention(
        source=source,
        query=query,
        text=text,
        url=url,
        created_at=datetime(2026, 6, 1, tzinfo=timezone.utc),
        sentiment=sentiment,
    )


def test_upsert_and_dedupe(tmp_path):
    db = Store(tmp_path / "t.db")
    new1 = db.upsert([mk("hello", url="https://x/1"), mk("world", url="https://x/2")])
    assert new1 == 2
    # re-inserting the same urls adds no new rows
    new2 = db.upsert([mk("hello", url="https://x/1")])
    assert new2 == 0
    assert len(db.mentions(query="acme")) == 2
    db.close()


def test_fixed_multisource_replay_preserves_first_fetch_across_restart(tmp_path):
    path = tmp_path / "replay.db"
    batch = [mk("shared", query=q, source=s, url=f"https://{s}/1")
             for s in ("rss", "hackernews") for q in ("CMBS", "EDGAR")]
    for mention in batch:
        if mention.source == "rss":
            mention.source_id = "rss:stable-feed-fingerprint"
        else:
            mention.published_at = mention.created_at
            mention.publication_provenance = "fixture_source_timestamp"
    with Store(path) as db:
        assert db.upsert(batch) == 4
        first = {(m.id, m.query): m.fetched_at for m in db.mentions(limit=None)}
        assert all(first.values())
    with Store(path) as db:
        assert db.upsert(batch) == 0
        rows = db.mentions(limit=None)
        assert {(m.id, m.query): m.fetched_at for m in rows} == first
        assert all(m.published_at is None for m in rows if m.source == "rss")
        assert all(m.source_id == "rss:stable-feed-fingerprint"
                   for m in rows if m.source == "rss")
        assert all(m.published_at == m.created_at for m in rows if m.source == "hackernews")
        collected = []
        cursor = None
        while page := db.mention_page(after=cursor, limit=1):
            m = page[0]
            collected.append((m.id, m.query))
            cursor = (m.fetched_at.isoformat(), m.id, m.query)
        assert len(collected) == len(set(collected)) == 4
        assert set(collected) == set(first)
        since = min(first.values())
        assert len(db.mention_page(source="rss", since=since)) == 2
        assert db.mention_page(since=since + timedelta(days=1)) == []


def test_v2_migration_preserves_ambiguous_rss_dates(tmp_path):
    path = tmp_path / "legacy.db"
    with sqlite3.connect(path) as old:
        old.execute("""CREATE TABLE mentions (
            id TEXT NOT NULL, source TEXT NOT NULL, query TEXT NOT NULL,
            author TEXT, title TEXT, text TEXT, url TEXT, created_at TEXT NOT NULL,
            score INTEGER, sentiment TEXT, sentiment_score REAL, theme TEXT,
            fetched_at TEXT NOT NULL, PRIMARY KEY(id,query))""")
        for source in ("rss", "hackernews"):
            old.execute("INSERT INTO mentions(id,source,query,created_at,fetched_at) "
                        "VALUES(?,?,?,?,?)", (source, source, "CMBS",
                        "2026-06-01T00:00:00+00:00", "2026-09-01T00:00:00+00:00"))
        old.execute("PRAGMA user_version=2")
    with Store(path) as db:
        rows = {m.source: m for m in db.mentions()}
        assert rows["rss"].published_at is None
        assert rows["rss"].publication_provenance == "legacy_unknown"
        assert rows["rss"].source_id == "rss"  # historical feed identity is unavailable
        assert rows["hackernews"].published_at is None
        assert rows["hackernews"].publication_provenance == "legacy_unknown"
        assert rows["rss"].fetched_at == datetime(2026, 9, 1, tzinfo=timezone.utc)


def test_retention_preview_expiry_replay_and_metadata_removal(tmp_path):
    path = tmp_path / "retention.db"
    rss = mk("old body", source="rss", url="https://rss/1")
    rss.title = "old title"
    rss.theme = "body-derived theme"
    with Store(path) as db:
        db.upsert([rss, mk("other source", url="https://hn/1")])
        db.enqueue_alerts([rss], "target")
        db.activate_threshold_alert("acme", "spike", "target", "old body",
                                    {"text": "old body"}, cooldown_hours=0)
        original = db.mentions(source="rss")[0]
        later = original.fetched_at + timedelta(days=31)
        assert db.retention(now=later, source="rss") == {
            "bodies": 1, "identities": 0, "alert_payloads": 1}
        assert db.mentions(source="rss")[0].text == "old body"
        assert db.retention(now=later, source="rss", apply=True) == {
            "bodies": 1, "identities": 0, "alert_payloads": 1}
        assert db.upsert([rss]) == 0
        expired = db.mentions(source="rss")[0]
        assert expired.text == "" and expired.title is None and expired.body_expired
        assert expired.theme is None
        assert db._conn.execute("SELECT COUNT(*) FROM threshold_alerts").fetchone()[0] == 0
        assert db.pending_alerts("acme", "target")[0].text == ""
        assert expired.fetched_at == original.fetched_at
        assert db.mentions(source="hackernews")[0].text == "other source"
        assert db.retention(now=original.fetched_at + timedelta(days=91), source="rss") == {
            "bodies": 0, "identities": 1, "alert_payloads": 0}
        assert len(db.mentions()) == 2
        assert db.retention(now=original.fetched_at + timedelta(days=91), source="rss",
                            apply=True) == {"bodies": 0, "identities": 1, "alert_payloads": 0}
        assert len(db.mentions()) == 1


def test_upsert_updates_sentiment(tmp_path):
    db = Store(tmp_path / "t.db")
    db.upsert([mk("buggy", url="https://x/1")])
    db.upsert([mk("buggy", url="https://x/1", sentiment=Sentiment.NEGATIVE)])
    got = db.mentions(query="acme")[0]
    assert got.sentiment is Sentiment.NEGATIVE
    db.close()


def test_upsert_refreshes_mutable_source_fields(tmp_path):
    db = Store(tmp_path / "t.db")
    first = mk("old text", url="https://x/1")
    db.upsert([first])
    refreshed = mk("edited text", url="https://x/1")
    refreshed.score = 42
    refreshed.created_at += timedelta(days=1)
    db.upsert([refreshed])
    got = db.mentions(query="acme")[0]
    assert got.text == "edited text"
    assert got.score == 42
    assert got.created_at == refreshed.created_at
    db.close()


def test_pre_cluster_upsert_preserves_theme_but_post_cluster_can_clear_it(tmp_path):
    db = Store(tmp_path / "t.db")
    themed = mk("fast tool", url="https://x/1")
    themed.theme = "performance"
    db.upsert([themed])
    # Pre-cluster re-ingest (theme unknown) must NOT wipe the stored label.
    db.upsert([mk("fast tool", url="https://x/1")], update_theme=False)
    assert db.mentions(query="acme")[0].theme == "performance"
    # Post-cluster write (default) is authoritative and may clear a de-clustered label.
    db.upsert([mk("fast tool", url="https://x/1")])
    assert db.mentions(query="acme")[0].theme is None
    db.close()


def test_timeseries_reports_zero_not_null_for_all_null_sentiment_day(tmp_path):
    db = Store(tmp_path / "t.db")
    db.upsert([mk("no sentiment", url="https://x/1")])  # sentiment defaults to None
    row = db.timeseries(query="acme")[0]
    assert row["positive"] == 0 and row["negative"] == 0
    assert row["neutral"] == 1 and row["total"] == 1
    db.close()


def test_same_mention_can_belong_to_multiple_queries(tmp_path):
    db = Store(tmp_path / "t.db")
    assert db.upsert([mk("shared", query="acme", url="https://x/shared")]) == 1
    assert db.upsert([mk("shared", query="beta", url="https://x/shared")]) == 1
    assert len(db.mentions(query="acme")) == 1
    assert len(db.mentions(query="beta")) == 1
    db.close()


def test_filter_by_sentiment_and_source(tmp_path):
    db = Store(tmp_path / "t.db")
    db.upsert(
        [
            mk("a", url="u1", sentiment=Sentiment.POSITIVE, source="hackernews"),
            mk("b", url="u2", sentiment=Sentiment.NEGATIVE, source="reddit"),
        ]
    )
    assert len(db.mentions(sentiment=Sentiment.POSITIVE)) == 1
    assert db.mentions(source="reddit")[0].text == "b"
    db.close()


def test_summary_aggregates(tmp_path):
    db = Store(tmp_path / "t.db")
    db.upsert(
        [
            mk("a", url="u1", sentiment=Sentiment.POSITIVE),
            mk("b", url="u2", sentiment=Sentiment.POSITIVE),
            mk("c", url="u3", sentiment=Sentiment.NEGATIVE, source="reddit"),
        ]
    )
    s = db.summary(query="acme")
    assert s["total"] == 3
    assert s["by_sentiment"]["positive"] == 2
    assert s["by_source"]["reddit"] == 1
    assert s["by_day"]["2026-06-01"] == 3
    db.close()


def test_timeseries_and_net_sentiment(tmp_path):
    db = Store(tmp_path / "t.db")
    db.upsert(
        [
            mk("a", url="u1", sentiment=Sentiment.POSITIVE),
            mk("b", url="u2", sentiment=Sentiment.POSITIVE),
            mk("c", url="u3", sentiment=Sentiment.NEGATIVE),
            mk("d", url="u4", sentiment=Sentiment.NEUTRAL),
        ]
    )
    ts = db.timeseries(query="acme")
    assert len(ts) == 1
    day = ts[0]
    assert day["date"] == "2026-06-01"
    assert day["positive"] == 2 and day["negative"] == 1 and day["neutral"] == 1
    assert day["total"] == 4
    # net = (2 - 1) / 4 = 0.25
    assert db.net_sentiment(query="acme") == 0.25
    db.close()


def test_unscored_rows_are_consistently_presented_as_neutral(tmp_path):
    db = Store(tmp_path / "t.db")
    db.upsert([mk("not analyzed yet", url="u1")])
    assert db.summary("acme")["by_sentiment"] == {"neutral": 1}
    assert db.timeseries("acme")[0]["neutral"] == 1
    db.close()


def test_theme_counts_cover_all_stored_rows(tmp_path):
    db = Store(tmp_path / "t.db")
    rows = [mk(str(i), url=f"u{i}") for i in range(205)]
    for row in rows:
        row.theme = "reliability"
    db.upsert(rows)
    assert db.themes(query="acme") == [{"label": "reliability", "count": 205}]
    assert len(db.mentions(query="acme", limit=None)) == 205
    db.close()


def test_online_backup_is_consistent_and_refuses_accidental_overwrite(tmp_path):
    db = Store(tmp_path / "source.db")
    db.upsert([mk("preserved", url="u1")])
    target = db.backup(tmp_path / "backup.db")
    with Store(target) as backup:
        assert backup.summary("acme")["total"] == 1
    with pytest.raises(FileExistsError):
        db.backup(target)
    db.backup(target, overwrite=True)
    db.close()


def test_retention_removes_old_mentions_and_matching_alert_state(tmp_path):
    db = Store(tmp_path / "retention.db")
    old = mk("old", url="old")
    recent = mk("recent", url="recent")
    recent.created_at = datetime(2026, 7, 20, tzinfo=timezone.utc)
    db.upsert([old, recent])
    db.enqueue_alerts([old, recent], "target")
    cutoff = datetime(2026, 7, 1, tzinfo=timezone.utc)
    assert db.count_before(cutoff) == 1
    assert db.delete_before(cutoff) == 1
    assert [row.text for row in db.mentions(limit=None)] == ["recent"]
    assert db.pending_alert_count("acme", "target") == 1
    db.close()


def test_operational_stats_include_alert_state(tmp_path):
    db = Store(tmp_path / "stats.db")
    row = mk("negative", url="u1", sentiment=Sentiment.NEGATIVE)
    db.upsert([row])
    db.enqueue_alerts([row], "target")
    assert db.operational_stats() == {
        "mentions": 1,
        "queries": 1,
        "alerts_pending": 1,
        "alerts_delivered": 0,
        "threshold_alerts_pending": 0,
        "threshold_alerts_delivered": 0,
    }
    db.mark_alerts_delivered("acme", [row.id], "target")
    assert db.operational_stats()["alerts_delivered"] == 1
    db.close()


def test_source_metrics_accumulate_durably_by_source(tmp_path):
    path = tmp_path / "metrics.db"
    first_at = datetime(2026, 7, 20, tzinfo=timezone.utc)
    second_at = first_at + timedelta(minutes=5)
    with Store(path) as db:
        db.record_source_metric(
            "hackernews",
            duration_seconds=0.25,
            fetched=4,
            pages=1,
            retries=0,
            now=first_at,
        )
        db.record_source_metric(
            "hackernews",
            duration_seconds=1.5,
            fetched=0,
            pages=0,
            retries=2,
            error="HTTPStatusError: upstream unavailable",
            now=second_at,
        )
        db.record_source_metric(
            "bluesky",
            duration_seconds=0.5,
            fetched=2,
            pages=1,
            retries=0,
            now=second_at,
        )

    with Store(path) as reopened:
        rows = {row["source"]: row for row in reopened.source_metrics()}
    hackernews = rows["hackernews"]
    assert hackernews["scans_total"] == 2
    assert hackernews["errors_total"] == 1
    assert hackernews["fetched_total"] == 4
    assert hackernews["pages_total"] == 1
    assert hackernews["retries_total"] == 2
    assert hackernews["duration_seconds_total"] == pytest.approx(1.75)
    assert hackernews["last_duration_seconds"] == pytest.approx(1.5)
    assert hackernews["last_success"] == 0
    assert hackernews["last_success_at"] == first_at.isoformat()
    assert hackernews["last_error_at"] == second_at.isoformat()
    assert rows["bluesky"]["last_success"] == 1


def test_source_metrics_reject_negative_values(tmp_path):
    with Store(tmp_path / "metrics.db") as db:
        with pytest.raises(ValueError, match="must not be negative"):
            db.record_source_metric(
                "hackernews", duration_seconds=-1, fetched=0, pages=0, retries=0
            )


def test_tracking_persists_empty_keywords_and_selected_sources(tmp_path):
    db = Store(tmp_path / "tracking.db")
    db.save_tracking("No Results Yet", ["hackernews", "bluesky", "hackernews"])
    assert db.queries() == ["No Results Yet"]
    assert db.tracking("No Results Yet")["sources"] == ["hackernews", "bluesky"]
    assert db.summary("No Results Yet")["total"] == 0
    assert db.operational_stats()["queries"] == 1
    db.close()


def test_named_projects_group_keywords_and_report_aggregates(tmp_path):
    with Store(tmp_path / "projects.db") as db:
        acme_positive = mk("great", query="acme", url="acme", sentiment=Sentiment.POSITIVE)
        beta_negative = mk("broken", query="beta", url="beta", sentiment=Sentiment.NEGATIVE)
        outside = mk("outside", query="other", url="other", sentiment=Sentiment.NEUTRAL)
        beta_negative.theme = "reliability"
        db.upsert([acme_positive, beta_negative, outside])

        default = db.project(1)
        assert default["name"] == "Default"
        assert set(default["queries"]) == {"acme", "beta", "other"}

        project = db.create_project("  Product Suite  ")
        assert project["name"] == "Product Suite"
        assert db.add_query_to_project(project["id"], "acme")
        assert db.add_query_to_project(project["id"], "beta")
        assert not db.add_query_to_project(project["id"], "beta")

        refreshed = db.project(project["id"])
        assert set(refreshed["queries"]) == {"acme", "beta"}
        assert refreshed["query_count"] == 2
        assert refreshed["mention_count"] == 2
        assert db.summary(project_id=project["id"])["total"] == 2
        assert db.net_sentiment(project_id=project["id"]) == 0.0
        assert len(db.timeseries(project_id=project["id"])) == 1
        assert db.themes(project_id=project["id"])[0] == {
            "label": "reliability",
            "count": 1,
        }
        assert {row.query for row in db.mentions(project_id=project["id"])} == {
            "acme",
            "beta",
        }

        assert db.remove_query_from_project(project["id"], "beta")
        assert db.summary(project_id=project["id"])["total"] == 1
        assert db.summary("beta")["total"] == 1
        assert db.delete_project(project["id"])
        assert db.project(project["id"]) is None
        assert db.summary("acme")["total"] == 1


def test_project_names_are_unique_and_default_is_protected(tmp_path):
    with Store(tmp_path / "projects.db") as db:
        db.create_project("Platform")
        with pytest.raises(ValueError, match="already exists"):
            db.create_project("platform")
        with pytest.raises(ValueError, match="Default"):
            db.delete_project(1)
        with pytest.raises(ValueError, match="Default"):
            db.remove_query_from_project(1, "acme")
        with pytest.raises(ValueError, match="unknown project"):
            db.save_tracking("acme", ["hackernews"], project_id=999)


def test_project_scoped_tracking_does_not_implicitly_join_default(tmp_path):
    with Store(tmp_path / "projects.db") as db:
        project = db.create_project("Focused")
        db.save_tracking("new-keyword", ["hackernews"], project_id=project["id"])
        db.save_tracking("new-keyword", ["bluesky"])
        assert db.queries(project_id=project["id"]) == ["new-keyword"]
        assert "new-keyword" not in db.queries(project_id=1)


def test_source_state_keeps_forward_and_backfill_cursors_separate(tmp_path):
    db = Store(tmp_path / "cursors.db")
    first = mk("recent", url="recent")
    db.save_tracking("acme", ["hackernews"])
    db.upsert([first])
    db.record_source_success(
        "acme", "hackernews", [first], mode="incremental", next_cursor="older-1"
    )
    state = db.source_state("acme", "hackernews")
    assert state["backfill_cursor"] == "older-1"
    assert state["incremental_cursor"] is None
    assert not state["backfill_complete"]

    newer = mk("newer", url="newer")
    newer.created_at += timedelta(days=1)
    db.upsert([newer])
    db.record_source_success(
        "acme",
        "hackernews",
        [newer],
        mode="incremental",
        next_cursor="forward-2",
        incremental_since=first.created_at,
    )
    state = db.source_state("acme", "hackernews")
    assert state["incremental_cursor"] == "forward-2"
    assert state["incremental_since"] == first.created_at.isoformat()
    assert state["backfill_cursor"] == "older-1"

    db.record_source_success(
        "acme",
        "hackernews",
        [],
        mode="incremental",
        next_cursor=None,
        incremental_since=first.created_at,
    )
    db.record_source_success("acme", "hackernews", [], mode="backfill", next_cursor=None)
    state = db.source_state("acme", "hackernews")
    assert state["incremental_cursor"] is None
    assert state["incremental_since"] is None
    assert state["backfill_cursor"] is None
    assert state["backfill_complete"]
    db.close()


def test_opens_and_migrates_v01_database(tmp_path):
    path = tmp_path / "legacy.db"
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE mentions (
            id TEXT PRIMARY KEY, source TEXT NOT NULL, query TEXT NOT NULL,
            author TEXT, title TEXT, text TEXT, url TEXT, created_at TEXT NOT NULL,
            score INTEGER, sentiment TEXT, sentiment_score REAL, theme TEXT,
            fetched_at TEXT NOT NULL
        );
        INSERT INTO mentions VALUES (
            'legacy', 'hackernews', 'acme', NULL, NULL, 'hello', 'https://x/1',
            '2026-06-01T00:00:00+00:00', NULL, 'neutral', 0.0, NULL,
            '2026-06-01T00:00:00+00:00'
        );
        """
    )
    conn.close()

    db = Store(path)
    assert db.summary("acme")["total"] == 1
    primary_key = [
        row["name"]
        for row in sorted(
            db._conn.execute("PRAGMA table_info(mentions)"), key=lambda row: row["pk"]
        )
        if row["pk"]
    ]
    assert primary_key == ["id", "query"]
    assert db._conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'alert_outbox'"
    ).fetchone()
    assert db._conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'tracked_queries'"
    ).fetchone()
    assert db._conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'threshold_alerts'"
    ).fetchone()
    assert db.tracking("acme")["sources"] == []
    assert db.project(1)["queries"] == ["acme"]
    db.close()
