"""CLI tests — cover the surface the review found at 0% coverage.

Uses Typer's CliRunner; HTTP is mocked with respx where a command talks to a
live source. `_serve` (which blocks on uvicorn.run) is stubbed out.
"""

import csv
import json
from datetime import datetime, timedelta, timezone

import httpx
import respx
from typer.testing import CliRunner

from harken import cli
from harken.models import Mention
from harken.store import Store

runner = CliRunner()


def test_store_epoch_requires_matching_restore_precondition(tmp_path):
    path = tmp_path / "epoch.db"
    initial = runner.invoke(cli.app, ["store-epoch", "--db", str(path)])
    assert initial.exit_code == 0, initial.output
    inspected = json.loads(initial.output)
    assert not inspected["rotated"]
    for args in (["--rotate"], ["--rotate", "--expected", "stale"]):
        refused = runner.invoke(cli.app, ["store-epoch", "--db", str(path), *args])
        assert refused.exit_code == 2
        with Store(path) as db:
            assert db.store_epoch == inspected["store_epoch"]
    rotated = runner.invoke(cli.app, ["store-epoch", "--db", str(path), "--rotate",
                                     "--expected", inspected["store_epoch"]])
    assert rotated.exit_code == 0, rotated.output
    assert json.loads(rotated.output)["store_epoch"] != inspected["store_epoch"]


def test_retention_defaults_to_preview_and_export_preserves_provenance(tmp_path, monkeypatch):
    path = tmp_path / "retention.db"
    with Store(path) as db:
        db.upsert([Mention(source="rss", query="CMBS", text="dated body",
                           created_at=datetime.now(timezone.utc))])
        first = db.mentions()[0]

    class Future(datetime):
        @classmethod
        def now(cls, tz=None):
            return first.fetched_at + timedelta(days=31)

    monkeypatch.setattr(cli, "datetime", Future)
    preview = runner.invoke(cli.app, ["retention", "--db", str(path)])
    assert preview.exit_code == 0, preview.output
    assert "Would expire 1 bodies" in preview.output
    with Store(path) as db:
        assert db.mentions()[0].text == "dated body"
    applied = runner.invoke(cli.app, ["retention", "--db", str(path), "--yes"])
    assert applied.exit_code == 0, applied.output
    with Store(path) as db:
        record = cli._mention_record(db.mentions()[0])
        assert record["published_at"] is None
        assert record["publication_provenance"] == "unknown"
        assert record["fetched_at"] == first.fetched_at.isoformat()
        assert record["body_expired"] and record["text"] == ""


def test_demo_and_serve_default_to_the_same_db(tmp_path, monkeypatch):
    # regression test: `demo` used to write to harken-demo.db while `report`
    # and `serve` defaulted to harken.db, so the documented quickstart
    # (harken demo -> harken serve) opened an empty dashboard.
    monkeypatch.chdir(tmp_path)
    seen_dbs = []
    monkeypatch.setattr(cli, "_serve", lambda db, port, *args: seen_dbs.append(db))

    demo_result = runner.invoke(cli.app, ["demo"])
    assert demo_result.exit_code == 0, demo_result.output

    serve_result = runner.invoke(cli.app, ["serve"])
    assert serve_result.exit_code == 0, serve_result.output

    assert seen_dbs == ["harken.db", "harken.db"]


def test_demo_then_report_share_data_with_no_flags(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    demo_result = runner.invoke(cli.app, ["demo", "--no-serve"])
    assert demo_result.exit_code == 0
    assert "No data yet" not in demo_result.output

    report_result = runner.invoke(cli.app, ["report"])
    assert report_result.exit_code == 0
    assert "No data yet" not in report_result.output
    assert "mentions" in report_result.output


def test_demo_dedupes_on_a_second_run(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    runner.invoke(cli.app, ["demo", "--no-serve"])
    first = runner.invoke(cli.app, ["report"])
    runner.invoke(cli.app, ["demo", "--no-serve"])
    second = runner.invoke(cli.app, ["report"])
    assert first.output == second.output  # same totals, not doubled


def test_report_with_no_data_exits_nonzero(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(cli.app, ["report", "--db", str(tmp_path / "empty.db")])
    assert result.exit_code == 1
    assert "No data yet" in result.output


def test_report_closes_the_store_on_the_no_data_exit_path(tmp_path, monkeypatch):
    # regression test: `report` used to raise typer.Exit(1) before reaching
    # store.close() on the "no data yet" path, leaking the sqlite connection.
    from harken.store import Store

    closed = []
    original_close = Store.close
    monkeypatch.setattr(Store, "close", lambda self: (closed.append(True), original_close(self)))

    runner.invoke(cli.app, ["report", "--db", str(tmp_path / "empty.db")])
    assert closed == [True]


@respx.mock
def test_track_exits_nonzero_when_every_source_fails(tmp_path, monkeypatch):
    monkeypatch.setenv("HARKEN_RETRIES", "0")
    respx.get("https://hn.algolia.com/api/v1/search_by_date").mock(return_value=httpx.Response(500))
    db = str(tmp_path / "t.db")
    result = runner.invoke(cli.app, ["track", "acme", "--sources", "hackernews", "--db", db])
    assert result.exit_code == 1


@respx.mock
def test_track_exits_zero_when_a_source_succeeds(tmp_path):
    respx.get("https://hn.algolia.com/api/v1/search_by_date").mock(
        return_value=httpx.Response(200, json={"hits": []})
    )
    db = str(tmp_path / "t.db")
    result = runner.invoke(cli.app, ["track", "acme", "--sources", "hackernews", "--db", db])
    assert result.exit_code == 0


@respx.mock
def test_backfill_command_resumes_saved_source_cursor(tmp_path):
    route = respx.get("https://hn.algolia.com/api/v1/search_by_date").mock(
        side_effect=[
            httpx.Response(
                200,
                json={
                    "hits": [
                        {
                            "objectID": "new",
                            "title": "Acme now",
                            "created_at_i": 1_700_000_100,
                        }
                    ],
                    "page": 0,
                    "nbPages": 2,
                },
            ),
            httpx.Response(
                200,
                json={
                    "hits": [
                        {
                            "objectID": "old",
                            "title": "Acme before",
                            "created_at_i": 1_699_000_000,
                        }
                    ],
                    "page": 0,
                    "nbPages": 1,
                },
            ),
        ]
    )
    db = str(tmp_path / "backfill.db")
    tracked = runner.invoke(
        cli.app,
        ["track", "acme", "--sources", "hackernews", "--limit", "1", "--db", db],
    )
    assert tracked.exit_code == 0, tracked.output
    result = runner.invoke(cli.app, ["backfill", "acme", "--limit", "1", "--db", db])
    assert result.exit_code == 0, result.output
    assert "history complete" in result.output
    with Store(db) as store:
        assert store.summary("acme")["total"] == 2
    assert route.call_count == 2


def test_sources_lists_registry():
    result = runner.invoke(cli.app, ["sources"])
    assert result.exit_code == 0
    assert "hackernews" in result.output
    assert "reddit" in result.output


def test_project_cli_create_add_report_remove_and_delete(tmp_path):
    db = str(tmp_path / "projects.db")
    assert runner.invoke(cli.app, ["demo", "--no-serve", "--db", db]).exit_code == 0
    created = runner.invoke(cli.app, ["project", "create", "Product Suite", "--db", db])
    assert created.exit_code == 0, created.output
    with Store(db) as store:
        project_id = next(
            project["id"] for project in store.projects() if project["name"] == "Product Suite"
        )

    added = runner.invoke(cli.app, ["project", "add", str(project_id), "Quill", "--db", db])
    assert added.exit_code == 0, added.output
    listed = runner.invoke(cli.app, ["project", "list", "--db", db])
    assert listed.exit_code == 0
    assert "Product Suite" in listed.output
    report = runner.invoke(cli.app, ["project", "report", str(project_id), "--db", db])
    assert report.exit_code == 0
    assert "32 mentions across 1 keyword" in report.output

    removed = runner.invoke(cli.app, ["project", "remove", str(project_id), "Quill", "--db", db])
    assert removed.exit_code == 0
    preview = runner.invoke(cli.app, ["project", "delete", str(project_id), "--db", db])
    assert preview.exit_code == 0
    assert "Would delete" in preview.output
    deleted = runner.invoke(cli.app, ["project", "delete", str(project_id), "--yes", "--db", db])
    assert deleted.exit_code == 0
    with Store(db) as store:
        assert store.project(project_id) is None
        assert store.summary("Quill")["total"] == 32


def test_user_cli_lifecycle_and_last_admin_protection(tmp_path):
    db = str(tmp_path / "users.db")
    created = runner.invoke(
        cli.app,
        ["user", "create", "admin", "--db", db],
        input="admin password 123\nadmin password 123\n",
    )
    assert created.exit_code == 0, created.output
    assert "created admin as admin" in created.output

    viewer = runner.invoke(
        cli.app,
        ["user", "create", "reader", "--role", "viewer", "--db", db],
        input="reader password 123\nreader password 123\n",
    )
    assert viewer.exit_code == 0, viewer.output
    listed = runner.invoke(cli.app, ["user", "list", "--db", db])
    assert listed.exit_code == 0
    assert "admin" in listed.output and "reader" in listed.output
    assert "password_hash" not in listed.output

    protected = runner.invoke(cli.app, ["user", "disable", "admin", "--db", db])
    assert protected.exit_code != 0
    assert "last active admin" in protected.output
    disabled = runner.invoke(cli.app, ["user", "disable", "reader", "--db", db])
    assert disabled.exit_code == 0
    assert runner.invoke(cli.app, ["user", "enable", "reader", "--db", db]).exit_code == 0
    assert runner.invoke(cli.app, ["user", "delete", "reader", "--yes", "--db", db]).exit_code == 0


def test_user_cli_password_policy(tmp_path):
    db = str(tmp_path / "password-policy.db")
    result = runner.invoke(
        cli.app,
        ["user", "create", "admin", "--db", db],
        input="too-short\ntoo-short\n",
    )
    assert result.exit_code != 0
    assert "at least 12 characters" in result.output


@respx.mock
def test_track_can_assign_new_keyword_to_project(tmp_path):
    respx.get("https://hn.algolia.com/api/v1/search_by_date").mock(
        return_value=httpx.Response(200, json={"hits": []})
    )
    db = str(tmp_path / "project-track.db")
    with Store(db) as store:
        project = store.create_project("Focused")
    result = runner.invoke(
        cli.app,
        [
            "track",
            "project-only",
            "--sources",
            "hackernews",
            "--project",
            str(project["id"]),
            "--db",
            db,
        ],
    )
    assert result.exit_code == 0, result.output
    with Store(db) as store:
        assert store.queries(project_id=project["id"]) == ["project-only"]
        assert "project-only" not in store.queries(project_id=1)


def test_version_flag():
    result = runner.invoke(cli.app, ["--version"])
    assert result.exit_code == 0


def test_evaluate_command_supports_json_and_accuracy_gate():
    result = runner.invoke(cli.app, ["evaluate", "--format", "json"])
    assert result.exit_code == 0, result.output
    report = json.loads(result.output)
    assert report["accuracy"] == 0.9667
    assert report["dataset"]["examples"] == 60

    passing = runner.invoke(cli.app, ["evaluate", "--min-accuracy", "0.95"])
    assert passing.exit_code == 0
    failing = runner.invoke(cli.app, ["evaluate", "--min-accuracy", "0.99"])
    assert failing.exit_code == 1
    assert "below required" in failing.output


def test_export_json_and_csv_include_complete_records(tmp_path):
    db = str(tmp_path / "demo.db")
    assert runner.invoke(cli.app, ["demo", "--no-serve", "--db", db]).exit_code == 0

    json_path = tmp_path / "mentions.json"
    result = runner.invoke(
        cli.app,
        ["export", "Quill", "--format", "json", "--output", str(json_path), "--db", db],
    )
    assert result.exit_code == 0
    records = json.loads(json_path.read_text())
    assert len(records) == 32
    assert set(records[0]) >= {"id", "source", "query", "sentiment", "theme"}

    csv_path = tmp_path / "mentions.csv"
    result = runner.invoke(
        cli.app,
        ["export", "--format", "csv", "--output", str(csv_path), "--db", db],
    )
    assert result.exit_code == 0
    with csv_path.open() as exported:
        assert len(list(csv.DictReader(exported))) == 32


def test_backup_command_preserves_data_and_requires_force(tmp_path):
    db = str(tmp_path / "demo.db")
    assert runner.invoke(cli.app, ["demo", "--no-serve", "--db", db]).exit_code == 0
    destination = tmp_path / "backup.db"
    first = runner.invoke(cli.app, ["backup", str(destination), "--db", db])
    assert first.exit_code == 0
    with Store(destination) as backup_store:
        assert backup_store.summary("Quill")["total"] == 32
    assert runner.invoke(cli.app, ["backup", str(destination), "--db", db]).exit_code != 0
    forced = runner.invoke(cli.app, ["backup", str(destination), "--force", "--db", db])
    assert forced.exit_code == 0


def test_prune_is_preview_only_without_explicit_yes(tmp_path):
    db = str(tmp_path / "demo.db")
    assert runner.invoke(cli.app, ["demo", "--no-serve", "--db", db]).exit_code == 0
    preview = runner.invoke(cli.app, ["prune", "--older-than", "1", "--db", db])
    assert preview.exit_code == 0
    assert "Would remove" in preview.output
    with Store(db) as store:
        assert store.summary("Quill")["total"] == 32

    applied = runner.invoke(cli.app, ["prune", "--older-than", "1", "--yes", "--db", db])
    assert applied.exit_code == 0
    with Store(db) as store:
        assert store.summary("Quill")["total"] < 32


@respx.mock
def test_alert_command_sends_synthetic_notification():
    route = respx.post("https://alerts.example.test/harken").mock(return_value=httpx.Response(204))
    result = runner.invoke(
        cli.app,
        ["test-alert", "--webhook-url", "https://alerts.example.test/harken"],
    )
    assert result.exit_code == 0
    assert route.called
    assert b"synthetic negative-mention alert" in route.calls[0].request.content


@respx.mock
def test_alert_command_can_send_synthetic_threshold_event():
    route = respx.post("https://alerts.example.test/harken").mock(return_value=httpx.Response(204))
    result = runner.invoke(
        cli.app,
        [
            "test-alert",
            "--kind",
            "volume",
            "--webhook-url",
            "https://alerts.example.test/harken",
        ],
    )
    assert result.exit_code == 0
    assert json.loads(route.calls[0].request.content)["event"] == "harken.volume_spike"


def test_alert_command_can_send_synthetic_email(monkeypatch):
    monkeypatch.setenv("HARKEN_EMAIL_TO", "ops@example.test")
    monkeypatch.setenv("HARKEN_EMAIL_FROM", "harken@example.test")
    monkeypatch.setenv("HARKEN_SMTP_HOST", "smtp.example.test")
    monkeypatch.setenv("HARKEN_SMTP_SECURITY", "none")
    delivered = []
    monkeypatch.setattr(
        cli,
        "send_negative_email",
        lambda settings, query, mentions: delivered.append((settings, query, mentions)),
    )
    result = runner.invoke(cli.app, ["test-alert", "--transport", "email"])
    assert result.exit_code == 0, result.output
    settings, query, mentions = delivered[0]
    assert settings.host == "smtp.example.test"
    assert settings.recipients == ("ops@example.test",)
    assert query == "webhook test"
    assert "synthetic negative-mention alert" in mentions[0].text


def test_alert_command_requires_configured_email_transport():
    result = runner.invoke(cli.app, ["test-alert", "--transport", "email"])
    assert result.exit_code != 0
    assert "HARKEN_EMAIL_TO" in result.output


def test_track_rejects_blank_query():
    result = runner.invoke(cli.app, ["track", "   "])
    assert result.exit_code != 0
    assert "must not be empty" in result.output


def test_track_rejects_nonpositive_limit():
    result = runner.invoke(cli.app, ["track", "acme", "--limit", "0"])
    assert result.exit_code != 0


def test_report_rejects_unknown_query(tmp_path):
    db = str(tmp_path / "t.db")
    runner.invoke(cli.app, ["demo", "--no-serve", "--db", db])
    result = runner.invoke(cli.app, ["report", "missing", "--db", db])
    assert result.exit_code == 1
    assert "No data found" in result.output


def test_serve_warns_when_exposed_without_authentication(tmp_path, monkeypatch):
    monkeypatch.setattr(cli, "_serve", lambda *args: None)
    result = runner.invoke(
        cli.app,
        ["serve", "--host", "0.0.0.0", "--db", str(tmp_path / "t.db")],
    )
    assert result.exit_code == 0
    assert "no authentication" in result.output


def test_serve_reports_enabled_auth_and_https_requirement(tmp_path, monkeypatch):
    monkeypatch.setenv("HARKEN_AUTH_USERNAME", "admin")
    monkeypatch.setenv("HARKEN_AUTH_PASSWORD", "secret")
    monkeypatch.setattr(cli, "_serve", lambda *args: None)
    result = runner.invoke(
        cli.app,
        ["serve", "--host", "0.0.0.0", "--db", str(tmp_path / "t.db")],
    )
    assert result.exit_code == 0
    assert "authentication is enabled" in result.output
    assert "HTTPS" in result.output


@respx.mock
def test_watch_can_run_a_bounded_scan(tmp_path):
    route = respx.get("https://hn.algolia.com/api/v1/search_by_date").mock(
        return_value=httpx.Response(200, json={"hits": []})
    )
    result = runner.invoke(
        cli.app,
        [
            "watch",
            "acme",
            "--sources",
            "hackernews",
            "--runs",
            "1",
            "--db",
            str(tmp_path / "watch.db"),
        ],
    )
    assert result.exit_code == 0, result.output
    assert route.call_count == 1
    assert "scan 1" in result.output


def test_watch_survives_a_failing_scan(tmp_path, monkeypatch):
    # A store error inside a scan must be logged and the watcher must keep
    # going / exit cleanly on the run cap, not crash the long-running process.
    class BoomPipe:
        def __init__(self, cfg):
            pass

        def track(self, query, pages=3):
            raise RuntimeError("database is locked")

        def close(self):
            pass

    monkeypatch.setattr(cli, "Pipeline", BoomPipe)
    result = runner.invoke(
        cli.app,
        ["watch", "acme", "--sources", "hackernews", "--runs", "1", "--db", str(tmp_path / "w.db")],
    )
    assert result.exit_code == 0, result.output
    assert "failed" in result.output


def test_export_to_a_directory_exits_cleanly(tmp_path):
    db = str(tmp_path / "e.db")
    assert runner.invoke(cli.app, ["demo", "--no-serve", "--db", db]).exit_code == 0
    result = runner.invoke(cli.app, ["export", "--db", db, "-o", str(tmp_path)])
    assert result.exit_code == 1
    assert "Traceback" not in result.output
