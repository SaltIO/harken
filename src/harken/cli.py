"""Harken command-line interface."""

from __future__ import annotations

import csv
import io
import json
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from harken import __version__
from harken.alerts import (
    EmailSettings,
    send_negative_alert,
    send_negative_email,
    send_threshold_alert,
    send_threshold_email,
)
from harken.analyze.insights import ThemeExtractor
from harken.analyze.sentiment import LexiconSentiment
from harken.auth import ROLES, hash_password, validate_password, validate_role, validate_username
from harken.config import Config
from harken.evaluate import evaluate_sentiment, load_sentiment_dataset
from harken.models import Mention, Sentiment
from harken.observability import configure_logging
from harken.pipeline import Pipeline
from harken.sample_data import DEMO_QUERY, sample_mentions
from harken.sources import REGISTRY
from harken.store import Store

app = typer.Typer(
    help="Harken — self-hosted social listening. Hear what the internet says about you.",
    no_args_is_help=True,
    add_completion=False,
)
project_app = typer.Typer(help="Group tracked keywords and report across them.")
app.add_typer(project_app, name="project")
user_app = typer.Typer(help="Manage opt-in local dashboard accounts and roles.")
app.add_typer(user_app, name="user")
console = Console()


def _version(value: bool):
    if value:
        console.print(f"harken {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    _v: bool = typer.Option(
        False, "--version", callback=_version, is_eager=True, help="Show version."
    ),
):
    cfg = Config()
    configure_logging(cfg.log_format, cfg.log_level)


@app.command()
def demo(
    serve: bool = typer.Option(True, help="Launch the web dashboard after loading."),
    port: int = typer.Option(8042, min=1, max=65535, help="Port for the dashboard."),
    db: str = typer.Option(None, help="Database path (default: harken.db, or $HARKEN_DB)."),
):
    """Load a bundled sample dataset and show the full pipeline — zero config, no keys."""
    console.print(
        Panel.fit(
            "[bold]Harken demo[/bold]\nLoading a bundled [italic]sample[/italic] dataset (synthetic, not real posts)\n"
            "and running the real pipeline: aggregate → sentiment → themes.",
            border_style="cyan",
        )
    )
    db_path = db or Config().db_path
    with Store(db_path) as store:
        mentions = sample_mentions()
        sentiment = LexiconSentiment()
        for m in mentions:
            r = sentiment.score(m.content)
            m.sentiment, m.sentiment_score = r.label, r.score
        store.upsert(mentions, update_theme=False)  # pre-cluster; themes written below
        stored = store.mentions(query=DEMO_QUERY, limit=10_000)
        ThemeExtractor().extract(stored)  # tags each mention with its theme, in place
        store.upsert(stored)
        _print_report(store, DEMO_QUERY)

    if serve:
        console.print(f"\n[bold cyan]Dashboard →[/bold cyan] http://localhost:{port}\n")
        _serve(db_path, port)


@app.command()
def track(
    query: str = typer.Argument(..., help="Keyword / brand / product to track."),
    sources: str = typer.Option(
        None, help="Comma-separated sources (default: hackernews,bluesky)."
    ),
    limit: int = typer.Option(50, min=1, max=100, help="Max items per source."),
    pages: int = typer.Option(
        3, min=1, max=20, help="Max continuation pages when catching up new mentions."
    ),
    db: str = typer.Option(None, help="Database path (default: harken.db)."),
    project: int = typer.Option(
        None, min=1, help="Add this keyword to a named project by numeric ID."
    ),
):
    """Fetch live mentions for a keyword from free sources, analyze, and store them."""
    query = _clean_query(query)
    cfg = _tracking_config(sources, limit, db)

    console.print(f"Listening for [bold]“{query}”[/bold] across: {', '.join(cfg.sources)} …")
    pipe = Pipeline(cfg)
    try:
        result = pipe.track(query, pages=pages, project_id=project)

        for src, err in result.errors.items():
            console.print(f"  [yellow]![/yellow] {src}: {err}")
        _print_retries(result)
        if result.analysis_error:
            console.print(f"  [yellow]![/yellow] optional LLM labels: {result.analysis_error}")
        if result.sentiment_error:
            console.print(f"  [yellow]![/yellow] sentiment: {result.sentiment_error}")
        _print_alert_result(result)
        console.print(
            f"[green]✓[/green] {result.fetched} fetched · [bold]{result.new}[/bold] new · "
            f"{sum(result.by_source.values())} matched"
        )
        _print_report(pipe.store, query)
    finally:
        pipe.close()
    console.print(f"\nView the dashboard: [cyan]harken serve --db {cfg.db_path}[/cyan]")

    configured_sources = {name.strip().lower() for name in cfg.sources if name.strip()}
    if configured_sources and len(result.errors) == len(configured_sources):
        raise typer.Exit(1)  # every configured source failed — nothing was fetched


@app.command()
def backfill(
    query: str = typer.Argument(..., help="Tracked keyword to fetch older mentions for."),
    pages: int = typer.Option(3, min=1, max=20, help="Older pages to fetch per source."),
    sources: str = typer.Option(
        None, help="Comma-separated sources (default: the keyword's saved sources)."
    ),
    limit: int = typer.Option(50, min=1, max=100, help="Max items per source page."),
    db: str = typer.Option(None, help="Database path (default: harken.db)."),
):
    """Fetch older result pages using durable per-source cursors."""
    query = _clean_query(query)
    cfg = _tracking_config(sources, limit, db)
    if sources is None:
        with Store(cfg.db_path) as store:
            tracking = store.tracking(query)
        if tracking and tracking["sources"]:
            cfg.sources = tracking["sources"]

    console.print(
        f"Backfilling [bold]“{query}”[/bold] across: {', '.join(cfg.sources)} "
        f"(up to {pages} pages each) …"
    )
    pipe = Pipeline(cfg)
    try:
        result = pipe.track(query, backfill=True, pages=pages)
        for src, err in result.errors.items():
            console.print(f"  [yellow]![/yellow] {src}: {err}")
        _print_retries(result)
        if result.sentiment_error:
            console.print(f"  [yellow]![/yellow] sentiment: {result.sentiment_error}")
        for source in cfg.sources:
            count = result.by_source.get(source, 0)
            page_count = result.pages_by_source.get(source, 0)
            complete = " · history complete" if result.backfill_complete.get(source) else ""
            console.print(f"  {source}: {count} matched from {page_count} page(s){complete}")
        console.print(
            f"[green]✓[/green] {result.fetched} fetched · [bold]{result.new}[/bold] historical mentions added"
        )
    finally:
        pipe.close()

    configured_sources = {name.strip().lower() for name in cfg.sources if name.strip()}
    if configured_sources and len(result.errors) == len(configured_sources):
        raise typer.Exit(1)


@app.command()
def watch(
    query: str = typer.Argument(..., help="Keyword / brand / product to track."),
    every: int = typer.Option(900, min=30, help="Seconds between scans (minimum: 30)."),
    runs: int = typer.Option(
        None, min=1, help="Stop after this many scans (default: keep running)."
    ),
    sources: str = typer.Option(
        None, help="Comma-separated sources (default: hackernews,bluesky)."
    ),
    limit: int = typer.Option(50, min=1, max=100, help="Max items per source per scan."),
    pages: int = typer.Option(
        3, min=1, max=20, help="Max continuation pages when a scan has fallen behind."
    ),
    db: str = typer.Option(None, help="Database path (default: harken.db)."),
):
    """Poll sources on an interval until stopped, suitable for a service or terminal."""
    query = _clean_query(query)
    cfg = _tracking_config(sources, limit, db)
    console.print(
        f"Watching [bold]“{query}”[/bold] every {every}s across: {', '.join(cfg.sources)} "
        "(Ctrl-C to stop)"
    )
    pipe = Pipeline(cfg)
    completed = 0
    try:
        while True:
            completed += 1
            try:
                result = pipe.track(query, pages=pages)
            except KeyboardInterrupt:
                raise
            except Exception as e:  # a single scan failing must not kill the watcher
                console.print(f"  [red]✗[/red] scan {completed} failed: {type(e).__name__}: {e}")
            else:
                for src, err in result.errors.items():
                    console.print(f"  [yellow]![/yellow] {src}: {err}")
                _print_retries(result)
                if result.analysis_error:
                    console.print(
                        f"  [yellow]![/yellow] optional LLM labels: {result.analysis_error}"
                    )
                if result.sentiment_error:
                    console.print(f"  [yellow]![/yellow] sentiment: {result.sentiment_error}")
                _print_alert_result(result)
                console.print(
                    f"[green]✓[/green] scan {completed}: {result.fetched} fetched · "
                    f"[bold]{result.new}[/bold] new"
                )
            if runs is not None and completed >= runs:
                break
            time.sleep(every)
    except KeyboardInterrupt:
        console.print("\n[dim]Stopped watching.[/dim]")
    finally:
        pipe.close()


@app.command()
def report(
    query: str = typer.Argument(None, help="Keyword to report on (default: most recent)."),
    db: str = typer.Option(None, help="Database path (default: harken.db, or $HARKEN_DB)."),
):
    """Print a sentiment + theme report for a tracked keyword."""
    store = Store(db or Config().db_path)
    try:
        queries = store.queries()
        q = query.strip() if query else (queries[0] if queries else None)
        if not q:
            console.print(
                '[yellow]No data yet. Run `harken track "keyword"` or `harken demo`.[/yellow]'
            )
            raise typer.Exit(1)
        if q not in queries:
            console.print(f"[yellow]No data found for “{q}”.[/yellow]")
            raise typer.Exit(1)
        _print_report(store, q)
    finally:
        store.close()


@app.command()
def serve(
    db: str = typer.Option(None, help="Database path (default: harken.db, or $HARKEN_DB)."),
    port: int = typer.Option(8042, min=1, max=65535, help="Port."),
    host: str = typer.Option("127.0.0.1", help="Bind host."),
):
    """Launch the local web dashboard."""
    cfg = Config()
    if host not in {"127.0.0.1", "localhost", "::1"}:
        if cfg.auth_mode == "accounts":
            console.print(
                "[yellow]![/yellow] Local account authentication is enabled. "
                "Use HTTPS and set HARKEN_SESSION_SECURE=true before exposing it remotely."
            )
        elif cfg.auth_username and cfg.auth_password:
            console.print(
                "[yellow]![/yellow] HTTP Basic authentication is enabled. "
                "Use HTTPS at the reverse proxy before exposing credentials remotely."
            )
        else:
            console.print(
                "[yellow]![/yellow] The dashboard has no authentication. "
                "Use a trusted network or an authenticated reverse proxy."
            )
    console.print(f"[bold cyan]Harken dashboard →[/bold cyan] http://{host}:{port}")
    _serve(db or cfg.db_path, port, host, cfg)


@user_app.command("list")
def user_list(
    db: str = typer.Option(None, help="Database path (default: harken.db)."),
):
    """List local accounts without exposing password or session material."""
    with Store(db or Config().db_path) as store:
        users = store.users()
    table = Table(title="Local users")
    table.add_column("id", justify="right")
    table.add_column("username", style="bold")
    table.add_column("role")
    table.add_column("status")
    table.add_column("last login")
    for user in users:
        table.add_row(
            str(user["id"]),
            user["username"],
            user["role"],
            "active" if user["active"] else "disabled",
            user["last_login_at"] or "—",
        )
    console.print(table)


@user_app.command("create")
def user_create(
    username: str = typer.Argument(..., help="Local username."),
    role: str | None = typer.Option(
        None, help="viewer, operator, or admin (first user defaults to admin)."
    ),
    db: str = typer.Option(None, help="Database path (default: harken.db)."),
):
    """Create a local account; the password is read from a hidden prompt."""
    try:
        username = validate_username(username)
        password = _prompt_new_password()
        password_hash = hash_password(password)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    with Store(db or Config().db_path) as store:
        selected_role = role or ("admin" if not store.users() else "viewer")
        try:
            user = store.create_user(username, password_hash, validate_role(selected_role))
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc
    console.print(f"[green]✓[/green] created {user['username']} as {user['role']}")


@user_app.command("password")
def user_password(
    username: str = typer.Argument(..., help="Local username."),
    db: str = typer.Option(None, help="Database path (default: harken.db)."),
):
    """Replace a password and revoke every existing session for that user."""
    try:
        password_hash = hash_password(_prompt_new_password())
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    with Store(db or Config().db_path) as store:
        user = _find_user(store, username)
        if user is None:
            raise typer.BadParameter(f"unknown user: {username}")
        store.set_user_password(user["id"], password_hash)
    console.print(f"[green]✓[/green] password updated and sessions revoked for {username}")


@user_app.command("role")
def user_role(
    username: str = typer.Argument(..., help="Local username."),
    role: str = typer.Argument(..., help=f"One of: {', '.join(ROLES)}."),
    db: str = typer.Option(None, help="Database path (default: harken.db)."),
):
    """Change a user's global role."""
    try:
        role = validate_role(role)
    except ValueError as exc:
        raise typer.BadParameter(str(exc), param_hint="role") from exc
    with Store(db or Config().db_path) as store:
        user = _find_user(store, username)
        if user is None:
            raise typer.BadParameter(f"unknown user: {username}")
        try:
            store.set_user_role(user["id"], role)
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc
    console.print(f"[green]✓[/green] {username} is now {role}")


@user_app.command("enable")
def user_enable(
    username: str = typer.Argument(..., help="Local username."),
    db: str = typer.Option(None, help="Database path (default: harken.db)."),
):
    """Enable a disabled local account."""
    _set_user_active(username, True, db)


@user_app.command("disable")
def user_disable(
    username: str = typer.Argument(..., help="Local username."),
    db: str = typer.Option(None, help="Database path (default: harken.db)."),
):
    """Disable an account and revoke its sessions."""
    _set_user_active(username, False, db)


@user_app.command("delete")
def user_delete(
    username: str = typer.Argument(..., help="Local username."),
    yes: bool = typer.Option(False, "--yes", help="Confirm permanent account deletion."),
    db: str = typer.Option(None, help="Database path (default: harken.db)."),
):
    """Delete an account; the last active admin is protected."""
    if not yes:
        console.print(f"Would delete local user {username}. Re-run with --yes to confirm.")
        return
    with Store(db or Config().db_path) as store:
        user = _find_user(store, username)
        if user is None:
            raise typer.BadParameter(f"unknown user: {username}")
        try:
            store.delete_user(user["id"])
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc
    console.print(f"[green]✓[/green] deleted {username}")


@app.command()
def sources():
    """List available mention sources."""
    table = Table(title="Sources")
    table.add_column("name", style="bold")
    table.add_column("label")
    table.add_column("zero-config", justify="center")
    for name, cls in REGISTRY.items():
        zc = "[green]yes[/green]" if not cls.needs_config else "—"
        table.add_row(name, cls.label, zc)
    console.print(table)


@app.command("evaluate")
def evaluate_analyzer(
    dataset: Path | None = typer.Option(  # noqa: B008 - Typer declarative option
        None,
        exists=True,
        dir_okay=False,
        readable=True,
        help="Custom JSON dataset (default: bundled sentiment_v1).",
    ),
    fmt: str = typer.Option("table", "--format", "-f", help="Output: table or json."),
    show_failures: int = typer.Option(
        10, min=0, max=100, help="Maximum misclassified examples to display."
    ),
    min_accuracy: float = typer.Option(
        None, min=0.0, max=1.0, help="Exit nonzero when accuracy is below this value."
    ),
):
    """Measure the local sentiment analyzer on a versioned labeled dataset."""
    fmt = fmt.strip().lower()
    if fmt not in {"table", "json"}:
        raise typer.BadParameter("format must be table or json", param_hint="--format")
    try:
        report = evaluate_sentiment(load_sentiment_dataset(dataset))
    except (OSError, ValueError) as exc:
        raise typer.BadParameter(str(exc), param_hint="--dataset") from exc

    if fmt == "json":
        typer.echo(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        dataset_meta = report["dataset"]
        console.print(
            Panel.fit(
                f"[bold]{dataset_meta['name']} v{dataset_meta['version']}[/bold]\n"
                f"{report['analyzer']} · {report['correct']}/{report['total']} correct · "
                f"accuracy [bold]{report['accuracy']:.1%}[/bold] · "
                f"macro F1 [bold]{report['macro_f1']:.1%}[/bold]\n"
                f"[dim]{dataset_meta['license']} · {dataset_meta['description']}[/dim]",
                title="Sentiment evaluation",
                border_style="cyan",
            )
        )
        metrics = Table(title="Per-label metrics")
        metrics.add_column("label", style="bold")
        metrics.add_column("precision", justify="right")
        metrics.add_column("recall", justify="right")
        metrics.add_column("F1", justify="right")
        metrics.add_column("support", justify="right")
        for label, row in report["per_label"].items():
            metrics.add_row(
                label,
                f"{row['precision']:.1%}",
                f"{row['recall']:.1%}",
                f"{row['f1']:.1%}",
                str(row["support"]),
            )
        console.print(metrics)

        confusion = Table(title="Confusion matrix · rows expected / columns predicted")
        confusion.add_column("expected", style="bold")
        for label in ("positive", "neutral", "negative"):
            confusion.add_column(label, justify="right")
        for expected, row in report["confusion_matrix"].items():
            confusion.add_row(expected, *(str(row[label]) for label in row))
        console.print(confusion)

        failures = report["failures"][:show_failures]
        if failures:
            failure_table = Table(title=f"Misclassifications · showing {len(failures)}")
            failure_table.add_column("id", style="dim")
            failure_table.add_column("expected → predicted")
            failure_table.add_column("score", justify="right")
            failure_table.add_column("text", overflow="fold")
            for failure in failures:
                failure_table.add_row(
                    failure["id"],
                    f"{failure['expected']} → {failure['predicted']}",
                    f"{failure['score']:+.3f}",
                    failure["text"],
                )
            console.print(failure_table)

    if min_accuracy is not None and report["accuracy"] < min_accuracy:
        console.print(
            f"[red]Accuracy {report['accuracy']:.1%} is below required {min_accuracy:.1%}.[/red]"
        )
        raise typer.Exit(1)


@project_app.command("list")
def list_projects(
    db: str = typer.Option(None, help="Database path (default: harken.db, or $HARKEN_DB)."),
):
    """List named projects and their aggregate sizes."""
    with Store(db or Config().db_path) as store:
        projects = store.projects()
    table = Table(title="Projects")
    table.add_column("id", justify="right", style="dim")
    table.add_column("name", style="bold")
    table.add_column("keywords", justify="right")
    table.add_column("mentions", justify="right")
    for project in projects:
        table.add_row(
            str(project["id"]),
            project["name"],
            str(project["query_count"]),
            str(project["mention_count"]),
        )
    console.print(table)


@project_app.command("create")
def create_project(
    name: str = typer.Argument(..., help="Project name."),
    db: str = typer.Option(None, help="Database path (default: harken.db, or $HARKEN_DB)."),
):
    """Create an empty named project."""
    with Store(db or Config().db_path) as store:
        try:
            project = store.create_project(name)
        except ValueError as exc:
            raise typer.BadParameter(str(exc), param_hint="name") from exc
    console.print(
        f"[green]✓[/green] created project [bold]{project['name']}[/bold] (id {project['id']})"
    )


@project_app.command("add")
def add_project_query(
    project_id: int = typer.Argument(..., min=1, help="Project ID."),
    query: str = typer.Argument(..., help="Existing tracked keyword."),
    db: str = typer.Option(None, help="Database path (default: harken.db, or $HARKEN_DB)."),
):
    """Add an existing tracked keyword to a project."""
    with Store(db or Config().db_path) as store:
        try:
            added = store.add_query_to_project(project_id, query)
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc
    message = "added" if added else "already belongs to"
    console.print(f"[green]✓[/green] “{query.strip()}” {message} project {project_id}")


@project_app.command("remove")
def remove_project_query(
    project_id: int = typer.Argument(..., min=1, help="Project ID."),
    query: str = typer.Argument(..., help="Tracked keyword to ungroup."),
    db: str = typer.Option(None, help="Database path (default: harken.db, or $HARKEN_DB)."),
):
    """Remove a keyword from a project without deleting its data."""
    with Store(db or Config().db_path) as store:
        try:
            removed = store.remove_query_from_project(project_id, query)
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc
    if not removed:
        console.print(f"[yellow]“{query.strip()}” is not in project {project_id}.[/yellow]")
        raise typer.Exit(1)
    console.print(f"[green]✓[/green] removed “{query.strip()}” from project {project_id}")


@project_app.command("delete")
def delete_project(
    project_id: int = typer.Argument(..., min=1, help="Project ID."),
    yes: bool = typer.Option(False, "--yes", help="Confirm deletion of the grouping."),
    db: str = typer.Option(None, help="Database path (default: harken.db, or $HARKEN_DB)."),
):
    """Delete a project grouping; keywords and mentions are preserved."""
    if not yes:
        console.print(
            f"Would delete project {project_id}; tracked keywords and mentions stay intact. "
            "Add [bold]--yes[/bold] to apply."
        )
        return
    with Store(db or Config().db_path) as store:
        try:
            deleted = store.delete_project(project_id)
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc
    if not deleted:
        console.print(f"[yellow]Project {project_id} does not exist.[/yellow]")
        raise typer.Exit(1)
    console.print(f"[green]✓[/green] deleted project {project_id}; data was preserved")


@project_app.command("report")
def project_report(
    project_id: int = typer.Argument(..., min=1, help="Project ID."),
    db: str = typer.Option(None, help="Database path (default: harken.db, or $HARKEN_DB)."),
):
    """Print aggregate sentiment and themes across a project."""
    with Store(db or Config().db_path) as store:
        project = store.project(project_id)
        if project is None:
            console.print(f"[yellow]Project {project_id} does not exist.[/yellow]")
            raise typer.Exit(1)
        _print_project_report(store, project)


@app.command("export")
def export_data(
    query: str = typer.Argument(None, help="Keyword to export (default: all keywords)."),
    fmt: str = typer.Option("json", "--format", "-f", help="Output format: json or csv."),
    output: str = typer.Option("-", "--output", "-o", help="Output file, or - for stdout."),
    db: str = typer.Option(None, help="Database path (default: harken.db, or $HARKEN_DB)."),
):
    """Export complete mention records as JSON or CSV."""
    fmt = fmt.strip().lower()
    if fmt not in {"json", "csv"}:
        raise typer.BadParameter("format must be json or csv", param_hint="--format")
    with Store(db or Config().db_path) as store:
        if query and query not in store.queries():
            console.print(f"[yellow]No data found for “{query}”.[/yellow]")
            raise typer.Exit(1)
        rows = store.mentions(query=query, limit=None)
    if not rows:
        console.print("[yellow]No data to export.[/yellow]")
        raise typer.Exit(1)
    records = [_mention_record(mention) for mention in rows]
    content = _serialize_export(records, fmt)
    if output == "-":
        typer.echo(content, nl=False)
        return
    target = Path(output).expanduser()
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
    except OSError as exc:
        console.print(f"[red]✗[/red] could not write {target}: {exc}")
        raise typer.Exit(1) from exc
    console.print(f"[green]✓[/green] exported {len(records)} mentions to {target}")


@app.command()
def backup(
    output: str = typer.Argument(..., help="Destination .db file."),
    force: bool = typer.Option(False, "--force", help="Overwrite an existing backup."),
    db: str = typer.Option(None, help="Database path (default: harken.db, or $HARKEN_DB)."),
):
    """Create a consistent SQLite backup while Harken is running."""
    with Store(db or Config().db_path) as store:
        try:
            target = store.backup(output, overwrite=force)
        except (FileExistsError, ValueError) as exc:
            raise typer.BadParameter(str(exc), param_hint="output") from exc
        except (OSError, sqlite3.Error) as exc:
            console.print(f"[red]✗[/red] backup failed: {exc}")
            raise typer.Exit(1) from exc
    console.print(f"[green]✓[/green] backup written to {target}")


@app.command()
def prune(
    older_than: int = typer.Option(
        ..., "--older-than", min=1, help="Remove mentions older than this many days."
    ),
    query: str = typer.Option(None, help="Only prune this tracked keyword."),
    yes: bool = typer.Option(False, "--yes", help="Actually delete; default is a preview."),
    db: str = typer.Option(None, help="Database path (default: harken.db, or $HARKEN_DB)."),
):
    """Preview or remove old mentions and their queued alert state."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=older_than)
    with Store(db or Config().db_path) as store:
        if query and query not in store.queries():
            console.print(f"[yellow]No data found for “{query}”.[/yellow]")
            raise typer.Exit(1)
        count = store.count_before(cutoff, query=query)
        scope = f" for “{query}”" if query else ""
        if not yes:
            console.print(
                f"Would remove [bold]{count}[/bold] mentions{scope} older than "
                f"{cutoff.date().isoformat()}. Add [bold]--yes[/bold] to apply."
            )
            return
        deleted = store.delete_before(cutoff, query=query)
    console.print(f"[green]✓[/green] removed {deleted} mentions{scope}")


@app.command("retention")
def retain_data(
    source: str | None = typer.Option(None, help="Limit retention to this source."),
    yes: bool = typer.Option(False, "--yes", help="Apply reviewed expiry; default is a preview."),
    db: str | None = typer.Option(None, help="Database path (default: configured HARKEN_DB)."),
):
    """Preview 30-day body expiry and 90-day identity removal, by first ingestion."""
    with Store(db or Config().db_path) as store:
        result = store.retention(now=datetime.now(timezone.utc), source=source, apply=yes)
    verb = "Expired" if yes else "Would expire"
    console.print(f"{verb} {result['bodies']} bodies and {result['identities']} identities.")
    console.print(f"{verb} {result['alert_payloads']} associated threshold-alert payloads.")
    if not yes:
        console.print("Review the preview, then pass --yes to apply. No retention changes made.")


@app.command("test-alert")
def test_alert(
    webhook_url: str = typer.Option(
        None, "--webhook-url", help="Override HARKEN_WEBHOOK_URL for this test."
    ),
    transport: str = typer.Option(
        None, help="Delivery transport: webhook or email (default: configured transport)."
    ),
    kind: str = typer.Option("negative", help="Synthetic event: negative, volume, or sentiment."),
):
    """Send one synthetic alert to verify a webhook or SMTP configuration."""
    cfg = Config()
    selected = (
        transport or ("webhook" if webhook_url or cfg.webhook_url or not cfg.email_to else "email")
    ).lower()
    if selected not in {"webhook", "email"}:
        raise typer.BadParameter("transport must be webhook or email", param_hint="--transport")
    kind = kind.strip().lower()
    if kind not in {"negative", "volume", "sentiment"}:
        raise typer.BadParameter("kind must be negative, volume, or sentiment", param_hint="--kind")

    url = webhook_url or cfg.webhook_url
    email_settings = _email_settings(cfg) if selected == "email" else None
    if selected == "webhook" and not url:
        raise typer.BadParameter(
            "set HARKEN_WEBHOOK_URL or pass --webhook-url", param_hint="--webhook-url"
        )
    if selected == "email" and email_settings is None:
        raise typer.BadParameter(
            "configure HARKEN_EMAIL_TO, HARKEN_EMAIL_FROM, and HARKEN_SMTP_HOST",
            param_hint="--transport",
        )
    try:
        if kind == "negative":
            mention = Mention(
                source="harken",
                query="webhook test",
                author="Harken",
                text=(
                    "This is a synthetic negative-mention alert. "
                    "Your alert transport is configured correctly."
                ),
                created_at=datetime.now(timezone.utc),
                sentiment=Sentiment.NEGATIVE,
                sentiment_score=-1.0,
            )
            if selected == "webhook":
                send_negative_alert(url or "", mention.query, [mention])
            else:
                send_negative_email(email_settings, mention.query, [mention])
        else:
            event = f"harken.{kind}_spike" if kind == "volume" else "harken.sentiment_drop"
            text = f"Harken test: synthetic {kind} threshold alert"
            payload = {"event": event, "query": "webhook test", "synthetic": True}
            if selected == "webhook":
                send_threshold_alert(url or "", text, payload)
            else:
                send_threshold_email(email_settings, text, payload)
    except Exception as exc:
        console.print(f"[red]Alert test failed:[/red] {type(exc).__name__}: {exc}")
        raise typer.Exit(1) from exc
    console.print(f"[green]✓[/green] {selected} test delivered")


# -- helpers -----------------------------------------------------------------
def _prompt_new_password() -> str:
    password = typer.prompt("Password", hide_input=True, confirmation_prompt=True)
    return validate_password(password)


def _find_user(store: Store, username: str) -> dict | None:
    expected = username.strip().casefold()
    return next((user for user in store.users() if user["username"].casefold() == expected), None)


def _set_user_active(username: str, active: bool, db: str | None) -> None:
    with Store(db or Config().db_path) as store:
        user = _find_user(store, username)
        if user is None:
            raise typer.BadParameter(f"unknown user: {username}")
        try:
            store.set_user_active(user["id"], active)
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc
    state = "enabled" if active else "disabled and sessions revoked for"
    console.print(f"[green]✓[/green] {state} {username}")


def _clean_query(query: str) -> str:
    query = query.strip()
    if not query:
        raise typer.BadParameter("query must not be empty", param_hint="query")
    return query


def _tracking_config(sources: str | None, limit: int, db: str | None) -> Config:
    cfg = Config()
    if sources is not None:
        cfg.sources = [s.strip().lower() for s in sources.split(",") if s.strip()]
        if not cfg.sources:
            raise typer.BadParameter("provide at least one source", param_hint="--sources")
    cfg.per_source_limit = limit
    if db:
        cfg.db_path = db
    return cfg


def _email_settings(config: Config) -> EmailSettings | None:
    if not config.email_to:
        return None
    return EmailSettings(
        host=config.smtp_host or "",
        port=config.smtp_port,
        sender=config.email_from or "",
        recipients=tuple(config.email_to),
        security=config.smtp_security,
        username=config.smtp_username,
        password=config.smtp_password,
    )


def _print_alert_result(result) -> None:
    if result.alerted:
        console.print(f"  [green]↗[/green] delivered {result.alerted} negative-mention alerts")
    if result.alert_error:
        pending_count = result.alert_pending + result.threshold_pending
        pending = f" ({pending_count} queued for retry)" if pending_count else ""
        console.print(f"  [yellow]![/yellow] alert delivery: {result.alert_error}{pending}")
    if result.threshold_alerted:
        names = ", ".join(result.threshold_events) or "threshold"
        console.print(
            f"  [green]↗[/green] delivered {result.threshold_alerted} metric alert(s): {names}"
        )


def _print_retries(result) -> None:
    for source, count in result.retry_counts.items():
        console.print(f"  [dim]↻ {source}: {count} retr{'y' if count == 1 else 'ies'}[/dim]")


def _serve(db: str, port: int, host: str = "127.0.0.1", config: Config | None = None):
    import uvicorn

    from harken.web.app import create_app

    cfg = config or Config()
    uvicorn.run(
        create_app(
            db,
            auth_username=cfg.auth_username,
            auth_password=cfg.auth_password,
            config=cfg,
        ),
        host=host,
        port=port,
        log_level="warning",
    )


def _mention_record(mention) -> dict:
    return {
        "id": mention.id,
        "source": mention.source,
        "query": mention.query,
        "author": mention.author,
        "title": mention.title,
        "text": mention.text,
        "url": mention.url,
        "created_at": mention.created_at.isoformat(),
        "published_at": mention.published_at.isoformat() if mention.published_at else None,
        "publication_provenance": mention.publication_provenance,
        "source_updated_at": (mention.source_updated_at.isoformat()
                              if mention.source_updated_at else None),
        "fetched_at": mention.fetched_at.isoformat() if mention.fetched_at else None,
        "body_expired": mention.body_expired,
        "source_id": mention.source_id,
        "score": mention.score,
        "sentiment": mention.sentiment.value if mention.sentiment else None,
        "sentiment_score": mention.sentiment_score,
        "theme": mention.theme,
    }


def _serialize_export(records: list[dict], fmt: str) -> str:
    if fmt == "json":
        return json.dumps(records, ensure_ascii=False, indent=2) + "\n"
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(records[0]))
    writer.writeheader()
    writer.writerows(records)
    return buffer.getvalue()


def _print_report(store: Store, query: str):
    summary = store.summary(query=query)
    total = summary["total"] or 1
    bs = summary["by_sentiment"]
    pos, neu, neg = bs.get("positive", 0), bs.get("neutral", 0), bs.get("negative", 0)

    table = Table(title=f"“{query}” — {summary['total']} mentions", show_header=False, box=None)
    table.add_column(justify="right", style="bold")
    table.add_column()
    table.add_row("positive", f"[green]{'█' * round(20 * pos / total)}[/green] {pos}")
    table.add_row("neutral", f"[grey50]{'█' * round(20 * neu / total)}[/grey50] {neu}")
    table.add_row("negative", f"[red]{'█' * round(20 * neg / total)}[/red] {neg}")
    table.add_row("sources", ", ".join(f"{k} ({v})" for k, v in summary["by_source"].items()))
    console.print(table)

    mentions = store.mentions(query=query, limit=None)
    themes = ThemeExtractor().extract(mentions)
    store.upsert(mentions)  # persist theme labels so the dashboard sees the same themes
    if themes:
        tt = Table(title="Top themes", show_header=True, box=None)
        tt.add_column("theme", style="yellow")
        tt.add_column("mentions", justify="right")
        for t in themes[:6]:
            tt.add_row(t.label, str(t.count))
        console.print(tt)

    # a couple of representative quotes
    neg_quotes = [m for m in mentions if m.sentiment is Sentiment.NEGATIVE and m.text][:2]
    pos_quotes = [m for m in mentions if m.sentiment is Sentiment.POSITIVE and m.text][:2]
    if pos_quotes or neg_quotes:
        console.print()
        for m in pos_quotes:
            console.print(f"  [green]+[/green] [dim]{m.source}[/dim] {m.text[:120]}")
        for m in neg_quotes:
            console.print(f"  [red]−[/red] [dim]{m.source}[/dim] {m.text[:120]}")


def _print_project_report(store: Store, project: dict) -> None:
    summary = store.summary(project_id=project["id"])
    total = summary["total"] or 1
    sentiments = summary["by_sentiment"]
    pos = sentiments.get("positive", 0)
    neu = sentiments.get("neutral", 0)
    neg = sentiments.get("negative", 0)
    table = Table(
        title=(
            f"{project['name']} — {summary['total']} mentions across "
            f"{project['query_count']} keyword"
            f"{'s' if project['query_count'] != 1 else ''}"
        ),
        show_header=False,
        box=None,
    )
    table.add_column(justify="right", style="bold")
    table.add_column()
    table.add_row("keywords", ", ".join(project["queries"]) or "—")
    table.add_row("positive", f"[green]{'█' * round(20 * pos / total)}[/green] {pos}")
    table.add_row("neutral", f"[grey50]{'█' * round(20 * neu / total)}[/grey50] {neu}")
    table.add_row("negative", f"[red]{'█' * round(20 * neg / total)}[/red] {neg}")
    table.add_row(
        "sources", ", ".join(f"{key} ({value})" for key, value in summary["by_source"].items())
    )
    console.print(table)

    themes = store.themes(project_id=project["id"])
    if themes:
        theme_table = Table(title="Top project themes", show_header=True, box=None)
        theme_table.add_column("theme", style="yellow")
        theme_table.add_column("mentions", justify="right")
        for theme in themes[:6]:
            theme_table.add_row(theme["label"], str(theme["count"]))
        console.print(theme_table)


if __name__ == "__main__":
    app()
