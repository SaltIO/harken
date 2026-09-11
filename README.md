<div align="center">

# ◟◞ Harken

**Self-hosted social listening — hear what the internet says about you, on your own box.**

Track a keyword, brand, or product across Hacker News, Reddit, Mastodon, Bluesky, Stack Overflow, RSS, X, and YouTube.
Get sentiment and themes in a clean local dashboard. No Harken account, telemetry, or per-seat pricing — the database stays on your machine.

[![CI](https://github.com/VladUZH/harken/actions/workflows/ci.yml/badge.svg)](https://github.com/VladUZH/harken/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-22c55e.svg)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-4ec9b0.svg)](https://www.python.org/)
[![PRs welcome](https://img.shields.io/badge/PRs-welcome-d8a657.svg)](#contributing)

</div>

<div align="center">
<img src="docs/dashboard-hero.png" alt="Harken dashboard" width="820">
</div>

---

## Why Harken

This fork adds bounded multi-term collection, honest RSS failure reporting,
publication provenance, immutable first-fetch timestamps, and a read-only API mode.
`HARKEN_USER_AGENT` sets a contact-bearing HTTP user agent. Install this fork
with `uv sync --locked` to reproduce the checked-in dependency resolution.

```bash
# One sequential batch; one configured RSS feed is fetched once for all terms.
uv run python -m harken.batch --terms example another --db harken.db \
  --receipt batch-result.json

# Loopback API that rejects mutation requests, including manual collection.
HARKEN_READ_ONLY=true uv run harken serve --host 127.0.0.1 --port 8042

# Preview only: 30-day body expiry, 90-day identity expiry. No automatic deletion.
uv run harken retention --db harken.db
```

Set `HARKEN_RSS_FEEDS` to exactly one feed for the batch command. The caller
owns scheduling and locking, and must honor the receipt's `retry_after_seconds`
before another batch. The API's `/api/mentions/page` accepts source/query/time
filters and returns a stable three-part first-ingestion cursor. Use the observation
change stream below for edits and removals. Undated publication remains null;
`created_at` alone is an operational ordering time, not publication proof.
The original quickstart and wider adapter interface are documented below.

### Observation and coverage API (fork)

These reads query the local store and make no source requests. A profile, outcome,
case, or relevance classification is not required. Optional batch flags
`--profile-id NAME --profile-version VERSION` record caller context without adding
business policy. The existing authentication and read-only settings also protect
these routes.

| Route | Result |
|---|---|
| `GET /api/observations/changes` | Metadata changes, query matches, edits, expiry and removal events |
| `GET /api/observations/{id}` | Current reference and available `title`/`text`; unavailable bodies are null |
| `GET /api/coverage` | Source/query acquisition and durable landing attempt revisions, with resource counts |

Both paged routes accept `limit` (1–1000, default 200), `cursor`, `source`, `query`,
and an optional paired `profile_id`/`profile_version`. Profile fields bind the
continuation's context; they do not silently filter stored objects or attempts.
For example, `GET /api/observations/changes?source=bluesky&limit=100` returns:

```json
{
  "schema_version": "harken.observations.v1",
  "store_epoch": "store-generation",
  "scope_digest": "scope-digest",
  "scope": {"stream": "changes", "source": "bluesky", "query": null,
            "profile_id": null, "profile_version": null},
  "upper_seq": 12,
  "retained_after_seq": 0,
  "rows": [],
  "has_more": false,
  "next_cursor": null,
  "checkpoint": "opaque-signed-continuation"
}
```

Each change row has a monotonic `seq`, `kind`, stable `id`, evidence `revision`,
`source`, `source_id`, native `source_item_id` and `author_id` when known,
`identity_provenance`, display `url`/`author`, nullable `published_at` and its
`publication_provenance`, separate `indexed_at`/`source_updated_at`, immutable
first-ingestion `fetched_at`, `changed_at`, `availability`, current `queries`, and
`matched_queries` needed to deliver removals to scoped readers. Bodies and titles
are never copied into the journal. Query membership changes do not change an
unchanged evidence revision. Detail reads return the current revision, which may
be newer than the event; callers must compare it before attaching evidence or
annotations. An unknown or fully expired ID returns `observation_not_found` (404).

Bluesky identities use the source AT URI and author DID, independent of display
handle. Publication comes only from a valid timezone-aware `record.createdAt`;
`indexedAt` remains separate. HN publication comes from a valid `created_at_i`.
RSS preserves source publication/update provenance. Migration preserves existing
internal IDs: canonical HN item URLs can establish native IDs, and an exact RSS
URL can reconcile a newly available GUID only inside the same known feed.
Historical Bluesky handle URLs and unknown RSS feed identities remain
`legacy_uncertain`; the migration cannot establish their native identity.

Follow `next_cursor` until `has_more` is false, preserving the same scope. The
first page fixes `upper_seq`, so concurrent writes wait for the next run. Commit
each page with its `checkpoint` only after accepting that page durably. At the
end of the finite range, `checkpoint` resumes later changes, including late edits
to old publications. Cursors are opaque and signed; do not construct them from
timestamps. Repeated reads at the same cursor repeat the same sequence range.
Retention may remove metadata or make its detail unavailable during a replay.

Coverage uses stable `attempt_id` and append-only state revisions; the greatest
`seq` for each attempt is current. Count HTTP requests and other resources once
per attempt, using that latest revision. `acquisition` records
`completed|partial|failed|not_attempted`; `landing` independently records
`pending|stored|failed|not_applicable`. A crash can leave an honest pending landing.
The stored revision commits atomically with the collected rows and observation
changes. `execution` is partial while landing is pending and failed if landing
fails. `returned_count: 0` means a successful empty acquisition; a failed or
unattempted acquisition has null. `stored_count`, `landed_at`, actual
`http_requests`, `pages`, `retries`, status/backoff, bounds, window, method,
`truncated` and `completeness` distinguish storage, healthy zero, bounded results,
and unavailable coverage. Unmeasured counts remain null. Shared RSS cache reads
can return matches with zero new HTTP requests. `attempt_scope_digest` binds the
actual source/feed set, query, profile pair, method, window and limits;
`last_success_at` only inherits a completed acquisition and stored landing in
that exact scope. The older aggregate source metrics describe acquisition only.

Invalid continuations return `invalid_cursor` (400); scope, epoch or history
regression returns `cursor_scope_mismatch`, `cursor_epoch_mismatch`, or
`cursor_history_gap` (409); expired history returns `cursor_expired` (410).
Errors include `schema_version`, `code`, and `detail`. Invalid limits or an
unpaired profile context return 422. Preserve the previous checkpoint on errors
and explicitly select a bounded new baseline when history or scope changes.

Retention remains preview-first: `harken retention --db PATH` shows the proposed
30-day body and 90-day identity expiry measured from first ingestion. Explicit
`--yes` applies it. Identity expiry also scrubs identifying metadata from retained
older journal entries, including recent edits, while preserving minimal IDs,
revisions and query references for removal delivery. Removed IDs cannot restore
their evidence while their tombstone remains. Global retention also prunes
90-day change/attempt history and removed tombstones and advances the continuation
horizon. Source-scoped retention never prunes another source's history; use an
explicit global retention run to bound the shared journal.

For a host move, stop collection and serving, take a verified consistent backup,
copy it with the configuration, then start the new host. Preserve the store epoch
when moving the same complete history. When intentionally restoring an older
backup, rotate its epoch before starting readers to prevent silent cursor reuse:

```bash
uv run harken store-epoch --db restored.db
uv run harken store-epoch --db restored.db --rotate --expected INSPECTED_EPOCH
```

Rotation changes only the store generation and cursor signing key. It preserves
the observations and invalidates old continuations. Never rotate a live store
merely to repair a scope error. For this deployment's pinned controller, exact
stop/start/move commands live in the sibling
[cins Harken runbook](../cmdrvl-gtm/deploy/harken/README.md).

Brand-monitoring tools like Brand24 and Mention are capable — and closed, cloud-only, and now priced in the **hundreds of dollars per month**. They ingest everything you track into their servers. For indie founders, OSS maintainers, and privacy-conscious teams, that's often backwards.

Harken does the core job those tools do — **"what are people saying about X, and is it good or bad?"** — as a small open-source program you run yourself:

- **🏠 Self-hosted & local-first.** One SQLite file on your machine. No Harken telemetry or account required.
- **🔓 Open source (MIT).** Read it, fork it, extend it. No lock-in.
- **🆓 Free, and zero-config.** `harken demo` works on a clean clone with **no API key and no signup**.
- **🧩 LLM-agnostic.** Sentiment and themes work with **no model at all** (transparent local analysis). Want richer theme labels? Plug in Anthropic, OpenAI, *or a fully-local Ollama* — your choice, swappable in one env var.
- **🌐 Free sources first.** Hacker News and Bluesky are the no-credential defaults; Stack Overflow is another no-key option, while Reddit, Mastodon, RSS, X, and YouTube are available when configured.

## The problem

You shipped something. People are talking about it — on HN, in a subreddit, on Mastodon, on Bluesky, or in a Stack Overflow question. Some of it is praise, some of it is a bug report you'd really like to see, some of it is "is there an open alternative?". Manually checking every site each day doesn't scale, and the SaaS tools that automate it want a monthly subscription and a copy of all your data.

Harken is the small, honest, self-hosted version: point it at a keyword, and it aggregates the mentions, scores their sentiment, and clusters what people keep bringing up — locally.

## How it works

```
  sources ──▶ normalize ──▶ sentiment ──▶ themes ──▶ SQLite ──▶ dashboard / CLI
 HN, Reddit,   (one Mention   (local         (TF-based     (one file,   (web + terminal)
 Mastodon,      schema)        lexicon, or     clustering,   on your box)
 Bluesky, SO,                  optional LLM)   + optional LLM labels)
 RSS
```

1. **Sources** are pluggable adapters. The two defaults (Hacker News and Bluesky) need **no credentials**.
2. Each result is normalized into a common `Mention` and **de-duplicated** by content hash.
3. **Sentiment** is scored by a built-in lexicon analyzer — no key, no model download, runs instantly. An LLM is an *optional* upgrade, never a requirement.
4. **Themes** are extracted by clustering mentions on their shared salient terms ("pricing", "performance", "docs"…).
5. Everything lands in **SQLite** and renders in a local **web dashboard** and a **terminal report**.

## 30-second quickstart

Requires Python 3.10+. (Examples use [`uv`](https://github.com/astral-sh/uv); plain `pip` works too.)

```bash
git clone https://github.com/SaltIO/harken
cd harken
uv sync --locked

# 1. See the whole thing on bundled sample data — no key, no network:
harken demo
#    → loads a sample dataset, scores sentiment + themes,
#      and opens the dashboard at http://localhost:8042

# 2. Track something real (Hacker News + Bluesky, still no API key needed):
harken track "your-product"
harken serve            # open the dashboard at http://localhost:8042
```

That's it for the default sources. No account, key, or config file is required.

> Want richer theme names from an LLM? It's optional — copy `.env.example` to `.env` and set `HARKEN_LLM_PROVIDER` to `anthropic`, `openai`, or `ollama` (local). Everything still works without it.

### Keep listening

Run repeated scans in a terminal or under your service manager:

```bash
harken watch "your-product" --every 900
```

Each scan is de-duplicated into the same SQLite database, and a failed source does not stop the others. Add `--runs N` for a bounded job.

Harken keeps forward catch-up and historical cursors for each keyword/source pair. Fetch older result pages without re-walking the same history:

```bash
harken backfill "your-product" --pages 5
```

Cursor progress is committed only after the fetched mentions are stored, so an interrupted backfill safely resumes on the next run. Historical mentions do not trigger new-mention alerts. The dashboard also lets you add a keyword, choose sources, scan latest results, and request a three-page backfill without using the CLI.

### Projects

Group related keywords into named projects for combined sentiment, source, theme, timeline, and mention reporting. Existing databases migrate safely: legacy keywords appear in the protected `Default` project, while their original keyword URLs and reports keep working.

```bash
harken project create "Product Suite"
harken project list                         # note the numeric project ID
harken track "Product A" --project 2
harken project add 2 "Existing Keyword"
harken project report 2
```

The dashboard can create projects, add or remove existing keywords, scan a new keyword directly into a project, and switch between the combined project rollup and individual members. Removing a keyword from a project or deleting a project only removes the grouping; stored mentions and keyword tracking data are preserved.

Set `HARKEN_WEBHOOK_URL` to a generic HTTP endpoint or Slack incoming webhook to receive batches of newly ingested negative mentions. SMTP email delivery is also available by setting `HARKEN_EMAIL_TO`, `HARKEN_EMAIL_FROM`, and `HARKEN_SMTP_HOST`; optional port, TLS mode, and authentication variables are shown in [`.env.example`](.env.example). Webhook and email delivery can run together.

Delivery state is kept in SQLite independently for each target: successful alerts are de-duplicated and failed alerts remain queued for the next scan. Webhook URLs and SMTP passwords are treated as secrets and never stored in alert target identifiers or error messages.

Run `harken test-alert` to verify a configured webhook, or `harken test-alert --transport email` to send one clearly marked synthetic email without waiting for a real mention. Add `--kind volume` or `--kind sentiment` to test threshold notifications.

Optional volume and sentiment thresholds use every configured durable alert transport. They are disabled until explicitly configured:

```bash
HARKEN_ALERT_VOLUME_MULTIPLIER=2.0   # current window vs. baseline average
HARKEN_ALERT_SENTIMENT_DROP=0.25    # 25-point net-sentiment deterioration
HARKEN_ALERT_MIN_MENTIONS=5
HARKEN_ALERT_WINDOW_HOURS=24
HARKEN_ALERT_BASELINE_WINDOWS=7
HARKEN_ALERT_COOLDOWN_HOURS=24
```

The baseline is built from the preceding complete windows. A crossing opens one persisted alert episode per target: failed delivery is retried, repeated scans do not duplicate it, recovery re-arms it, and the cooldown prevents immediate re-alerting. Backfills never open threshold or new-negative episodes.

### Local accounts and roles

The dashboard remains account-free by default. For an opt-in multi-user deployment, create the first administrator in the same SQLite database and enable account mode:

```bash
harken user create admin                    # hidden password + confirmation prompt
harken user create analyst --role viewer
harken user create oncall --role operator
harken user list

HARKEN_AUTH_MODE=accounts harken serve
```

`viewer` can read dashboards, reports, mentions, and metrics. `operator` can also scan and manage project groupings. `admin` has operator access and is the protected account-administration role; the CLI refuses to disable, demote, or delete the last active admin. Password changes and account disabling revoke all of that user's sessions.

Passwords use salted PBKDF2-HMAC-SHA256 hashes, never reversible encryption. Browser sessions are opaque random tokens; only their SHA-256 hashes are stored, they expire after `HARKEN_SESSION_HOURS`, and cookies are HttpOnly with `SameSite=Strict`. Login attempts are rate-limited and errors do not reveal whether a username exists. For HTTPS deployments, set `HARKEN_SESSION_SECURE=true`. `/health` and the bundled login-page assets remain public; the dashboard, JSON APIs, and metrics require a valid session.

`harken user password`, `role`, `enable`, `disable`, and `delete --yes` cover the rest of the account lifecycle. Account roles are global to this Harken instance; project-level ownership or tenant isolation is intentionally not implied.

### Export and backup

```bash
harken export "your-product" --format json --output mentions.json
harken export --format csv --output all-mentions.csv
harken backup backups/harken.db
harken prune --older-than 90          # preview only
harken prune --older-than 90 --yes    # apply
```

Exports contain the complete stored result set rather than the dashboard's visible page. `harken backup` uses SQLite's online backup API, so it is consistent even while the dashboard is running; it refuses to overwrite a file unless `--force` is explicit. To restore, stop Harken and replace the configured database with a verified backup copy. Retention pruning is preview-only unless `--yes` is explicit and also removes the matching alert-delivery state.

### Docker

```bash
docker compose up --build -d
docker compose exec harken harken demo --no-serve
# dashboard: http://127.0.0.1:8042
```

The named volume keeps `/data/harken.db` across container replacements. The published port is loopback-only by default; optional local accounts or legacy Basic authentication are available for intentionally remote deployments.

Operational probes are available at `/health` (including a SQLite read/write check) and `/metrics` (Prometheus text format). Metrics include stored mention/query/alert counts plus durable per-source scan, error, retry, fetched-page, fetched-mention, and latency totals. Per-source counters live in SQLite, so they survive service restarts.

Set `HARKEN_LOG_FORMAT=json` for one JSON object per application event, suitable for Docker or a log collector. Source scan completion and retry events include the source, query, mode, outcome, item/page counts, retries, and duration; alert credentials are never logged. `HARKEN_LOG_LEVEL` accepts `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL`.

For an intentionally remote single-user deployment, setting both `HARKEN_AUTH_USERNAME` and `HARKEN_AUTH_PASSWORD` still enables the simpler backward-compatible Basic-auth mode. For multiple users, prefer `HARKEN_AUTH_MODE=accounts`. Both modes must be protected with HTTPS at the reverse proxy; account mode should also use `HARKEN_SESSION_SECURE=true`. The Compose default remains safer: loopback-only and unreachable from other hosts.

## Demo

`harken demo` — the full pipeline on bundled sample data, zero config:

<div align="center">
<img src="docs/cli-demo.svg" alt="harken demo terminal output" width="720">
</div>

…and the local dashboard it serves:

<div align="center">
<img src="docs/dashboard.png" alt="Harken dashboard — full view" width="760">
</div>

## Harken vs. the closed tools

Only ✅ items are **built today**. 🚧 = on the [roadmap](#roadmap).

| | **Harken** | Brand24 / Mention / Honestly |
|---|:---:|:---:|
| Self-hostable, runs on your box | ✅ | ❌ |
| Open source (MIT) | ✅ | ❌ |
| Local database; no Harken telemetry | ✅ | ❌ |
| Free | ✅ | ❌ (Brand24 starts around $199/mo billed annually) |
| Zero-config / no-key first run | ✅ | ❌ |
| LLM-agnostic (Anthropic / OpenAI / local Ollama / none) | ✅ | ❌ |
| Hacker News | ✅ | partial |
| Reddit | ✅ | ✅ |
| Mastodon / Bluesky | ✅ | partial |
| Stack Overflow / RSS / custom feeds | ✅ | partial |
| Sentiment analysis | ✅ | ✅ |
| Theme / topic clustering | ✅ | ✅ |
| Web dashboard + CLI | ✅ | ✅ (web) |
| X/Twitter and YouTube | ✅ (BYO key) | ✅ |
| TikTok and Instagram | 🚧 (BYO access) | ✅ |
| Slack / generic webhook alerts | ✅ | ✅ |
| Email alerts | ✅ | ✅ |
| Scheduled polling | ✅ | ✅ |
| Historical backfill | ✅ | ✅ |

**Honest take:** paid tools still offer broader coverage of locked-down platforms, hosted operations, and vendor support. Harken wins on openness, local storage, cost, and operator control — and it is small enough to audit and extend.

## Sources

| Source | Zero-config | Notes |
|--------|:-----------:|-------|
| Hacker News | ✅ | Public Algolia API — no key; service limit is 10,000 requests/hour/IP. |
| Bluesky | ✅ | Public AppView search, or authenticated PDS search when public search is blocked. |
| Stack Overflow | ✅ | Public Stack Exchange question search; Harken preserves API backoff and anonymous quota state. |
| X / Twitter | needs bearer token | X API v2 recent-post search; requires an X developer plan that includes recent search. |
| YouTube | needs API key | YouTube Data API v3 video search, ordered by publication time; provider quota applies. |
| Reddit | needs OAuth | Set a Reddit app client or access token; anonymous JSON search is no longer reliable. |
| Mastodon | usually needs token | Status full-text search depends on the instance and normally needs a user token. |
| RSS / Atom | needs feeds | Point it at any feed — a blog, a news site, or a Google Alerts RSS. |

Adding a source is one small class implementing `fetch()`. Cursor-aware sources can additionally implement `fetch_page()` — see [`src/harken/sources/`](src/harken/sources/).

## Configuration

Everything is optional and has a sane default — see [`.env.example`](.env.example). Highlights:

| Variable | Default | What it does |
|----------|---------|--------------|
| `HARKEN_SOURCES` | `hackernews,bluesky` | Which sources to query. |
| `HARKEN_DB` | `harken.db` | SQLite path. |
| `HARKEN_LOG_FORMAT` | `console` | Human-readable `console` or machine-readable `json` application logs. |
| `HARKEN_LOG_LEVEL` | `INFO` | Minimum application log severity. |
| `HARKEN_RETRIES` | `2` | Retries for network, HTTP 429, and HTTP 5xx failures. RSS reports feed failures for the next scheduled scan rather than immediately retrying the entire feed list. |
| `HARKEN_RETRY_BACKOFF` | `1.0` | Initial exponential-backoff delay in seconds. |
| `HARKEN_LLM_PROVIDER` | `none` | `none` \| `anthropic` \| `openai` \| `ollama`. |
| `HARKEN_SENTIMENT_ANALYZER` | `lexicon` | Transparent local `lexicon` or explicitly opt-in batched `llm`. |
| `HARKEN_RSS_FEEDS` | — | Comma-separated feed URLs. |
| `HARKEN_USER_AGENT` | Harken project User-Agent | Optional printable ASCII HTTP User-Agent, including your operator contact URL/email. |
| `HARKEN_BLUESKY_HANDLE` / `HARKEN_BLUESKY_APP_PASSWORD` | — | Optional full handle and app password for a Bluesky-hosted account; set both together. |
| `HARKEN_X_BEARER_TOKEN` | — | App-only bearer token for X API v2 recent search. |
| `HARKEN_YOUTUBE_API_KEY` | — | API key for YouTube Data API v3 video search. |
| `HARKEN_WEBHOOK_URL` | — | Generic or Slack webhook for mention and threshold alerts. |
| `HARKEN_EMAIL_TO` / `HARKEN_EMAIL_FROM` | — | Comma-separated recipients and sender for email alerts. |
| `HARKEN_SMTP_HOST` / `HARKEN_SMTP_PORT` | — / `587` | SMTP relay hostname and port. |
| `HARKEN_SMTP_SECURITY` | `starttls` | `starttls`, `ssl`, or `none` for a trusted local relay. |
| `HARKEN_SMTP_USERNAME` / `HARKEN_SMTP_PASSWORD` | — | Optional SMTP authentication pair. |
| `HARKEN_ALERT_VOLUME_MULTIPLIER` | `0` | Volume-spike ratio; `0` disables it. |
| `HARKEN_ALERT_SENTIMENT_DROP` | `0` | Net-sentiment deterioration threshold; `0` disables it. |
| `HARKEN_ALERT_WINDOW_HOURS` | `24` | Current and baseline window size. |
| `HARKEN_ALERT_BASELINE_WINDOWS` | `7` | Complete preceding windows used for the baseline. |
| `HARKEN_ALERT_MIN_MENTIONS` | `5` | Minimum current/baseline sample before evaluation. |
| `HARKEN_ALERT_COOLDOWN_HOURS` | `24` | Minimum delay before a recovered event may re-alert. |
| `HARKEN_AUTH_USERNAME` / `HARKEN_AUTH_PASSWORD` | — | Optional dashboard/API Basic authentication. |
| `HARKEN_AUTH_MODE` | inferred / `none` | `none`, legacy `basic`, or persisted multi-user `accounts`. |
| `HARKEN_SESSION_HOURS` | `12` | Account-session lifetime before re-authentication. |
| `HARKEN_SESSION_SECURE` | `false` | Set `true` when account mode is served through HTTPS. |

Reddit accepts `HARKEN_REDDIT_CLIENT_ID` plus `HARKEN_REDDIT_CLIENT_SECRET`, or an existing `HARKEN_REDDIT_ACCESS_TOKEN`. Mastodon accepts `HARKEN_MASTODON_ACCESS_TOKEN`. See [`.env.example`](.env.example) for the full set.

Bluesky public search may return 403 even when public profile lookup works. For authenticated search, create an app password in [Bluesky settings](https://bsky.app/settings/app-passwords), leaving direct-message access off. Set `HARKEN_BLUESKY_HANDLE` to the full handle (for example, `example.bsky.social`) and `HARKEN_BLUESKY_APP_PASSWORD` to that password in your private environment. Do not commit credentials. Harken logs in once per source instance through `bsky.social`, then proxies read-only search through the PDS returned by the session. The login path supports Bluesky-hosted accounts only; custom PDS hosts are rejected. Sessions stay in memory and failed logins are not repeated for each term in a batch. See the official [authentication](https://atproto.com/guides/sdk-auth) and [API routing](https://bsky.network/docs/api-directory/) documentation.

X accepts `HARKEN_X_BEARER_TOKEN`; YouTube accepts `HARKEN_YOUTUBE_API_KEY`. Both adapters support incremental time boundaries and persisted pagination cursors. Harken never logs either credential. See the official [X recent-search documentation](https://docs.x.com/x-api/posts/search/integrate/overview) and [YouTube `search.list` reference](https://developers.google.com/youtube/v3/docs/search/list) for access and quota requirements.

Harken stores its database locally and sends no telemetry. Tracking necessarily sends your search term to the source APIs you enable. Selecting Anthropic or OpenAI for optional theme labels also sends a small sample of mention text to that provider; the default local analyzer and Ollama do not.

### Analyzer evaluation

The zero-config lexicon analyzer has a reproducible quality report rather than an unmeasured accuracy claim:

```bash
harken evaluate
harken evaluate --format json
harken evaluate --dataset my-labeled-examples.json
harken evaluate --min-accuracy 0.95       # useful as a CI quality gate
```

On the bundled v1 dataset it currently scores **96.7% accuracy** and **96.6% macro-F1** across 60 balanced positive, neutral, and negative product-monitoring examples. The CC0 dataset is hand-authored and intentionally shipped with the project; it is a regression suite, not an independent benchmark. The two retained failures are sarcastic sentences, a known limitation that remains visible in every report. Custom datasets use the same documented JSON structure as [`sentiment_v1.json`](src/harken/evaluation/sentiment_v1.json).

For operators who explicitly prefer provider-based classification, set `HARKEN_SENTIMENT_ANALYZER=llm` together with a configured `HARKEN_LLM_PROVIDER`. Harken classifies in batches of 25, treats mention text as untrusted prompt data, requires a complete JSON response with bounded scores, and automatically reprocesses the whole batch with the local lexicon if anything is unavailable or malformed. This mode may incur provider cost and sends up to 1,000 characters of every fetched mention to that provider; the default `lexicon` mode sends nothing.

## Roadmap

Built today is everything marked ✅ above. Production-readiness work, in priority order:

- [x] **Scheduled polling** via `harken watch` (run it under your service manager).
- [x] **Docker / compose** one-liner.
- [x] **Durable webhook / Slack alerts** for newly ingested negative mentions.
- [x] **Durable webhook threshold alerts** for sentiment deterioration and volume spikes.
- [x] **Email delivery** for mention and threshold alerts.
- [x] **Historical backfill and per-keyword/source pagination cursors**.
- [x] **Add/track keywords, choose sources, and start scans from the web UI**.
- [x] **Named multi-keyword projects** and project-level reporting.
- [x] **Optional HTTP Basic authentication** for intentionally remote single-user deployments.
- [x] **CSRF-protected web mutations** for single- and multi-user dashboards.
- [x] **Opt-in multi-user accounts and global viewer/operator/admin RBAC**.
- [x] **JSON/CSV exports and consistent online backup**, with documented restore steps.
- [x] **Preview-first retention pruning** with query scoping and alert-state cleanup.
- [x] **Bounded polling retries** with exponential backoff and `Retry-After` handling.
- [x] **Persisted operational metrics** and a database-aware health probe.
- [x] **Structured logs and per-source latency/error metrics**.
- [x] **Stack Overflow** via the public Stack Exchange API.
- [x] **X/Twitter and YouTube keyed sources** with incremental boundaries and pagination.
- [ ] **Additional locked-platform sources** such as TikTok or Instagram, subject to operator-selected API access.
- [x] **Measured analyzer evaluation dataset, reports, and CI quality gate**.
- [x] **Optional batched LLM sentiment** with strict validation and local fallback.

## Contributing

Issues and PRs are welcome — new source adapters, a better sentiment lexicon, and roadmap items especially. Run the suite with:

```bash
uv pip install -e ".[dev]"
pytest -q && ruff check src tests
```

## License

MIT © Harken contributors. See [LICENSE](LICENSE).
