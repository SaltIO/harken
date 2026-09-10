"""Runtime configuration with zero-credential defaults and optional keyed sources."""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from dotenv import find_dotenv, load_dotenv

# usecwd=True: look for .env in the directory harken is *run* from, not the
# installed package's location (find_dotenv defaults to searching upward from
# this source file, which is wrong once harken is pip-installed).
load_dotenv(find_dotenv(usecwd=True))


def _env_list(name: str) -> list[str]:
    raw = os.getenv(name, "")
    return [x.strip() for x in raw.split(",") if x.strip()]


def _clean_env(name: str) -> str | None:
    """Return the env var's stripped value, or None if it is unset or blank.

    A set-but-empty var (``HARKEN_X=``) is treated as unset so it falls back to
    the documented default instead of crashing ``int('')`` / choice validation.
    """
    raw = os.getenv(name)
    if raw is None:
        return None
    stripped = raw.strip()
    return stripped or None


def _positive_env_int(name: str, default: int) -> int:
    raw = _clean_env(name)
    try:
        value = int(raw) if raw is not None else default
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer (got {raw!r})") from exc
    if value < 1:
        raise ValueError(f"{name} must be at least 1 (got {value})")
    return value


def _nonnegative_env_int(name: str, default: int) -> int:
    raw = _clean_env(name)
    try:
        value = int(raw) if raw is not None else default
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer (got {raw!r})") from exc
    if value < 0:
        raise ValueError(f"{name} must be at least 0 (got {value})")
    return value


def _positive_env_float(name: str, default: float) -> float:
    raw = _clean_env(name)
    try:
        value = float(raw) if raw is not None else default
    except ValueError as exc:
        raise ValueError(f"{name} must be a number (got {raw!r})") from exc
    if value <= 0:
        raise ValueError(f"{name} must be greater than 0 (got {value})")
    return value


def _nonnegative_env_float(name: str, default: float) -> float:
    raw = _clean_env(name)
    try:
        value = float(raw) if raw is not None else default
    except ValueError as exc:
        raise ValueError(f"{name} must be a number (got {raw!r})") from exc
    if value < 0:
        raise ValueError(f"{name} must be at least 0 (got {value})")
    return value


def _choice_env(name: str, default: str, choices: set[str]) -> str:
    value = (_clean_env(name) or default).lower()
    if value not in choices:
        expected = ", ".join(sorted(choices))
        raise ValueError(f"{name} must be one of {expected} (got {value!r})")
    return value


def _bool_env(name: str, default: bool = False) -> bool:
    raw = _clean_env(name)
    if raw is None:
        return default
    value = raw.lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be true or false (got {raw!r})")


def _auth_mode_env() -> str:
    explicit = _clean_env("HARKEN_AUTH_MODE")
    if explicit is not None:
        return _choice_env("HARKEN_AUTH_MODE", "none", {"accounts", "basic", "none"})
    if os.getenv("HARKEN_AUTH_USERNAME") and os.getenv("HARKEN_AUTH_PASSWORD"):
        return "basic"
    return "none"


def _log_level_env() -> str:
    value = (_clean_env("HARKEN_LOG_LEVEL") or "INFO").upper()
    if value == "WARN":
        value = "WARNING"
    choices = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
    if value not in choices:
        expected = ", ".join(sorted(choices))
        raise ValueError(f"HARKEN_LOG_LEVEL must be one of {expected} (got {value!r})")
    return value


@dataclass
class Config:
    db_path: str = field(default_factory=lambda: os.getenv("HARKEN_DB", "harken.db"))
    log_format: str = field(
        default_factory=lambda: _choice_env("HARKEN_LOG_FORMAT", "console", {"console", "json"})
    )
    log_level: str = field(default_factory=_log_level_env)
    user_agent: str | None = field(default_factory=lambda: _clean_env("HARKEN_USER_AGENT"))
    # which sources to query (default = zero-config ones)
    sources: list[str] = field(
        default_factory=lambda: _env_list("HARKEN_SOURCES") or ["hackernews", "bluesky"]
    )
    per_source_limit: int = field(default_factory=lambda: _positive_env_int("HARKEN_LIMIT", 50))
    source_retries: int = field(default_factory=lambda: _nonnegative_env_int("HARKEN_RETRIES", 2))
    retry_backoff: float = field(
        default_factory=lambda: _positive_env_float("HARKEN_RETRY_BACKOFF", 1.0)
    )
    sentiment_analyzer: str = field(
        default_factory=lambda: _choice_env(
            "HARKEN_SENTIMENT_ANALYZER", "lexicon", {"lexicon", "llm"}
        )
    )
    llm_provider: str = field(default_factory=lambda: os.getenv("HARKEN_LLM_PROVIDER", "none"))
    # source-specific options
    mastodon_instance: str = field(
        default_factory=lambda: os.getenv("HARKEN_MASTODON_INSTANCE", "mastodon.social")
    )
    mastodon_access_token: str | None = field(
        default_factory=lambda: os.getenv("HARKEN_MASTODON_ACCESS_TOKEN") or None
    )
    reddit_client_id: str | None = field(
        default_factory=lambda: os.getenv("HARKEN_REDDIT_CLIENT_ID") or None
    )
    reddit_client_secret: str | None = field(
        default_factory=lambda: os.getenv("HARKEN_REDDIT_CLIENT_SECRET") or None
    )
    reddit_access_token: str | None = field(
        default_factory=lambda: os.getenv("HARKEN_REDDIT_ACCESS_TOKEN") or None
    )
    x_bearer_token: str | None = field(
        default_factory=lambda: os.getenv("HARKEN_X_BEARER_TOKEN") or None
    )
    youtube_api_key: str | None = field(
        default_factory=lambda: os.getenv("HARKEN_YOUTUBE_API_KEY") or None
    )
    rss_feeds: list[str] = field(default_factory=lambda: _env_list("HARKEN_RSS_FEEDS"))
    webhook_url: str | None = field(default_factory=lambda: os.getenv("HARKEN_WEBHOOK_URL") or None)
    email_to: list[str] = field(default_factory=lambda: _env_list("HARKEN_EMAIL_TO"))
    email_from: str | None = field(default_factory=lambda: os.getenv("HARKEN_EMAIL_FROM") or None)
    smtp_host: str | None = field(default_factory=lambda: os.getenv("HARKEN_SMTP_HOST") or None)
    smtp_port: int = field(default_factory=lambda: _positive_env_int("HARKEN_SMTP_PORT", 587))
    smtp_security: str = field(
        default_factory=lambda: _choice_env(
            "HARKEN_SMTP_SECURITY", "starttls", {"none", "ssl", "starttls"}
        )
    )
    smtp_username: str | None = field(
        default_factory=lambda: os.getenv("HARKEN_SMTP_USERNAME") or None
    )
    smtp_password: str | None = field(
        default_factory=lambda: os.getenv("HARKEN_SMTP_PASSWORD") or None
    )
    alert_window_hours: int = field(
        default_factory=lambda: _positive_env_int("HARKEN_ALERT_WINDOW_HOURS", 24)
    )
    alert_baseline_windows: int = field(
        default_factory=lambda: _positive_env_int("HARKEN_ALERT_BASELINE_WINDOWS", 7)
    )
    alert_min_mentions: int = field(
        default_factory=lambda: _positive_env_int("HARKEN_ALERT_MIN_MENTIONS", 5)
    )
    alert_volume_multiplier: float = field(
        default_factory=lambda: _nonnegative_env_float("HARKEN_ALERT_VOLUME_MULTIPLIER", 0.0)
    )
    alert_sentiment_drop: float = field(
        default_factory=lambda: _nonnegative_env_float("HARKEN_ALERT_SENTIMENT_DROP", 0.0)
    )
    alert_cooldown_hours: int = field(
        default_factory=lambda: _nonnegative_env_int("HARKEN_ALERT_COOLDOWN_HOURS", 24)
    )
    auth_username: str | None = field(
        default_factory=lambda: os.getenv("HARKEN_AUTH_USERNAME") or None
    )
    auth_password: str | None = field(
        default_factory=lambda: os.getenv("HARKEN_AUTH_PASSWORD") or None
    )
    auth_mode: str = field(default_factory=_auth_mode_env)
    session_hours: int = field(
        default_factory=lambda: _positive_env_int("HARKEN_SESSION_HOURS", 12)
    )
    session_secure: bool = field(default_factory=lambda: _bool_env("HARKEN_SESSION_SECURE", False))

    def __post_init__(self) -> None:
        if self.user_agent is not None and (
            not self.user_agent.isascii()
            or any(ord(char) < 32 or ord(char) == 127 for char in self.user_agent)
        ):
            raise ValueError("HARKEN_USER_AGENT must contain printable ASCII characters")
        if self.smtp_port > 65535:
            raise ValueError(f"HARKEN_SMTP_PORT must be at most 65535 (got {self.smtp_port})")
        email_values = bool(
            self.email_to
            or self.email_from
            or self.smtp_host
            or self.smtp_username
            or self.smtp_password
        )
        if email_values and not (self.email_to and self.email_from and self.smtp_host):
            raise ValueError(
                "HARKEN_EMAIL_TO, HARKEN_EMAIL_FROM, and HARKEN_SMTP_HOST must be set together"
            )
        if bool(self.smtp_username) != bool(self.smtp_password):
            raise ValueError("HARKEN_SMTP_USERNAME and HARKEN_SMTP_PASSWORD must be set together")
        if bool(self.auth_username) != bool(self.auth_password):
            raise ValueError("HARKEN_AUTH_USERNAME and HARKEN_AUTH_PASSWORD must be set together")
        if self.auth_mode == "basic" and not (self.auth_username and self.auth_password):
            raise ValueError(
                "basic auth mode requires HARKEN_AUTH_USERNAME and HARKEN_AUTH_PASSWORD"
            )
        if self.auth_mode != "basic" and (self.auth_username or self.auth_password):
            raise ValueError(
                "HARKEN_AUTH_USERNAME and HARKEN_AUTH_PASSWORD require basic auth mode"
            )

    def source_options(self, name: str) -> dict:
        if name == "mastodon":
            return {
                "instance": self.mastodon_instance,
                "access_token": self.mastodon_access_token,
            }
        if name == "reddit":
            return {
                "client_id": self.reddit_client_id,
                "client_secret": self.reddit_client_secret,
                "access_token": self.reddit_access_token,
            }
        if name == "rss":
            return {"feeds": self.rss_feeds}
        if name == "x":
            return {"bearer_token": self.x_bearer_token}
        if name == "youtube":
            return {"api_key": self.youtube_api_key}
        return {}
