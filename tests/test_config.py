"""Tests for .env loading and env-var driven configuration."""

import importlib
import os

import pytest

import harken.config as config


def test_dotenv_in_cwd_is_picked_up(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("HARKEN_LLM_PROVIDER", raising=False)
    (tmp_path / ".env").write_text("HARKEN_LLM_PROVIDER=anthropic\n")
    try:
        importlib.reload(config)
        assert config.Config().llm_provider == "anthropic"
    finally:
        # load_dotenv() mutates os.environ directly, bypassing monkeypatch's
        # own undo tracking, so clean it up by hand rather than relying on
        # monkeypatch's (stacked, order-sensitive) teardown for this one.
        os.environ.pop("HARKEN_LLM_PROVIDER", None)


def test_dotenv_lookup_uses_cwd_not_package_location(tmp_path, monkeypatch):
    # config.py lives inside the installed package; .env must be resolved
    # against wherever the user *runs* harken from, not the package's own
    # directory (find_dotenv() defaults to the latter and would silently
    # ignore a project-local .env once harken is pip-installed).
    monkeypatch.chdir(tmp_path)
    found = config.find_dotenv(usecwd=True)
    assert found == "" or found.startswith(str(tmp_path))


def test_config_defaults_without_env():
    cfg = config.Config()
    assert cfg.db_path == "harken.db"
    assert cfg.sources == ["hackernews", "bluesky"]
    assert cfg.llm_provider == "none"
    assert cfg.alert_volume_multiplier == 0.0
    assert cfg.alert_sentiment_drop == 0.0
    assert cfg.log_format == "console"
    assert cfg.log_level == "INFO"
    assert cfg.email_to == []
    assert cfg.sentiment_analyzer == "lexicon"
    assert cfg.auth_mode == "none"
    assert cfg.session_hours == 12
    assert not cfg.session_secure


def test_limit_must_be_positive(monkeypatch):
    monkeypatch.setenv("HARKEN_LIMIT", "0")
    with pytest.raises(ValueError, match="at least 1"):
        config.Config()


def test_blank_env_vars_fall_back_to_defaults(monkeypatch):
    # A set-but-empty var (HARKEN_X=) must be treated as unset, not crash
    # startup on int('') / choice validation.
    for name in (
        "HARKEN_SMTP_PORT",
        "HARKEN_LIMIT",
        "HARKEN_LOG_FORMAT",
        "HARKEN_LOG_LEVEL",
        "HARKEN_SESSION_SECURE",
        "HARKEN_AUTH_MODE",
    ):
        monkeypatch.setenv(name, "")
    cfg = config.Config()
    assert cfg.smtp_port == 587
    assert cfg.log_format == "console"
    assert cfg.log_level == "INFO"
    assert cfg.session_secure is False
    assert cfg.auth_mode == "none"


def test_webhook_url_is_loaded(monkeypatch):
    monkeypatch.setenv("HARKEN_WEBHOOK_URL", "https://alerts.example.test/harken")
    assert config.Config().webhook_url == "https://alerts.example.test/harken"


def test_auth_credentials_are_loaded(monkeypatch):
    monkeypatch.setenv("HARKEN_AUTH_USERNAME", "admin")
    monkeypatch.setenv("HARKEN_AUTH_PASSWORD", "secret")
    cfg = config.Config()
    assert (cfg.auth_username, cfg.auth_password) == ("admin", "secret")
    assert cfg.auth_mode == "basic"


def test_account_auth_settings_are_loaded(monkeypatch):
    monkeypatch.setenv("HARKEN_AUTH_MODE", "accounts")
    monkeypatch.setenv("HARKEN_SESSION_HOURS", "24")
    monkeypatch.setenv("HARKEN_SESSION_SECURE", "true")
    cfg = config.Config()
    assert (cfg.auth_mode, cfg.session_hours, cfg.session_secure) == ("accounts", 24, True)


def test_keyed_source_credentials_are_loaded_and_routed(monkeypatch):
    monkeypatch.setenv("HARKEN_X_BEARER_TOKEN", "x-token")
    monkeypatch.setenv("HARKEN_YOUTUBE_API_KEY", "youtube-key")
    cfg = config.Config()
    assert cfg.source_options("x") == {"bearer_token": "x-token"}
    assert cfg.source_options("youtube") == {"api_key": "youtube-key"}


def test_email_delivery_settings_are_loaded(monkeypatch):
    monkeypatch.setenv("HARKEN_EMAIL_TO", "ops@example.test, owner@example.test")
    monkeypatch.setenv("HARKEN_EMAIL_FROM", "harken@example.test")
    monkeypatch.setenv("HARKEN_SMTP_HOST", "smtp.example.test")
    monkeypatch.setenv("HARKEN_SMTP_PORT", "465")
    monkeypatch.setenv("HARKEN_SMTP_SECURITY", "ssl")
    monkeypatch.setenv("HARKEN_SMTP_USERNAME", "mailer")
    monkeypatch.setenv("HARKEN_SMTP_PASSWORD", "secret")
    cfg = config.Config()
    assert cfg.email_to == ["ops@example.test", "owner@example.test"]
    assert (cfg.email_from, cfg.smtp_host, cfg.smtp_port, cfg.smtp_security) == (
        "harken@example.test",
        "smtp.example.test",
        465,
        "ssl",
    )
    assert (cfg.smtp_username, cfg.smtp_password) == ("mailer", "secret")


def test_bluesky_credentials_are_routed_and_not_in_repr(monkeypatch):
    monkeypatch.setenv("HARKEN_BLUESKY_HANDLE", "test.bsky.social")
    monkeypatch.setenv("HARKEN_BLUESKY_APP_PASSWORD", "app-password-secret")
    cfg = config.Config()
    assert cfg.source_options("bluesky") == {
        "handle": "test.bsky.social", "app_password": "app-password-secret"}
    assert "app-password-secret" not in repr(cfg)
    assert "test.bsky.social" not in repr(cfg)


@pytest.mark.parametrize("name", ["HARKEN_BLUESKY_HANDLE", "HARKEN_BLUESKY_APP_PASSWORD"])
def test_bluesky_partial_credentials_rejected(monkeypatch, name):
    monkeypatch.delenv("HARKEN_BLUESKY_HANDLE", raising=False)
    monkeypatch.delenv("HARKEN_BLUESKY_APP_PASSWORD", raising=False)
    monkeypatch.setenv(name, "secret")
    with pytest.raises(ValueError, match="must be set together"):
        config.Config()


def test_llm_sentiment_is_explicitly_opt_in(monkeypatch):
    monkeypatch.setenv("HARKEN_SENTIMENT_ANALYZER", "llm")
    assert config.Config().sentiment_analyzer == "llm"


def test_partial_email_configuration_is_rejected(monkeypatch):
    monkeypatch.setenv("HARKEN_EMAIL_TO", "ops@example.test")
    with pytest.raises(ValueError, match="must be set together"):
        config.Config()


def test_threshold_alert_settings_are_loaded(monkeypatch):
    monkeypatch.setenv("HARKEN_ALERT_WINDOW_HOURS", "12")
    monkeypatch.setenv("HARKEN_ALERT_BASELINE_WINDOWS", "4")
    monkeypatch.setenv("HARKEN_ALERT_MIN_MENTIONS", "8")
    monkeypatch.setenv("HARKEN_ALERT_VOLUME_MULTIPLIER", "2.5")
    monkeypatch.setenv("HARKEN_ALERT_SENTIMENT_DROP", "0.3")
    monkeypatch.setenv("HARKEN_ALERT_COOLDOWN_HOURS", "6")
    cfg = config.Config()
    assert (
        cfg.alert_window_hours,
        cfg.alert_baseline_windows,
        cfg.alert_min_mentions,
        cfg.alert_volume_multiplier,
        cfg.alert_sentiment_drop,
        cfg.alert_cooldown_hours,
    ) == (12, 4, 8, 2.5, 0.3, 6)


@pytest.mark.parametrize(
    ("name", "value", "message"),
    [
        ("HARKEN_RETRIES", "-1", "at least 0"),
        ("HARKEN_RETRY_BACKOFF", "0", "greater than 0"),
        ("HARKEN_ALERT_WINDOW_HOURS", "0", "at least 1"),
        ("HARKEN_ALERT_VOLUME_MULTIPLIER", "-1", "at least 0"),
        ("HARKEN_ALERT_SENTIMENT_DROP", "nope", "must be a number"),
        ("HARKEN_LOG_FORMAT", "xml", "must be one of"),
        ("HARKEN_LOG_LEVEL", "trace", "must be one of"),
        ("HARKEN_SMTP_SECURITY", "sometimes", "must be one of"),
        ("HARKEN_SMTP_PORT", "70000", "at most 65535"),
        ("HARKEN_SENTIMENT_ANALYZER", "magic", "must be one of"),
        ("HARKEN_AUTH_MODE", "oauth", "must be one of"),
        ("HARKEN_SESSION_SECURE", "maybe", "must be true or false"),
    ],
)
def test_retry_settings_are_validated(monkeypatch, name, value, message):
    monkeypatch.setenv(name, value)
    with pytest.raises(ValueError, match=message):
        config.Config()
