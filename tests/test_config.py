from __future__ import annotations

import importlib

import pytest

def test_load_settings_reads_env(monkeypatch) -> None:
    monkeypatch.setenv("DELTA_API_KEY", "key")
    monkeypatch.setenv("DELTA_API_SECRET", "secret")
    monkeypatch.setenv("DELTA_BASE_URL", "https://example.com")
    monkeypatch.setenv("DELTA_SSL_VERIFY", "true")
    monkeypatch.setenv("POLL_INTERVAL_SECONDS", "3")
    monkeypatch.setenv("PNL_PROFIT_THRESHOLD_USD", "12")
    monkeypatch.setenv("PNL_LOSS_THRESHOLD_USD", "-8")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("BTC_INDEX_PRICE_TRIGGER", "70000")

    import src.config as config_module
    importlib.reload(config_module)

    settings = config_module.load_settings()

    assert settings.__class__.__name__ == "Settings"
    assert settings.api_key == "key"
    assert settings.api_secret == "secret"
    assert settings.base_url == "https://example.com"
    assert settings.ssl_verify is True
    assert settings.poll_interval_seconds == 3.0
    assert settings.profit_exit_threshold_usd == 12.0
    assert settings.loss_exit_threshold_usd == -8.0
    assert settings.log_level == "DEBUG"
    assert settings.index_price_trigger == 70000.0


def test_load_settings_requires_credentials(monkeypatch) -> None:
    import src.config as config_module
    importlib.reload(config_module)

    def fake_getenv(name: str, default: str = "") -> str:
        if name in {"DELTA_API_KEY", "DELTA_API_SECRET"}:
            return ""
        return default

    monkeypatch.setattr(config_module.os, "getenv", fake_getenv)

    with pytest.raises(ValueError, match="DELTA_API_KEY"):
        config_module.load_settings()
