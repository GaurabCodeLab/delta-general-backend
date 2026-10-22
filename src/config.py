from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


load_dotenv()


def _parse_ratio_list(raw: str) -> list[float]:
    ratios: list[float] = []
    for token in (raw or "").split(","):
        token = token.strip()
        if not token:
            continue

        if ":" in token:
            left, right = token.split(":", 1)
            try:
                left_value = float(left.strip())
                right_value = float(right.strip())
            except ValueError:
                continue
            if left_value <= 0 or right_value <= 0:
                continue
            ratios.append(right_value / left_value)
            continue

        try:
            ratios.append(float(token))
        except ValueError:
            continue

    if not ratios:
        raise ValueError(
            "RATIO_SPREADS must contain at least one ratio, e.g. '1:2' or '2,3,5'."
        )
    return ratios


@dataclass(slots=True)
class Settings:
    api_key: str
    api_secret: str
    base_url: str
    poll_interval_seconds: float
    profit_exit_threshold_usd: float
    loss_exit_threshold_usd: float
    ratio_spreads: list[float]
    log_level: str
    ssl_verify: bool
    index_price_trigger: float | None = None


def load_settings() -> Settings:
    api_key = os.getenv("DELTA_API_KEY", "")
    api_secret = os.getenv("DELTA_API_SECRET", "")
    if not api_key or not api_secret:
        raise ValueError("Missing DELTA_API_KEY or DELTA_API_SECRET in environment.")

    return Settings(
        api_key=api_key,
        api_secret=api_secret,
        base_url=os.getenv("DELTA_BASE_URL", "https://cdn-ind.testnet.deltaex.org"),
        poll_interval_seconds=float(os.getenv("POLL_INTERVAL_SECONDS", "2")),
        profit_exit_threshold_usd=float(os.getenv("PNL_PROFIT_THRESHOLD_USD", "10")),
        loss_exit_threshold_usd=float(os.getenv("PNL_LOSS_THRESHOLD_USD", "-20")),
        ratio_spreads=_parse_ratio_list(os.getenv("RATIO_SPREADS", "1:2")),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        ssl_verify=os.getenv("DELTA_SSL_VERIFY", "false").strip().lower() in {"1", "true", "yes", "y"},
        index_price_trigger=(
            float(os.environ["BTC_INDEX_PRICE_TRIGGER"])
            if os.getenv("BTC_INDEX_PRICE_TRIGGER", "").strip()
            else None
        ),
    )
