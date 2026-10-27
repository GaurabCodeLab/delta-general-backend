from __future__ import annotations

import asyncio
import importlib
import logging
import runpy
import sys
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from src.exchange_client import DeltaExchangeClient, ExchangeClientError
from src.models import OptionLeg, Quote
from src.order_executor import OrderExecutor
from src.position_manager import PositionManager


class FakeResponse:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def json(self) -> dict:
        return self._payload


class FakeDeltaRestClient:
    def __init__(self, base_url: str, api_key: str, api_secret: str) -> None:
        self.session = SimpleNamespace(verify=True)
        self.base_url = base_url
        self.api_key = api_key
        self.api_secret = api_secret

    def get_ticker(self, **kwargs):
        identifier = kwargs.get("identifier")
        if identifier == "BTCUSD":
            return {"spot_price": "123.45"}
        return {
            "quotes": {
                "best_bid": 101.0,
                "best_ask": 102.0,
            }
        }

    def request(self, **kwargs):
        path = kwargs.get("path")
        method = kwargs.get("method")
        auth = kwargs.get("auth")
        query = kwargs.get("query")

        if method == "GET" and path == "/v2/positions/margined" and auth:
            return FakeResponse(
                {
                    "success": True,
                    "result": [
                        {
                            "product_id": 1001,
                            "size": 1.0,
                            "entry_price": 100.0,
                            "product": {
                                "symbol": "BTC-240930-C-100000",
                                "contract_type": "option_call",
                                "strike_price": 100000.0,
                                "settlement_time": "2024-09-30T00:00:00Z",
                                "contract_value": 0.001,
                            },
                        },
                        {
                            "product_id": 1002,
                            "size": -2.0,
                            "entry_price": 101.0,
                            "product": {
                                "symbol": "BTC-240930-P-100000",
                                "contract_type": "option_put",
                                "strike_price": 100000.0,
                                "settlement_time": "2024-09-30T00:00:00Z",
                                "contract_value": 0.001,
                            },
                        },
                    ],
                }
            )

        if method == "GET" and path == "/v2/products" and auth:
            if query and query.get("symbol") == "BTC-240930-C-100000":
                return FakeResponse(
                    {
                        "success": True,
                        "result": [
                            {
                                "id": 1001,
                                "symbol": "BTC-240930-C-100000",
                            }
                        ],
                    }
                )
            return FakeResponse(
                {
                    "success": True,
                    "result": [
                        {"id": 1001, "symbol": "BTC-240930-C-100000"},
                        {"id": 1002, "symbol": "BTC-240930-P-100000"},
                    ],
                }
            )

        if method == "GET" and path == "/v2/products" and not query and auth:
            return FakeResponse(
                {
                    "success": True,
                    "result": [
                        {"id": 1001, "symbol": "BTC-240930-C-100000"},
                        {"id": 1002, "symbol": "BTC-240930-P-100000"},
                    ],
                }
            )

        return FakeResponse({"success": True, "result": []})

    def place_order(self, **kwargs):
        return {"result": {"id": "ord-123"}}

    def order_history(self, **kwargs):
        return {"result": [{"id": "ord-123", "product_id": kwargs.get("query", {}).get("product_id")} ]}

    def fills(self, query=None, page_size=100, after=None):
        return {"success": True, "result": [], "meta": {}}


@pytest.fixture
def fake_delta_client_module(monkeypatch):
    fake_module = SimpleNamespace(
        DeltaRestClient=FakeDeltaRestClient,
        OrderType=SimpleNamespace(MARKET="MARKET"),
        TimeInForce=SimpleNamespace(IOC="IOC"),
    )
    monkeypatch.setitem(sys.modules, "delta_rest_client", fake_module)
    return fake_module


@pytest.fixture
def api_module(monkeypatch):
    monkeypatch.setenv("DELTA_API_KEY", "key")
    monkeypatch.setenv("DELTA_API_SECRET", "secret")
    monkeypatch.setenv("DELTA_BASE_URL", "https://example.com")
    monkeypatch.setenv("DELTA_SSL_VERIFY", "false")
    monkeypatch.setenv("POLL_INTERVAL_SECONDS", "0")
    monkeypatch.setenv("PNL_PROFIT_THRESHOLD_USD", "10")
    monkeypatch.setenv("PNL_LOSS_THRESHOLD_USD", "-20")
    monkeypatch.setenv("LOG_LEVEL", "INFO")

    class FakeExchange:
        def __init__(self, *args, **kwargs) -> None:
            pass

        async def get_index_price(self, symbol: str) -> float:
            return 100.0

        async def parse_option_positions(self):
            return []

        async def get_best_quote(self, product_id: int, symbol: str | None = None):
            return Quote(best_bid=100.0, best_ask=101.0)

    import src.exchange_client as exchange_client_module
    exchange_client_module.DeltaExchangeClient = FakeExchange

    sys.modules.pop("src.api", None)
    api_module = importlib.import_module("src.api")
    return api_module


@pytest.mark.asyncio
async def test_exchange_client_runtime_paths_are_covered(fake_delta_client_module) -> None:
    exchange = DeltaExchangeClient(
        api_key="k",
        api_secret="s",
        base_url="https://example.com",
        ssl_verify=False,
    )

    assert await exchange.get_index_price("BTCUSD") == pytest.approx(123.45)

    assert await exchange.get_open_positions_raw() != []
    assert await exchange.get_position_size(1001) == pytest.approx(1.0)

    quote = await exchange.get_best_quote(1001)
    assert quote == Quote(best_bid=101.0, best_ask=102.0)

    order_resp = await exchange.place_market_order(1001, "buy", 1.0, reduce_only=False)
    assert order_resp["result"]["id"] == "ord-123"

    legs = await exchange.parse_option_positions()
    assert len(legs) == 2
    assert [leg.option_type for leg in legs] == ["call", "put"]


@pytest.mark.asyncio
async def test_exchange_client_uses_realtime_positions_and_symbol_tickers(fake_delta_client_module) -> None:
    exchange = DeltaExchangeClient("k", "s", "https://example.com")

    class RealtimeClient:
        def __init__(self) -> None:
            self.ticker_identifier = None

        def get_position(self, product_id: int):
            return {"size": -2.5}

        def get_ticker(self, identifier, auth=False):
            self.ticker_identifier = identifier
            return {"quotes": {"best_bid": "1.0", "best_ask": "2.0"}}

    client = RealtimeClient()
    exchange._client = client

    assert await exchange.get_position_size(1001) == pytest.approx(-2.5)
    quote = await exchange.get_best_quote(1001, "BTC-OPTION-SYMBOL")
    assert quote == Quote(best_bid=1.0, best_ask=2.0)
    assert client.ticker_identifier == "BTC-OPTION-SYMBOL"

    class PaginatedFillsClient:
        def __init__(self) -> None:
            self.cursors = []

        def fills(self, query=None, page_size=100, after=None):
            self.cursors.append(after)
            if after is None:
                return {
                    "success": True,
                    "result": [{"id": "fill-1", "product_id": 1001}],
                    "meta": {"after": "next-page"},
                }
            return {"success": True, "result": [{"id": "fill-2", "product_id": 1001}], "meta": {}}

    fills_client = PaginatedFillsClient()
    exchange._client = fills_client
    fills = await exchange.get_recent_fills(product_id=1001, limit=2)
    assert [fill["id"] for fill in fills] == ["fill-1", "fill-2"]
    assert fills_client.cursors == [None, "next-page"]

    class FailedFillsClient:
        def fills(self, query=None, page_size=100, after=None):
            return {"success": False, "error": "fills unavailable", "result": []}

    exchange._client = FailedFillsClient()
    with pytest.raises(ExchangeClientError, match="fills unavailable"):
        await exchange.get_recent_fills(product_id=1001)


@pytest.mark.asyncio
async def test_exchange_client_datetime_conversion_and_error_handling(fake_delta_client_module) -> None:
    exchange = DeltaExchangeClient("k", "s", "https://example.com")

    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert exchange._to_dt(now) == now
    assert exchange._to_dt(1704067200) == datetime.fromtimestamp(1704067200, tz=timezone.utc)
    assert exchange._to_dt("2024-09-30T00:00:00Z") == datetime(2024, 9, 30, tzinfo=timezone.utc)

    with pytest.raises(ExchangeClientError):
        exchange._to_dt("not-a-date")

    with pytest.raises(ExchangeClientError):
        exchange._to_dt(object())


@pytest.mark.asyncio
async def test_exchange_client_error_paths_and_fallbacks(fake_delta_client_module) -> None:
    exchange = DeltaExchangeClient("k", "s", "https://example.com")

    class ErrorClient:
        def request(self, **kwargs):
            raise RuntimeError("request failed")

        def get_ticker(self, **kwargs):
            raise RuntimeError("ticker failed")

        def place_order(self, **kwargs):
            return {"result": {"id": "ord-123"}}

        def order_history(self, **kwargs):
            return {"result": []}

    exchange._client = ErrorClient()

    with pytest.raises(ExchangeClientError):
        await exchange.get_open_positions_raw()

    with pytest.raises(ExchangeClientError):
        await exchange.get_index_price("BTCUSD")

    with pytest.raises(ExchangeClientError):
        await exchange.get_best_quote(1001)

@pytest.mark.asyncio
async def test_exchange_client_covers_more_fallback_and_error_branches(fake_delta_client_module) -> None:
    exchange = DeltaExchangeClient("k", "s", "https://example.com")

    class MissingMethodClient:
        pass

    exchange._client = MissingMethodClient()
    with pytest.raises(ExchangeClientError):
        await exchange._call("request")

    class NoKwargMethodClient:
        def request(self, **kwargs):
            return FakeResponse({"success": True, "result": [{"id": 5001}]})

    exchange._client = NoKwargMethodClient()
    response = await exchange._call("request", method="GET", path="/v2/products", auth=True)
    assert response.json()["result"][0]["id"] == 5001

    class FallbackRequestClient:
        def request(self):
            return FakeResponse({"success": True, "result": [{"id": 4001}]})

    exchange._client = FallbackRequestClient()
    resp = await exchange._call("request", method="GET", path="/v2/products", auth=True)
    assert resp.json()["result"][0]["id"] == 4001

    class ErrorPositionsClient:
        def request(self, **kwargs):
            return FakeResponse({"success": False, "error": "boom"})

    exchange._client = ErrorPositionsClient()
    with pytest.raises(ExchangeClientError):
        await exchange.get_open_positions_raw()

    class SizeEdgeClient:
        def request(self, **kwargs):
            return FakeResponse({"success": True, "result": [{"product_id": 5001, "size": 1.0, "product": None}]})

    exchange._client = SizeEdgeClient()
    assert await exchange.get_position_size(5001) == pytest.approx(1.0)

    class BadOrderClient:
        def place_order(self, **kwargs):
            return "not-a-dict"

    exchange._client = BadOrderClient()
    with pytest.raises(ExchangeClientError):
        await exchange.place_market_order(1001, "buy", 1.0, reduce_only=False)

@pytest.mark.asyncio
async def test_exchange_client_parse_option_positions_skips_invalid_entries(fake_delta_client_module) -> None:
    exchange = DeltaExchangeClient("k", "s", "https://example.com")

    class PositionClient:
        def request(self, **kwargs):
            return FakeResponse(
                {
                    "success": True,
                    "result": [
                        {
                            "product_id": 1001,
                            "size": 0.0,
                            "entry_price": 100.0,
                            "product": {
                                "symbol": "ZERO",
                                "contract_type": "option_call",
                                "strike_price": 100000.0,
                                "settlement_time": "2024-09-30T00:00:00Z",
                                "contract_value": 0.001,
                            },
                        },
                        {
                            "product_id": 1002,
                            "size": 1.0,
                            "entry_price": 100.0,
                            "product": None,
                        },
                        {
                            "product_id": 1003,
                            "size": 1.0,
                            "entry_price": 100.0,
                            "product": {
                                "symbol": "NON-OPTION",
                                "contract_type": "future",
                                "strike_price": 100000.0,
                                "settlement_time": "2024-09-30T00:00:00Z",
                                "contract_value": 0.001,
                            },
                        },
                        {
                            "product_id": 1004,
                            "size": 1.0,
                            "entry_price": 100.0,
                            "product": {
                                "symbol": "UNKNOWN",
                                "contract_type": "option_other",
                                "strike_price": 100000.0,
                                "settlement_time": "2024-09-30T00:00:00Z",
                                "contract_value": 0.001,
                            },
                        },
                        {
                            "product_id": 1005,
                            "size": 1.0,
                            "entry_price": 100.0,
                            "product": {
                                "symbol": "BAD-STRIKE",
                                "contract_type": "option_put",
                                "strike_price": 0.0,
                                "settlement_time": "2024-09-30T00:00:00Z",
                                "contract_value": 0.001,
                            },
                        },
                        {
                            "product_id": 1006,
                            "size": 1.0,
                            "entry_price": 100.0,
                            "product": {
                                "symbol": "BTC-VALID-CALL",
                                "contract_type": "option_call",
                                "strike_price": 100000.0,
                                "settlement_time": "2024-09-30T00:00:00Z",
                                "contract_value": 0.001,
                            },
                        },
                        {
                            "product_id": 1007,
                            "size": -1.0,
                            "entry_price": 100.0,
                            "product": {
                                "symbol": "BTC-VALID-PUT",
                                "contract_type": "option_put",
                                "strike_price": 100000.0,
                                "settlement_time": "2024-09-30T00:00:00Z",
                                "contract_value": 0.001,
                            },
                        },
                        {
                            "product_id": 1008,
                            "size": 1.0,
                            "entry_price": 100.0,
                            "product": {
                                "symbol": "ETH-VALID-CALL",
                                "contract_type": "option_call",
                                "underlying_asset": {"symbol": "ETH"},
                                "strike_price": 100000.0,
                                "settlement_time": "2024-09-30T00:00:00Z",
                                "contract_value": 0.001,
                            },
                        },
                    ],
                }
            )

    exchange._client = PositionClient()
    legs = await exchange.parse_option_positions()
    assert [leg.symbol for leg in legs] == ["BTC-VALID-CALL", "BTC-VALID-PUT"]
    assert [leg.option_type for leg in legs] == ["call", "put"]


@pytest.mark.asyncio
async def test_order_executor_confirms_single_submission_and_reduce_only_no_position(fake_delta_client_module) -> None:
    class FakeExchange:
        def __init__(self) -> None:
            self.position_sizes = {1001: 0.0, 1002: 0.0}

        async def get_position_size(self, product_id: int) -> float:
            return self.position_sizes.get(product_id, 0.0)

        async def place_market_order(self, *, product_id: int, side: str, size: float, reduce_only: bool) -> dict:
            self.position_sizes[product_id] = size
            return {"result": {"id": "ord-10"}}

    exec_engine = OrderExecutor(FakeExchange())
    confirmed = await exec_engine.execute_market_single_submission_with_fill_confirmation(
        product_id=1001,
        side="buy",
        size=2.0,
        reduce_only=False,
    )
    assert confirmed == pytest.approx(2.0)

    class ReduceOnlyExchange:
        async def get_position_size(self, product_id: int) -> float:
            return 0.0

        async def place_market_order(self, *, product_id: int, side: str, size: float, reduce_only: bool) -> dict:
            raise RuntimeError("no_position_for_reduce_only")

    reduce_only_engine = OrderExecutor(ReduceOnlyExchange())
    confirmed = await reduce_only_engine.execute_market_single_submission_with_fill_confirmation(
        product_id=1002,
        side="sell",
        size=1.0,
        reduce_only=True,
    )
    assert confirmed == pytest.approx(1.0)


def test_order_executor_rejects_non_positive_size(fake_delta_client_module) -> None:
    exchange = DeltaExchangeClient("k", "s", "https://example.com")
    executor = OrderExecutor(exchange)

    with pytest.raises(ValueError):
        asyncio.run(
            executor.execute_market_single_submission_with_fill_confirmation(
                product_id=1001,
                side="buy",
                size=0.0,
                reduce_only=False,
            )
        )


def test_main_module_entrypoint_calls_uvicorn_run(monkeypatch) -> None:
    captured: dict = {}

    import uvicorn

    def fake_run(app: str, host: str, port: int, log_level: str) -> None:
        captured.update({"app": app, "host": host, "port": port, "log_level": log_level})

    monkeypatch.setattr(uvicorn, "run", fake_run)
    runpy.run_module("src.main", run_name="__main__")

    assert captured == {
        "app": "src.api:app",
        "host": "0.0.0.0",
        "port": 8000,
        "log_level": "info",
    }


@pytest.mark.asyncio
async def test_position_manager_marks_mixed_expiry_btc_options() -> None:
    class MixedExpiryExchange:
        async def parse_option_positions(self):
            return [
                OptionLeg(1, "BTC-CALL-LONG", "call", 100.0, datetime(2026, 1, 1, tzinfo=timezone.utc), 1.0, 100.0, 1.0),
                OptionLeg(2, "BTC-PUT-SHORT", "put", 95.0, datetime(2026, 1, 2, tzinfo=timezone.utc), -2.0, 100.0, 1.0),
            ]

        async def get_best_quote(self, product_id: int, symbol: str | None = None):
            return Quote(best_bid=101.0, best_ask=102.0)

    manager = PositionManager(MixedExpiryExchange())
    legs = await manager.exchange.parse_option_positions()

    assert await manager.compute_unrealized_leg_pnl(legs[0]) == pytest.approx(1.0)
    assert await manager.compute_unrealized_leg_pnl(legs[1]) == pytest.approx(-4.0)


def test_api_status_includes_ratio_state_details(api_module) -> None:
    status = api_module.manager.status()
    assert status["strategy_state"]["action"] == "waiting for BTC option positions"


def test_json_log_handler_formats_payloads_and_limits_history() -> None:
    from src.api import JsonLogHandler

    handler = JsonLogHandler(max_records=2)

    json_record = logging.LogRecord(
        name="coverage.handler.test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg='{"symbol": "BTC", "value": 42}',
        args=(),
        exc_info=None,
    )
    plain_record = logging.LogRecord(
        name="coverage.handler.test",
        level=logging.INFO,
        pathname=__file__,
        lineno=2,
        msg="plain log message",
        args=(),
        exc_info=None,
    )

    handler.emit(json_record)
    handler.emit(plain_record)

    records = handler.latest(2)
    assert len(records) == 2
    assert records[0]["data"]["symbol"] == "BTC"
    assert records[1]["message"] == "plain log message"


def test_json_log_handler_falls_back_when_formatter_raises() -> None:
    from src.api import JsonLogHandler

    handler = JsonLogHandler(max_records=1)
    handler.format = MagicMock(side_effect=RuntimeError("formatter blew up"))

    record = logging.LogRecord(
        name="coverage.handler.test",
        level=logging.INFO,
        pathname=__file__,
        lineno=3,
        msg="fallback message",
        args=(),
        exc_info=None,
    )

    handler.emit(record)
    records = handler.latest(1)
    assert records[0]["message"] == "fallback message"


@pytest.mark.asyncio
async def test_collect_summary_handles_timeout_and_returns_partial_snapshot(api_module) -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)

    class TimeoutExchange:
        async def get_index_price(self, symbol: str) -> float:
            return 100.0

        async def parse_option_positions(self):
            return [
                OptionLeg(1001, "BTC-240930-C-100000", "call", 100000.0, now, 1.0, 100.0, 0.001)
            ]

        async def get_best_quote(self, product_id: int, symbol: str | None = None):
            raise asyncio.TimeoutError()

    api_module.manager.strategy.exchange = TimeoutExchange()
    api_module.manager.strategy.state.status_message = "running"
    api_module.manager.strategy.state.action = "monitoring ratio-spreads"
    api_module.manager.strategy.state.status = "monitoring pnl thresholds"

    summary = await api_module.manager.collect_summary()
    assert summary["index_price"] == pytest.approx(100.0)
    assert summary["position_count"] == 0
    assert summary["unrealized_pnl"] == pytest.approx(0.0)


@pytest.mark.asyncio
async def test_collect_summary_skips_quotes_with_invalid_data_and_runtime_errors(api_module) -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)

    class MixedQuoteExchange:
        async def get_index_price(self, symbol: str) -> float:
            return 100.0

        async def parse_option_positions(self):
            return [
                OptionLeg(1001, "BTC-240930-C-100000", "call", 100000.0, now, 1.0, 100.0, 0.001),
                OptionLeg(1002, "BTC-240930-P-100000", "put", 100000.0, now, -1.0, 100.0, 0.001),
                OptionLeg(1003, "BTC-240930-C-200000", "call", 100000.0, now, 1.0, 100.0, 0.001),
            ]

        async def get_best_quote(self, product_id: int, symbol: str | None = None):
            if product_id == 1001:
                return Quote(best_bid=None, best_ask=101.0)
            if product_id == 1002:
                raise RuntimeError("quote lookup failed")
            return Quote(best_bid=200.0, best_ask=201.0)

    api_module.manager.strategy.exchange = MixedQuoteExchange()
    summary = await api_module.manager.collect_summary()
    assert summary["position_count"] == 1
    assert summary["positions"][0]["product_id"] == 1003
    assert summary["unrealized_pnl"] == pytest.approx(0.1)


def test_summary_endpoint_returns_500_when_summary_fails(api_module) -> None:
    async def boom() -> dict:
        raise RuntimeError("summary failed")

    api_module.manager.collect_summary = boom
    client = TestClient(api_module.app)

    resp = client.get("/summary")
    assert resp.status_code == 500
    assert "summary failed" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_bot_manager_start_stop_state_transitions() -> None:
    from src.api import BotManager

    class FakeStrategy:
        def __init__(self) -> None:
            async def noop() -> None:
                return None

            self.run = noop
            self.exchange = None
            self.state = SimpleNamespace(
                last_index_price=None,
                status_message="",
                action="",
                status="",
                trigger_price=None,
                trigger_pnl=None,
            )

    manager = BotManager(FakeStrategy())
    first = manager.start()
    assert first["status"] == "started"

    second = manager.start()
    assert second["status"] == "already_running"

    stopped = manager.stop()
    assert stopped == {"status": "stopped", "running": False}

    await asyncio.sleep(0)
    idle = manager.stop()
    assert idle == {"status": "not_running", "running": False}
