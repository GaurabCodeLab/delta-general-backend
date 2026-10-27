from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.config import Settings
from src.models import OptionLeg, Quote
from src.strategy_engine import StrategyEngine


def make_leg(
    product_id: int,
    option_type: str,
    size: float,
    expiry_day: int,
    entry_price: float = 100.0,
) -> OptionLeg:
    return OptionLeg(
        product_id=product_id,
        symbol=f"BTC-{option_type.upper()}-{product_id}",
        option_type=option_type,
        strike=100.0 + product_id,
        expiry=datetime(2026, 2, expiry_day, tzinfo=timezone.utc),
        size=size,
        entry_price=entry_price,
        contract_value=1.0,
    )


class SnapshotExchange:
    def __init__(
        self,
        snapshots: list[list[OptionLeg]],
        index_prices: list[float] | None = None,
    ) -> None:
        self.snapshots = snapshots
        self.index_prices = index_prices or [100.0]
        self.snapshot_calls = 0
        self.index_calls = 0

    async def parse_option_positions(self) -> list[OptionLeg]:
        index = min(self.snapshot_calls, len(self.snapshots) - 1)
        self.snapshot_calls += 1
        return list(self.snapshots[index])

    async def get_index_price(self, symbol: str) -> float:
        index = min(self.index_calls, len(self.index_prices) - 1)
        self.index_calls += 1
        return self.index_prices[index]

    async def get_best_quote(self, product_id: int, symbol: str | None = None) -> Quote:
        return Quote(best_bid=100.0, best_ask=100.0)


class SnapshotPositions:
    def __init__(self, pnl_by_product: dict[int, float], realized_deltas: list[float] | None = None) -> None:
        self.pnl_by_product = pnl_by_product
        self.realized_deltas = list(realized_deltas or [])
        self.reconciliations: list[tuple[list[OptionLeg], list[OptionLeg]]] = []
        self.marked_legs: list[OptionLeg] = []

    async def reconcile_realized_pnl(
        self,
        previous_open_legs: list[OptionLeg],
        current_open_legs: list[OptionLeg],
    ) -> float:
        self.reconciliations.append((list(previous_open_legs), list(current_open_legs)))
        return self.realized_deltas.pop(0) if self.realized_deltas else 0.0

    async def compute_unrealized_leg_pnl(self, leg: OptionLeg) -> float:
        self.marked_legs.append(leg)
        return self.pnl_by_product.get(leg.product_id, 0.0)


class RecordingExecutor:
    def __init__(self) -> None:
        self.orders: list[tuple[int, str, float, bool]] = []

    async def execute_market_single_submission_with_fill_confirmation(
        self,
        product_id: int,
        side: str,
        size: float,
        reduce_only: bool,
    ) -> float:
        self.orders.append((product_id, side, size, reduce_only))
        return size


def settings(
    *,
    profit: float = 10.0,
    loss: float = 20.0,
    index_trigger: float | None = None,
) -> Settings:
    return Settings(
        api_key="key",
        api_secret="secret",
        base_url="https://example.invalid",
        poll_interval_seconds=0.0,
        profit_exit_threshold_usd=profit,
        loss_exit_threshold_usd=loss,
        log_level="INFO",
        ssl_verify=True,
        index_price_trigger=index_trigger,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("pnl", "profit", "loss", "expected_status"),
    [
        (12.0, 10.0, 20.0, "profit target reached"),
        (-22.0, 100.0, 20.0, "stop loss reached"),
    ],
)
async def test_profit_and_stop_loss_exit_independently(
    pnl: float,
    profit: float,
    loss: float,
    expected_status: str,
) -> None:
    leg = make_leg(1, "call", 2.0, 10)
    exchange = SnapshotExchange([[leg]])
    positions = SnapshotPositions({1: pnl})
    executor = RecordingExecutor()
    engine = StrategyEngine(exchange, positions, executor, settings(profit=profit, loss=loss))

    await engine.run()

    assert engine.state.status == expected_status
    assert engine.state.trigger_pnl == pytest.approx(pnl)
    assert executor.orders == [(1, "sell", 2.0, True)]


@pytest.mark.asyncio
async def test_index_trigger_detects_crossing_between_poll_samples() -> None:
    leg = make_leg(10, "put", -1.0, 12)
    exchange = SnapshotExchange([[leg]], index_prices=[99.0, 101.0])
    positions = SnapshotPositions({10: 0.0})
    executor = RecordingExecutor()
    engine = StrategyEngine(
        exchange,
        positions,
        executor,
        settings(profit=1000.0, loss=1000.0, index_trigger=100.0),
    )

    await engine.run()

    assert engine.state.status == "BTC index price trigger crossed"
    assert engine.state.trigger_price == "100.0"
    assert exchange.index_calls == 2
    assert executor.orders == [(10, "buy", 1.0, True)]


@pytest.mark.asyncio
async def test_dynamic_multi_expiry_portfolio_tracks_changes_and_session_pnl() -> None:
    initial = [
        make_leg(1, "call", 2.0, 10, entry_price=100.0),
        make_leg(2, "put", -1.0, 11, entry_price=100.0),
        make_leg(6, "put", 3.0, 12, entry_price=100.0),
    ]
    changed = [
        make_leg(1, "call", -1.0, 10, entry_price=101.0),
        make_leg(3, "put", 1.0, 18, entry_price=99.0),
        make_leg(4, "call", -1.0, 25, entry_price=102.0),
        make_leg(5, "put", -2.0, 28, entry_price=98.0),
        make_leg(6, "put", 1.0, 12, entry_price=103.0),
    ]
    exchange = SnapshotExchange([initial, changed, changed])
    positions = SnapshotPositions(
        {1: 3.0, 2: 0.0, 3: 3.0, 4: 3.0, 5: 3.0, 6: 3.0},
        realized_deltas=[4.0, 0.0],
    )
    executor = RecordingExecutor()
    engine = StrategyEngine(exchange, positions, executor, settings(profit=19.0, loss=100.0))

    await engine.run()

    assert engine.state.trigger_pnl == pytest.approx(19.0)
    assert engine.state.realized_pnl_total == pytest.approx(4.0)
    assert engine.state.unrealized_pnl_total == pytest.approx(15.0)
    assert engine.state.combined_pnl_total == pytest.approx(19.0)
    assert positions.reconciliations[0] == (initial, changed)
    second_cycle_marks = positions.marked_legs[3:8]
    assert [leg.product_id for leg in second_cycle_marks] == [1, 3, 4, 5, 6]
    assert second_cycle_marks[0].size == -1.0
    assert second_cycle_marks[0].entry_price == 101.0
    assert second_cycle_marks[-1].size == 1.0
    assert second_cycle_marks[-1].entry_price == 103.0
    assert 2 not in [leg.product_id for leg in second_cycle_marks]
    assert [order[0] for order in executor.orders] == [1, 3, 4, 5, 6]
    assert executor.orders == [
        (1, "buy", 1.0, True),
        (3, "sell", 1.0, True),
        (4, "buy", 1.0, True),
        (5, "buy", 2.0, True),
        (6, "sell", 1.0, True),
    ]


@pytest.mark.asyncio
async def test_partial_close_retries_only_unfilled_reduce_only_remainder() -> None:
    class PartialExecutor(RecordingExecutor):
        async def execute_market_single_submission_with_fill_confirmation(
            self,
            product_id: int,
            side: str,
            size: float,
            reduce_only: bool,
        ) -> float:
            self.orders.append((product_id, side, size, reduce_only))
            return size / 2 if len(self.orders) == 1 else size

    leg = make_leg(20, "put", -2.0, 14)
    exchange = SnapshotExchange([[leg]])
    positions = SnapshotPositions({20: 20.0})
    executor = PartialExecutor()
    engine = StrategyEngine(exchange, positions, executor, settings(profit=10.0, loss=100.0))

    await engine.run()

    assert executor.orders == [
        (20, "buy", 2.0, True),
        (20, "buy", 1.0, True),
    ]
