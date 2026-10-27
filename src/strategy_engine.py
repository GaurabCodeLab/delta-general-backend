from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Optional

from src.config import Settings
from src.exchange_client import DeltaExchangeClient
from src.models import OptionLeg
from src.order_executor import OrderExecutor
from src.position_manager import PositionManager

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class StrategyState:
    last_index_price: Optional[float] = None
    status_message: str = "starting"
    action: str = "waiting for BTC option positions"
    status: str = "initializing"
    trigger_price: Optional[str] = None
    trigger_pnl: Optional[float] = None
    realized_pnl_total: float = 0.0
    unrealized_pnl_total: float = 0.0
    combined_pnl_total: float = 0.0
    tracked_product_ids: set[int] = field(default_factory=set)
    previous_open_legs: Optional[list[OptionLeg]] = None


class StrategyEngine:
    def __init__(
        self,
        exchange: DeltaExchangeClient,
        positions: PositionManager,
        executor: OrderExecutor,
        settings: Settings,
    ) -> None:
        self.exchange = exchange
        self.positions = positions
        self.executor = executor
        self.settings = settings
        self.state = StrategyState()

    def _set_strategy_state(
        self,
        action: str,
        status: str,
        status_message: str | None = None,
        trigger_price: str | None = None,
        trigger_pnl: float | None = None,
    ) -> None:
        self.state.action = action
        self.state.status = status
        if status_message is not None:
            self.state.status_message = status_message
        self.state.trigger_price = trigger_price
        self.state.trigger_pnl = trigger_pnl

    @staticmethod
    def _price_crossed(previous: float | None, current: float, target: float | None) -> bool:
        if target is None:
            return False
        if previous is None:
            return current == target
        return min(previous, current) <= target <= max(previous, current)

    async def _close_managed_positions(self, legs: list[OptionLeg]) -> None:
        legs_by_product = {
            leg.product_id: leg
            for leg in legs
            if leg.product_id in self.state.tracked_product_ids and abs(leg.size) > 0
        }
        for leg in legs_by_product.values():
            remaining = abs(leg.size)
            for attempt in range(3):
                if remaining <= 1e-8:
                    break
                try:
                    confirmed = await self.executor.execute_market_single_submission_with_fill_confirmation(
                        product_id=leg.product_id,
                        side="sell" if leg.size > 0 else "buy",
                        size=remaining,
                        reduce_only=True,
                    )
                    confirmed = min(max(float(confirmed), 0.0), remaining)
                    remaining -= confirmed
                    LOGGER.info(
                        "Close attempt product_id=%s attempt=%s filled=%s remaining=%s",
                        leg.product_id,
                        attempt + 1,
                        confirmed,
                        remaining,
                    )
                    if confirmed <= 1e-8:
                        break
                except Exception:
                    LOGGER.exception("Failed to close managed product_id=%s", leg.product_id)
                    break
            if remaining > 1e-8:
                LOGGER.error("Product %s remains open with estimated size %s", leg.product_id, remaining)

    async def _refresh_after_close(self, previous: list[OptionLeg]) -> None:
        try:
            current = await self.exchange.parse_option_positions()
            self.state.realized_pnl_total += await self.positions.reconcile_realized_pnl(previous, current)
            self.state.previous_open_legs = current
            unrealized = 0.0
            for leg in current:
                unrealized += await self.positions.compute_unrealized_leg_pnl(leg)
            self.state.unrealized_pnl_total = unrealized
            self.state.combined_pnl_total = self.state.realized_pnl_total + unrealized
        except Exception:
            LOGGER.exception("Could not reconcile positions after close submissions")

    async def run(self) -> None:
        start_session = getattr(self.positions, "start_session", None)
        if start_session is not None:
            start_session()
        self.state.realized_pnl_total = 0.0
        self.state.unrealized_pnl_total = 0.0
        self.state.combined_pnl_total = 0.0
        self.state.tracked_product_ids.clear()
        self.state.previous_open_legs = None
        previous_index_price: float | None = None

        while True:
            snapshot: list[OptionLeg] | None = None
            pnl_complete = True
            try:
                snapshot = await self.exchange.parse_option_positions()
                previous_positions = self.state.previous_open_legs
                if previous_positions is not None:
                    self.state.realized_pnl_total += await self.positions.reconcile_realized_pnl(
                        previous_positions,
                        snapshot,
                    )
                self.state.previous_open_legs = snapshot
                self.state.tracked_product_ids.update(leg.product_id for leg in snapshot)
            except Exception:
                pnl_complete = False
                LOGGER.exception("Position snapshot or realized PnL reconciliation failed")

            index_price: float | None = None
            index_crossed = False
            try:
                index_price = await self.exchange.get_index_price("BTCUSD")
                index_crossed = self._price_crossed(
                    previous_index_price,
                    index_price,
                    self.settings.index_price_trigger,
                )
                previous_index_price = index_price
                self.state.last_index_price = index_price
            except Exception:
                LOGGER.exception("Failed to fetch BTC index price")

            unrealized = 0.0
            if pnl_complete and snapshot is not None:
                try:
                    for leg in snapshot:
                        unrealized += await self.positions.compute_unrealized_leg_pnl(leg)
                except Exception:
                    pnl_complete = False
                    LOGGER.exception("Could not calculate PnL for the complete BTC options portfolio")

            combined = self.state.realized_pnl_total + unrealized
            if pnl_complete and snapshot is not None:
                self.state.unrealized_pnl_total = unrealized
                self.state.combined_pnl_total = combined

            stop_loss = -abs(self.settings.loss_exit_threshold_usd)
            exit_reason: str | None = None
            if pnl_complete and combined >= self.settings.profit_exit_threshold_usd:
                exit_reason = "profit target reached"
            elif pnl_complete and combined <= stop_loss:
                exit_reason = "stop loss reached"
            elif index_crossed:
                exit_reason = "BTC index price trigger crossed"

            LOGGER.info(
                "BTC options portfolio | positions=%s index=%s realized_pnl=%.8f unrealized_pnl=%.8f combined_pnl=%.8f",
                len(snapshot) if snapshot is not None else "unavailable",
                index_price,
                self.state.realized_pnl_total,
                unrealized if pnl_complete else self.state.unrealized_pnl_total,
                combined if pnl_complete else self.state.combined_pnl_total,
            )

            if exit_reason is not None:
                self._set_strategy_state(
                    action="closing all managed positions",
                    status=exit_reason,
                    status_message=exit_reason,
                    trigger_price=(
                        str(self.settings.index_price_trigger)
                        if exit_reason == "BTC index price trigger crossed"
                        else None
                    ),
                    trigger_pnl=combined if pnl_complete else self.state.combined_pnl_total,
                )
                positions_to_close = snapshot if snapshot is not None else (self.state.previous_open_legs or [])
                try:
                    latest_positions = await self.exchange.parse_option_positions()
                    self.state.tracked_product_ids.update(leg.product_id for leg in latest_positions)
                    positions_to_close = latest_positions
                except Exception:
                    LOGGER.exception("Could not refresh option positions before closing")
                await self._close_managed_positions(positions_to_close)
                await self._refresh_after_close(positions_to_close)
                return

            self._set_strategy_state(
                action="monitoring BTC option positions",
                status="monitoring portfolio exits",
                status_message=(
                    "monitoring live BTC option positions"
                    if snapshot
                    else "waiting for BTC option positions"
                ),
            )
            await asyncio.sleep(self.settings.poll_interval_seconds)
