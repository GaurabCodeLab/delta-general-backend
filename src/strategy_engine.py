from __future__ import annotations

import asyncio
import contextlib
import json
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
class RatioState:
    """Track state for a single ratio spread."""
    ratio_id: str  # "call" or "put"
    status_message: str = "monitoring"
    action: str = "monitoring ratio-spread"
    trigger_pnl: Optional[float] = None


@dataclass(slots=True)
class StrategyState:
    last_index_price: Optional[float] = None
    status_message: str = "starting"
    action: str = "waiting for ratio spreads"
    status: str = "initializing"
    trigger_price: Optional[str] = None
    trigger_pnl: Optional[float] = None
    realized_pnl_total: float = 0.0
    unrealized_pnl_total: float = 0.0
    combined_pnl_total: float = 0.0
    tracked_product_ids: set[int] = field(default_factory=set)
    previous_open_legs: Optional[list[OptionLeg]] = None
    call_ratio_state: Optional[RatioState] = None
    put_ratio_state: Optional[RatioState] = None


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

    async def _close_entire_ratio(self, ratio: RatioSpread) -> None:
        long_qty = abs(ratio.long_leg.size)
        short_qty = abs(ratio.short_leg.size)

        # Long close = sell. Short close = buy.
        filled_long = await self.executor.execute_market_single_submission_with_fill_confirmation(
            product_id=ratio.long_leg.product_id,
            side="sell",
            size=long_qty,
            reduce_only=True,
        )
        filled_short = await self.executor.execute_market_single_submission_with_fill_confirmation(
            product_id=ratio.short_leg.product_id,
            side="buy",
            size=short_qty,
            reduce_only=True,
        )
        LOGGER.info("Ratio close requested long_filled=%s short_filled=%s", filled_long, filled_short)

    def _get_ratio_id(self, ratio: RatioSpread) -> str:
        """Return 'call' or 'put' as the ratio identifier."""
        return ratio.long_leg.option_type

    async def _run_ratio_strategy_legacy(self) -> None:
        active_ratios: dict[str, RatioSpread] = {}  # {"call": RatioSpread, "put": RatioSpread}
        wait_cycles = 0
        ratio_scan_task = None
        has_active_session = False

        try:
            while True:
                current_open_legs: list[OptionLeg] | None = None
                position_reconciliation_complete = True
                try:
                    current_open_legs = await self.exchange.parse_option_positions()
                    if self.state.previous_open_legs is not None:
                        previous_tracked_legs = [
                            leg for leg in self.state.previous_open_legs
                            if leg.product_id in self.state.tracked_product_ids
                        ]
                        current_tracked_legs = [
                            leg for leg in current_open_legs
                            if leg.product_id in self.state.tracked_product_ids
                        ]
                        self.state.realized_pnl_total += await self.positions.reconcile_realized_pnl(
                            previous_tracked_legs,
                            current_tracked_legs,
                        )
                    self.state.previous_open_legs = current_open_legs
                except Exception as exc:
                    LOGGER.error("Position snapshot or realized PnL reconciliation failed: %s", exc)
                    position_reconciliation_complete = False

                # If no active ratios, scan for them
                if not active_ratios:
                    self._set_strategy_state(
                        action="waiting for ratio spreads",
                        status="no ratio spreads found",
                        status_message="awaiting configured ratio spreads (call and/or put)",
                        trigger_price=None,
                        trigger_pnl=None,
                    )

                    if ratio_scan_task is None:
                        ratio_scan_task = asyncio.create_task(
                            self.positions.detect_ratio_spreads(self.settings.ratio_spreads)
                        )

                    if ratio_scan_task.done():
                        try:
                            found_ratios = ratio_scan_task.result()
                            ratio_scan_task = None

                            # Build active ratios dict
                            for ratio in found_ratios:
                                ratio_id = self._get_ratio_id(ratio)
                                active_ratios[ratio_id] = ratio
                                self.state.tracked_product_ids.update((ratio.long_leg.product_id, ratio.short_leg.product_id))
                                LOGGER.info(
                                    "Detected %s ratio spread ratio=%.2f long=%s @%.2f short=%s @%.2f",
                                    ratio_id,
                                    ratio.ratio,
                                    abs(ratio.long_leg.size),
                                    ratio.long_leg.strike,
                                    abs(ratio.short_leg.size),
                                    ratio.short_leg.strike,
                                )
                            has_active_session = has_active_session or bool(found_ratios)

                            # Update state
                            call_status = "detected" if "call" in active_ratios else "not found"
                            put_status = "detected" if "put" in active_ratios else "not found"
                            self._set_strategy_state(
                                action="monitoring ratio-spreads",
                                status="ratio spreads found",
                                status_message=f"call={call_status}, put={put_status}",
                                trigger_price=None,
                                trigger_pnl=None,
                            )
                            if "call" in active_ratios:
                                self.state.call_ratio_state = RatioState("call", "monitoring", "monitoring call ratio")
                            if "put" in active_ratios:
                                self.state.put_ratio_state = RatioState("put", "monitoring", "monitoring put ratio")
                        except Exception as exc:
                            ratio_scan_task = None
                            wait_cycles += 1
                            try:
                                index_price = await self.exchange.get_index_price("BTCUSDT")
                                self.state.last_index_price = index_price
                                LOGGER.info(
                                    "no ratio spread found | BTC index=%.2f | attempt=%s | detail=%s",
                                    index_price,
                                    wait_cycles,
                                    str(exc),
                                )
                            except Exception:
                                LOGGER.info(
                                    "no ratio spread found | BTC index=unavailable | attempt=%s | detail=%s",
                                    wait_cycles,
                                    str(exc),
                                )
                            if not has_active_session:
                                await asyncio.sleep(self.settings.poll_interval_seconds)
                                continue
                    else:
                        wait_cycles += 1
                        try:
                            index_price = await self.exchange.get_index_price("BTCUSDT")
                            self.state.last_index_price = index_price
                            LOGGER.info(
                                "no ratio spread found | BTC index=%.2f | attempt=%s | status=scan in progress",
                                index_price,
                                wait_cycles,
                            )
                        except Exception:
                            LOGGER.info(
                                "no ratio spread found | BTC index=unavailable | attempt=%s | status=scan in progress",
                                wait_cycles,
                            )
                        if not has_active_session:
                            await asyncio.sleep(self.settings.poll_interval_seconds)
                            continue

                # Monitor all active ratios
                self._set_strategy_state(
                    action="monitoring ratio-spreads",
                    status="monitoring pnl thresholds",
                    status_message="checking ratio spreads pnl against exit thresholds",
                    trigger_price=None,
                    trigger_pnl=None,
                )

                # Verify positions still exist
                ratios_verified = False
                try:
                    current_ratios = await self.positions.detect_ratio_spreads(self.settings.ratio_spreads)
                    ratios_verified = True
                    current_ids = {self._get_ratio_id(r) for r in current_ratios}
                    active_ratios.update({self._get_ratio_id(r): r for r in current_ratios})

                    # Remove any that are no longer present
                    removed_ids = set(active_ratios.keys()) - current_ids
                    for ratio_id in removed_ids:
                        LOGGER.info("%s ratio spread no longer present, removing from active", ratio_id)
                        del active_ratios[ratio_id]
                        if ratio_id == "call":
                            self.state.call_ratio_state = None
                        elif ratio_id == "put":
                            self.state.put_ratio_state = None
                except Exception:
                    LOGGER.info("failed to verify active ratios, continuing with existing state")
                    pass

                if not ratios_verified and current_open_legs is not None:
                    legs_by_id = {leg.product_id: leg for leg in current_open_legs}
                    for ratio_id, ratio in list(active_ratios.items()):
                        long_leg = legs_by_id.get(ratio.long_leg.product_id)
                        short_leg = legs_by_id.get(ratio.short_leg.product_id)
                        if long_leg is None or short_leg is None or long_leg.size <= 0 or short_leg.size >= 0:
                            LOGGER.info("%s ratio changed in the open book; removing stale spread snapshot", ratio_id)
                            del active_ratios[ratio_id]
                            if ratio_id == "call":
                                self.state.call_ratio_state = None
                            elif ratio_id == "put":
                                self.state.put_ratio_state = None
                        else:
                            ratio.long_leg = long_leg
                            ratio.short_leg = short_leg

                # If all ratios were removed, reset and scan again
                if not active_ratios and not has_active_session:
                    await asyncio.sleep(self.settings.poll_interval_seconds)
                    continue

                # Get index price once per cycle
                try:
                    index_price = await self.exchange.get_index_price("BTCUSDT")
                    self.state.last_index_price = index_price
                except Exception as e:
                    LOGGER.warning("Failed to fetch index price: %s", str(e))
                    index_price = None

                # Monitor all active ratios and compute overall portfolio PnL
                total_portfolio_pnl = self.state.realized_pnl_total
                ratio_pnls: dict[str, float] = {}  # Track individual PnL for logging
                monitored_product_ids: set[int] = set()
                pnl_calculation_complete = current_open_legs is not None and position_reconciliation_complete

                for ratio_id, ratio in active_ratios.items():
                    try:
                        pnl = await self.positions.compute_unrealized_pnl(ratio)
                        ratio_pnls[ratio_id] = pnl
                        total_portfolio_pnl += pnl
                        monitored_product_ids.update((ratio.long_leg.product_id, ratio.short_leg.product_id))

                        long_q = await self.exchange.get_best_quote(ratio.long_leg.product_id)
                        short_q = await self.exchange.get_best_quote(ratio.short_leg.product_id)

                        LOGGER.info(
                            "Ratio monitor | type=%s | index=%.2f strike=%.2f ratio=%.2f pnl=%.4f",
                            ratio_id,
                            index_price if index_price else "N/A",
                            ratio.short_leg.strike,
                            ratio.ratio,
                            pnl,
                        )

                        leg_snapshot = {
                            "event": "ratio_monitor_snapshot",
                            "type": ratio_id,
                            "index_price": round(index_price, 4) if index_price else None,
                            "unrealized_pnl": round(pnl, 8),
                            "ratio": round(ratio.ratio, 4),
                            "profit_threshold": self.settings.profit_exit_threshold_usd,
                            "loss_threshold": self.settings.loss_exit_threshold_usd,
                            "long_leg": {
                                "symbol": ratio.long_leg.symbol,
                                "type": ratio.long_leg.option_type,
                                "strike": ratio.long_leg.strike,
                                "quantity": abs(ratio.long_leg.size),
                                "entry_price": ratio.long_leg.entry_price,
                                "best_bid": long_q.best_bid,
                                "best_ask": long_q.best_ask,
                            },
                            "short_leg": {
                                "symbol": ratio.short_leg.symbol,
                                "type": ratio.short_leg.option_type,
                                "strike": ratio.short_leg.strike,
                                "quantity": abs(ratio.short_leg.size),
                                "entry_price": ratio.short_leg.entry_price,
                                "best_bid": short_q.best_bid,
                                "best_ask": short_q.best_ask,
                            },
                        }
                        LOGGER.info("Snapshot: %s", json.dumps(leg_snapshot, separators=(",", ":")))

                    except Exception as e:
                        LOGGER.error("Error monitoring %s ratio: %s", ratio_id, str(e))
                        pnl_calculation_complete = False

                if current_open_legs is not None:
                    for leg in current_open_legs:
                        if leg.product_id not in self.state.tracked_product_ids or leg.product_id in monitored_product_ids:
                            continue
                        try:
                            total_portfolio_pnl += await self.positions.compute_unrealized_leg_pnl(leg)
                        except Exception as exc:
                            LOGGER.error("Error monitoring unmatched position %s: %s", leg.product_id, exc)
                            pnl_calculation_complete = False

                # Log overall portfolio PnL
                pnl_breakdown = ", ".join(f"{k}={v:.2f}" for k, v in ratio_pnls.items())
                LOGGER.info(
                    "Portfolio PnL check | realized_pnl=%.4f | total_pnl=%.4f | breakdown=[%s] | thresholds=[profit=%s, loss=%s]",
                    self.state.realized_pnl_total,
                    total_portfolio_pnl,
                    pnl_breakdown,
                    self.settings.profit_exit_threshold_usd,
                    self.settings.loss_exit_threshold_usd,
                )

                # Check exit thresholds on OVERALL portfolio PnL
                if pnl_calculation_complete and (
                    total_portfolio_pnl >= self.settings.profit_exit_threshold_usd
                    or total_portfolio_pnl <= self.settings.loss_exit_threshold_usd
                ):
                    self._set_strategy_state(
                        action="closing all positions",
                        status="overall pnl threshold reached",
                        status_message=(
                            f"closing all ratio spreads | overall pnl={total_portfolio_pnl:.2f}"
                        ),
                        trigger_price=None,
                        trigger_pnl=total_portfolio_pnl,
                    )
                    LOGGER.info(
                        "Overall PnL threshold hit | total_pnl=%.2f | profit=%s loss=%s; closing ALL positions simultaneously",
                        total_portfolio_pnl,
                        self.settings.profit_exit_threshold_usd,
                        self.settings.loss_exit_threshold_usd,
                    )

                    # Close only legs belonging to spreads this strategy detected.
                    legs_to_close = {
                        leg.product_id: leg
                        for leg in (current_open_legs or [])
                        if leg.product_id in self.state.tracked_product_ids
                    }
                    for ratio in active_ratios.values():
                        legs_to_close.setdefault(ratio.long_leg.product_id, ratio.long_leg)
                        legs_to_close.setdefault(ratio.short_leg.product_id, ratio.short_leg)

                    for leg in legs_to_close.values():
                        try:
                            await self.executor.execute_market_single_submission_with_fill_confirmation(
                                product_id=leg.product_id,
                                side="sell" if leg.size > 0 else "buy",
                                size=abs(leg.size),
                                reduce_only=True,
                            )
                            LOGGER.info("Closed tracked product %s", leg.product_id)
                        except Exception as e:
                            LOGGER.error("Error closing product %s: %s", leg.product_id, str(e))

                    try:
                        post_close_legs = await self.exchange.parse_option_positions()
                        before_close_tracked = [
                            leg for leg in (current_open_legs or [])
                            if leg.product_id in self.state.tracked_product_ids
                        ]
                        after_close_tracked = [
                            leg for leg in post_close_legs
                            if leg.product_id in self.state.tracked_product_ids
                        ]
                        self.state.realized_pnl_total += await self.positions.reconcile_realized_pnl(
                            before_close_tracked,
                            after_close_tracked,
                        )
                        self.state.previous_open_legs = post_close_legs
                    except Exception as exc:
                        LOGGER.error("Could not reconcile bot close fills: %s", exc)

                    # Clear all ratio states
                    self.state.call_ratio_state = None
                    self.state.put_ratio_state = None

                    LOGGER.info("All ratio spreads have been closed")
                    if pnl_calculation_complete:
                        self.state.unrealized_pnl_total = total_portfolio_pnl - self.state.realized_pnl_total
                        self.state.combined_pnl_total = total_portfolio_pnl
                    return

                if pnl_calculation_complete:
                    self.state.unrealized_pnl_total = total_portfolio_pnl - self.state.realized_pnl_total
                    self.state.combined_pnl_total = total_portfolio_pnl

                await asyncio.sleep(self.settings.poll_interval_seconds)
        finally:
            if ratio_scan_task is not None and not ratio_scan_task.done():
                ratio_scan_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await ratio_scan_task

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
