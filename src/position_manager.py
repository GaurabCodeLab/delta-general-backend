from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any

from src.exchange_client import DeltaExchangeClient
from src.models import OptionLeg


class PositionManager:
    def __init__(self, exchange: DeltaExchangeClient) -> None:
        self.exchange = exchange
        self._realized_tracking_started_at = datetime.now(timezone.utc)
        self._seen_fill_ids: set[str] = set()
        self._consumed_fill_sizes: dict[str, float] = {}
        self._closed_positions: list[dict[str, Any]] = []

    @property
    def closed_positions(self) -> list[dict[str, Any]]:
        return [dict(position) for position in self._closed_positions]

    def start_session(self) -> None:
        self._realized_tracking_started_at = datetime.now(timezone.utc)
        self._seen_fill_ids.clear()
        self._consumed_fill_sizes.clear()
        self._closed_positions.clear()

    async def compute_unrealized_leg_pnl(self, leg: OptionLeg) -> float:
        quote = await self.exchange.get_best_quote(leg.product_id, leg.symbol)
        quantity = abs(leg.size)
        if leg.size > 0:
            return quantity * leg.contract_value * (quote.best_bid - leg.entry_price)
        return quantity * leg.contract_value * (leg.entry_price - quote.best_ask)

    async def reconcile_realized_pnl(
        self,
        previous_open_legs: list[OptionLeg],
        current_open_legs: list[OptionLeg] | None = None,
    ) -> float:
        """Compute gross realized PnL for observed position reductions using new fills."""
        if current_open_legs is None:
            current_open_legs = await self.exchange.parse_option_positions()

        current_by_id = {leg.product_id: leg for leg in current_open_legs}
        realized_total = 0.0
        pending_fill_sizes: dict[str, tuple[float, float]] = {}
        pending_closed_positions: list[dict[str, Any]] = []

        for leg in previous_open_legs:
            current_leg = current_by_id.get(leg.product_id)
            if current_leg is None or leg.size * current_leg.size <= 0:
                closed_quantity = abs(leg.size)
            else:
                closed_quantity = max(0.0, abs(leg.size) - abs(current_leg.size))
            if closed_quantity <= 1e-12:
                continue

            closing_side = "sell" if leg.size > 0 else "buy"
            fills = await self.exchange.get_recent_fills(leg.product_id)
            matching_fills = sorted(
                (fill for fill in fills if self._is_new_closing_fill(fill, leg.product_id, closing_side)),
                key=self._fill_timestamp,
            )
            remaining_quantity = closed_quantity
            leg_realized = 0.0
            for fill in matching_fills:
                fill_id = self._fill_id(fill)
                if fill_id is None:
                    raise RuntimeError(f"Closing fill for product {leg.product_id} has no stable fill ID")
                if fill_id in self._seen_fill_ids:
                    continue

                fill_quantity = self._fill_number(fill, "size", "fill_size", "quantity")
                fill_price = self._fill_number(fill, "price", "fill_price")
                consumed_quantity = pending_fill_sizes.get(
                    fill_id,
                    (self._consumed_fill_sizes.get(fill_id, 0.0), fill_quantity),
                )[0]
                quantity = min(remaining_quantity, max(0.0, fill_quantity - consumed_quantity))
                if quantity <= 0:
                    continue

                if leg.size > 0:
                    fill_realized = (fill_price - leg.entry_price) * quantity * leg.contract_value
                else:
                    fill_realized = (leg.entry_price - fill_price) * quantity * leg.contract_value
                leg_realized += fill_realized
                remaining_quantity -= quantity
                pending_fill_sizes[fill_id] = (consumed_quantity + quantity, fill_quantity)
                closed_at = self._fill_timestamp(fill)
                if closed_at is None:
                    raise RuntimeError(f"Closing fill {fill_id} for product {leg.product_id} has no valid timestamp")
                pending_closed_positions.append(
                    {
                        "fill_id": fill_id,
                        "product_id": leg.product_id,
                        "symbol": leg.symbol,
                        "type": leg.option_type,
                        "option_type": leg.option_type,
                        "strike": leg.strike,
                        "expiry": leg.expiry.isoformat(),
                        "side": "Buy" if leg.size > 0 else "Sell",
                        "position_side": "long" if leg.size > 0 else "short",
                        "closing_side": closing_side,
                        "size": quantity if leg.size > 0 else -quantity,
                        "quantity": quantity,
                        "entry": leg.entry_price,
                        "entry_price": leg.entry_price,
                        "closed_price": fill_price,
                        "exit_price": fill_price,
                        "contract_value": leg.contract_value,
                        "realized_pnl": fill_realized,
                        "closed_time": closed_at.isoformat(),
                        "closed_at": closed_at.isoformat(),
                    }
                )
                if remaining_quantity <= 1e-12:
                    break

            if remaining_quantity > 1e-8:
                raise RuntimeError(
                    f"Fills cover only {closed_quantity - remaining_quantity:g} of "
                    f"{closed_quantity:g} closed contracts for product {leg.product_id}; "
                    "realized PnL was not booked"
                )
            realized_total += leg_realized

        for fill_id, (consumed_quantity, fill_quantity) in pending_fill_sizes.items():
            self._consumed_fill_sizes[fill_id] = consumed_quantity
            if consumed_quantity >= fill_quantity - 1e-8:
                self._seen_fill_ids.add(fill_id)
        self._closed_positions.extend(pending_closed_positions)
        return realized_total

    def _is_new_closing_fill(self, fill: dict[str, Any], product_id: int, side: str) -> bool:
        try:
            fill_product_id = int(fill.get("product_id"))
        except (TypeError, ValueError):
            return False
        fill_side = str(fill.get("side") or "").lower()
        timestamp = self._fill_timestamp(fill)
        return (
            fill_product_id == product_id
            and fill_side == side
            and timestamp is not None
            and timestamp >= self._realized_tracking_started_at
        )

    @staticmethod
    def _fill_id(fill: dict[str, Any]) -> str | None:
        for key in ("id", "fill_id", "trade_id"):
            value = fill.get(key)
            if value is not None and str(value):
                return str(value)
        return None

    @staticmethod
    def _fill_number(fill: dict[str, Any], *keys: str) -> float:
        for key in keys:
            value = fill.get(key)
            if value is not None:
                number = float(value)
                if math.isfinite(number) and number > 0:
                    return number
        raise RuntimeError(f"Fill is missing a positive numeric value for {', '.join(keys)}")

    @staticmethod
    def _fill_timestamp(fill: dict[str, Any]) -> datetime | None:
        value = next(
            (fill.get(key) for key in ("created_at", "created_time", "timestamp", "time") if fill.get(key) is not None),
            None,
        )
        if isinstance(value, (int, float)):
            timestamp = float(value)
            if abs(timestamp) >= 1e12:
                timestamp /= 1000
            return datetime.fromtimestamp(timestamp, tz=timezone.utc)
        if isinstance(value, str):
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
                return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)
            except ValueError:
                return None
        return None

