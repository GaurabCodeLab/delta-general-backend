from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import urllib3
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from src.models import OptionLeg, Quote

LOGGER = logging.getLogger(__name__)


class ExchangeClientError(RuntimeError):
    pass


class DeltaExchangeClient:
    """
    Thin async wrapper around delta-rest-client sync methods.
    This class intentionally keeps all API-shape assumptions in one place.
    If Delta field names differ for your account, adjust mapping here only.
    """

    def __init__(self, api_key: str, api_secret: str, base_url: str, ssl_verify: bool = True) -> None:
        try:
            from delta_rest_client import DeltaRestClient, OrderType, TimeInForce  # type: ignore
        except Exception as exc:  # pragma: no cover
            raise ExchangeClientError(
                "delta-rest-client import failed. Install with: pip install delta-rest-client"
            ) from exc

        self._client = DeltaRestClient(
            base_url=base_url,
            api_key=api_key,
            api_secret=api_secret,
        )
        self._client.session.verify = ssl_verify
        if not ssl_verify:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self._order_type_market = OrderType.MARKET
        self._tif_ioc = TimeInForce.IOC

    @staticmethod
    def _to_dt(value: Any) -> datetime:
        if isinstance(value, datetime):
            return value
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        if isinstance(value, str):
            # Supports ISO strings ending in Z.
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError as exc:
                raise ExchangeClientError(f"Unsupported datetime value: {value}") from exc
        raise ExchangeClientError(f"Unsupported datetime value: {value}")

    @retry(
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=0.5, min=0.5, max=4),
        retry=retry_if_exception_type(Exception),
        reraise=True,
    )
    async def _call(self, method_name: str, **kwargs: Any) -> Any:
        method = getattr(self._client, method_name, None)
        if method is None:
            raise ExchangeClientError(f"delta-rest-client missing method: {method_name}")

        try:
            return await asyncio.to_thread(method, **kwargs)
        except TypeError:
            # Some client methods may not accept kwargs; try positionalless call.
            if kwargs:
                return await asyncio.to_thread(method)
            raise

    async def get_index_price(self, symbol: str = "BTCUSD") -> float:
        try:
            data = await self._call("get_ticker", **{"identifier": symbol, "auth": True})
            price = data.get("spot_price")
            if price is not None:
                return float(price)
        except Exception:   
            raise ExchangeClientError("Unable to fetch BTC index price from client methods.")

    async def get_open_positions_raw(self) -> List[Dict[str, Any]]:
        last_error = None     
        try:
            resp = await self._call("request", method="GET", path="/v2/positions/margined", auth=True)
            payload = await asyncio.to_thread(resp.json)
            LOGGER.debug(f"Margined positions response: {payload}")
            success = payload.get("success")
            if success is False:
                last_error = payload.get("error") or payload.get("message")
                raise ExchangeClientError(f"Unable to fetch open positions. Last error: {last_error}")
            else:
                result = payload.get("result") or []
                return result
        except Exception as exc:
            LOGGER.debug(f"Margined positions endpoint failed: {str(exc)}")
            last_error = str(exc)
            raise ExchangeClientError(f"Unable to fetch open positions. Last error: {last_error}")

    async def get_position_size(self, product_id: int) -> float:
        """Return signed net position size for a product (positive long, negative short)."""
        get_position = getattr(self._client, "get_position", None)
        if get_position is not None:
            position = await self._call("get_position", product_id=product_id)
            if not isinstance(position, dict):
                raise ExchangeClientError("Unexpected real-time position response")
            return float(position.get("size") or 0.0)

        positions = await self.get_open_positions_raw()
        for pos in positions:
            try:              
                pid = int(pos.get("product_id"))
                if pid != product_id:
                    continue
                return float(pos.get("size"))
            except Exception:
                continue
        return 0.0

    async def get_best_quote(self, product_id: int, symbol: str | None = None) -> Quote:
        errors = []
        try:
            data = await self._call("get_ticker", identifier=symbol or product_id, auth=True)
            LOGGER.debug(f"Quote method get_ticker for product {product_id}: {data}")
            root = data.get("quotes", data) if isinstance(data, dict) else {}
            best_bid = root.get("best_bid")
            best_ask = root.get("best_ask")
            if best_bid is not None and best_ask is not None:
                return Quote(best_bid=float(best_bid), best_ask=float(best_ask))              
        except Exception as exc:
            error_msg = f"METHOD failed: {str(exc)}"
            LOGGER.debug(f"Quote fetch attempt for product {product_id}: {error_msg}")
            errors.append(error_msg)
            error_summary = "; ".join(errors)
            raise ExchangeClientError(f"Unable to fetch quote for product_id={product_id}. Tried: {error_summary}")

    async def place_market_order(
        self,
        product_id: int,
        side: str,
        size: float,
        reduce_only: bool,
    ) -> Dict[str, Any]:
        params = {
            "product_id": product_id,
            "size": size,
            "side": side,
            "order_type": self._order_type_market,
            "time_in_force": self._tif_ioc,
            "reduce_only": "true" if reduce_only else "false",
        }
        LOGGER.info(
            "Placing market order product_id=%s side=%s size=%s reduce_only=%s",
            product_id,
            side,
            size,
            reduce_only,
        )
        resp = await self._call("place_order", **params)
        if not isinstance(resp, dict):
            raise ExchangeClientError("Unexpected place_order response")
        return resp

    async def get_recent_fills(self, product_id: Optional[int] = None, limit: int = 500) -> List[Dict[str, Any]]:
        if limit <= 0:
            return []

        limit = min(limit, 5000)
        page_size = min(100, limit)
        query: Dict[str, Any] = {}
        if product_id is not None:
            query["product_ids"] = product_id

        fills: List[Dict[str, Any]] = []
        after: Optional[str] = None
        seen_cursors: set[str] = set()

        while len(fills) < limit:
            page = await self._call(
                "fills",
                query=dict(query),
                page_size=min(page_size, limit - len(fills)),
                after=after,
            )
            if not isinstance(page, dict):
                raise ExchangeClientError("Unexpected fills response: expected an object")
            if page.get("success") is False:
                raise ExchangeClientError(f"Unable to fetch fills: {page.get('error') or page.get('message')}")

            result = page.get("result")
            if not isinstance(result, list):
                raise ExchangeClientError("Unexpected fills response: result must be a list")
            if any(not isinstance(fill, dict) for fill in result):
                raise ExchangeClientError("Unexpected fills response: fill entries must be objects")
            fills.extend(result)

            meta = page.get("meta")
            next_cursor = meta.get("after") if isinstance(meta, dict) else None
            if not result or not next_cursor:
                break
            next_cursor = str(next_cursor)
            if next_cursor in seen_cursors:
                raise ExchangeClientError("Fills pagination returned a repeated cursor")
            seen_cursors.add(next_cursor)
            after = next_cursor

        return fills[:limit]

    async def parse_option_positions(self) -> List[OptionLeg]:
        positions = await self.get_open_positions_raw()
        legs: List[OptionLeg] = []
        LOGGER.info(f"Parsing {len(positions)} raw positions")
        
        for idx, pos in enumerate(positions):
            size = float(pos.get("size"))
            if abs(size) <= 0:
                LOGGER.debug(f"Position {idx}: skipped (zero size)")
                continue

            product_id = int(pos.get("product_id"))
            product = pos.get("product") if isinstance(pos.get("product"), dict) else None
            if not product:
                LOGGER.debug(f"Position {idx}: skipped (no product found for id={product_id})")
                continue

            contract_type = str(product.get("contract_type") or "").lower()
            if "option" not in contract_type:
                LOGGER.debug(f"Position {idx}: skipped (contract_type={contract_type}, not option)")
                continue
            elif "call" in contract_type:
                option_type = "call"
            elif "put" in contract_type:
                option_type = "put"
            else:
                LOGGER.debug(f"Position {idx}: skipped (unable to determine option_type)")
                continue

            symbol = str(product.get("symbol") or product_id)
            underlying = product.get("underlying_asset")
            underlying_symbol = (
                str(underlying.get("symbol") or "").upper()
                if isinstance(underlying, dict)
                else str(product.get("underlying_asset_symbol") or "").upper()
            )
            if underlying_symbol and underlying_symbol != "BTC":
                LOGGER.debug("Position %s: skipped (underlying=%s, not BTC)", idx, underlying_symbol)
                continue
            if not underlying_symbol and not symbol.upper().startswith("BTC"):
                LOGGER.debug("Position %s: skipped (unable to confirm BTC underlying)", idx)
                continue

            strike = float(product.get("strike_price"))
            expiry_raw = product.get("settlement_time")
            if strike <= 0 or not expiry_raw:
                LOGGER.debug(f"Position {idx}: skipped (strike={strike}, expiry={expiry_raw})")
                continue

            entry_price = float(pos.get("entry_price"))
            contract_value = float(product.get("contract_value"))

            leg = OptionLeg(
                product_id=product_id,
                symbol=symbol,
                option_type=option_type,
                strike=strike,
                expiry=self._to_dt(expiry_raw),
                size=size,
                entry_price=entry_price,
                contract_value=contract_value,
            )
            legs.append(leg)
            LOGGER.info(f"Position {idx}: parsed as {leg.symbol} {option_type} strike={strike} size={size}")
        
        LOGGER.info(f"Parsed {len(legs)} option positions from {len(positions)} raw positions")
        return legs
