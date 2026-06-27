"""
mexc_client.py — Cliente MEXC para o CSA v1.0
Endpoints: tickers, candles 1m/5m/1h/4h, order book, trades recentes, OI, funding
"""

import time
import logging
import asyncio
import aiohttp
from typing import Optional

from config import MEXC_BASE, REQUEST_DELAY

logger = logging.getLogger(__name__)


class MexcClient:
    """Cliente assíncrono para a API pública MEXC Futures."""

    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self.base = MEXC_BASE

    async def _get(self, path: str, params: dict = None) -> Optional[dict]:
        url = f"{self.base}{path}"
        try:
            async with self.session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status == 200:
                    data = await r.json(content_type=None)
                    if isinstance(data, list):
                        return {"success": True, "data": data}
                    if isinstance(data, dict):
                        if data.get("success") is False:
                            logger.debug(f"MEXC erro {path}: {data.get('message')}")
                            return None
                        return data
                    return None
                else:
                    logger.debug(f"MEXC HTTP {r.status} {path}")
                    return None
        except asyncio.TimeoutError:
            logger.debug(f"MEXC timeout {path}")
            return None
        except Exception as e:
            logger.debug(f"MEXC excepção {path}: {e}")
            return None

    # ── Universo ──────────────────────────────────────────────────────────────

    async def get_all_tickers(self) -> list[dict]:
        """Todos os tickers de futuros perpétuos USDT-M."""
        data = await self._get("/api/v1/contract/ticker")
        if not data:
            return []
        tickers = data.get("data", [])
        return [t for t in tickers if str(t.get("symbol", "")).endswith("_USDT")]

    async def get_ticker(self, symbol: str) -> Optional[dict]:
        """
        Ticker individual.
        MEXC pode devolver 'data' como lista ou dict directamente
        quando é pedido um símbolo específico.
        """
        data = await self._get("/api/v1/contract/ticker", {"symbol": symbol})
        if not data:
            return None
        d = data.get("data", [])
        if isinstance(d, dict):
            return d
        if isinstance(d, list):
            return d[0] if d else None
        return None

    # ── Candles ───────────────────────────────────────────────────────────────

    async def get_candles(self, symbol: str, interval: str, limit: int = 200) -> list[dict]:
        """
        Candles OHLCV.
        interval: Min1, Min5, Min15, Min30, Min60, Hour4, Hour8, Day1
        Devolve lista de dicts com: time, open, high, low, close, volume
        """
        data = await self._get(
            f"/api/v1/contract/kline/{symbol}",
            {"interval": interval, "limit": limit}
        )
        if not data or not data.get("data"):
            return []

        raw = data["data"]
        candles = []
        times  = raw.get("time",  [])
        opens  = raw.get("open",  [])
        closes = raw.get("close", [])
        highs  = raw.get("high",  [])
        lows   = raw.get("low",   [])
        vols   = raw.get("vol",   [])

        for i in range(len(times)):
            try:
                candles.append({
                    "time":   int(times[i]),
                    "open":   float(opens[i]),
                    "high":   float(highs[i]),
                    "low":    float(lows[i]),
                    "close":  float(closes[i]),
                    "volume": float(vols[i]),
                })
            except (IndexError, ValueError, TypeError):
                continue

        return candles  # ordem cronológica (mais antigo primeiro)

    # ── Order Book ────────────────────────────────────────────────────────────

    async def get_orderbook(self, symbol: str, depth: int = 100) -> Optional[dict]:
        """
        Order book até `depth` níveis.
        MEXC devolve asks/bids como lista de triplos [price, qty, count].
        Devolve dict com keys 'bids' e 'asks', cada um lista de [price, qty].
        """
        data = await self._get(
            f"/api/v1/contract/depth/{symbol}",
            {"limit": depth}
        )
        if not data or not data.get("data"):
            return None

        d = data["data"]
        try:
            bids = [[float(row[0]), float(row[1])] for row in d.get("bids", [])]
            asks = [[float(row[0]), float(row[1])] for row in d.get("asks", [])]
        except (IndexError, ValueError, TypeError):
            return None

        return {"bids": bids, "asks": asks}

    # ── Trades recentes ───────────────────────────────────────────────────────

    async def get_recent_trades(self, symbol: str, limit: int = 100) -> list[dict]:
        """
        Trades recentes — base para cálculo de delta e CVD.
        Devolve lista de dicts: price, vol, side (1=buy, -1=sell), time
        """
        data = await self._get(
            f"/api/v1/contract/deals/{symbol}",
            {"limit": limit}
        )
        if not data:
            return []

        raw = data.get("data", [])
        if not isinstance(raw, list):
            raw = raw.get("resultList", []) if isinstance(raw, dict) else []

        trades = []
        for t in raw:
            try:
                trades.append({
                    "price": float(t["p"]),
                    "vol":   float(t["v"]),
                    "side":  1 if int(t["T"]) == 1 else -1,  # 1=buy, 2=sell
                    "time":  int(t["t"]),
                })
            except (KeyError, ValueError, TypeError):
                continue
        return trades

    # ── Open Interest ─────────────────────────────────────────────────────────

    async def get_open_interest(self, symbol: str) -> Optional[float]:
        """OI em USD."""
        data = await self._get(f"/api/v1/contract/open_interest/{symbol}")
        if not data or not data.get("data"):
            return None
        try:
            return float(data["data"].get("openInterest", 0))
        except (ValueError, TypeError):
            return None

    # ── Funding Rate ──────────────────────────────────────────────────────────

    async def get_funding_rate(self, symbol: str) -> Optional[float]:
        """Funding rate actual."""
        data = await self._get(f"/api/v1/contract/funding_rate/{symbol}")
        if not data or not data.get("data"):
            return None
        try:
            return float(data["data"].get("fundingRate", 0))
        except (ValueError, TypeError):
            return None

    # ── Detalhe do contrato ───────────────────────────────────────────────────

    async def get_contract_detail(self, symbol: str) -> Optional[dict]:
        """Detalhes do contrato incluindo tick size e min qty."""
        data = await self._get("/api/v1/contract/detail", {"symbol": symbol})
        if not data or not data.get("data"):
            return None
        details = data["data"]
        if isinstance(details, list):
            for d in details:
                if d.get("symbol") == symbol:
                    return d
            return None
        return details
