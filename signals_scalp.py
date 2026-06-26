"""
signals_scalp.py — Cálculo de sinais técnicos para o CSA v1.0

Sinais produzidos:
  - Zonas S/R com contagem de toques
  - RSI para 1h, 5m
  - ATR para 1h (actual e semanal)
  - Volume médio e relativo
  - Delta e CVD a partir de trades recentes
  - Bid/Ask walls do order book
"""

import math
import logging
from typing import Optional
from collections import defaultdict

from config import (
    SR_ZONE_TOLERANCE, SR_MIN_TOUCHES,
    WALL_MIN_USD, WALL_ZONE_PCT,
    CVD_CANDLES, CVD_ABSORB_RATIO,
    VOLUME_DRY_RATIO, VOLUME_COMPRESS_RATIO,
    ATR_COMPRESS_RATIO, RANGE_COMPRESS_PCT,
)

logger = logging.getLogger(__name__)


# ── RSI ───────────────────────────────────────────────────────────────────────

def calc_rsi(candles: list[dict], period: int = 14) -> Optional[float]:
    """RSI Wilder sobre os closes dos candles."""
    closes = [c["close"] for c in candles]
    if len(closes) < period + 1:
        return None

    gains, losses = [], []
    for i in range(1, period + 1):
        diff = closes[-period - 1 + i] - closes[-period - 2 + i]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    # suavização de Wilder para candles restantes
    for i in range(period, len(closes) - 1):
        diff = closes[i + 1] - closes[i]
        avg_gain = (avg_gain * (period - 1) + max(diff, 0)) / period
        avg_loss = (avg_loss * (period - 1) + max(-diff, 0)) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 2)


# ── ATR ───────────────────────────────────────────────────────────────────────

def calc_atr(candles: list[dict], period: int = 14) -> Optional[float]:
    """ATR sobre os últimos `period` candles."""
    if len(candles) < period + 1:
        return None

    trs = []
    for i in range(1, len(candles)):
        h = candles[i]["high"]
        l = candles[i]["low"]
        pc = candles[i - 1]["close"]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)

    if len(trs) < period:
        return None

    # ATR simples (média dos últimos `period` TRs)
    return round(sum(trs[-period:]) / period, 8)


# ── Zonas S/R ─────────────────────────────────────────────────────────────────

def find_sr_zones(candles_1h: list[dict], tolerance_pct: float = 0.005) -> list[dict]:
    """
    Identifica zonas S/R com >= SR_MIN_TOUCHES toques no 1h.
    Devolve lista de dicts: {price, touches, type: 'support'|'resistance'}
    """
    if len(candles_1h) < 20:
        return []

    # pivôs: máximos e mínimos locais
    pivots = []
    for i in range(2, len(candles_1h) - 2):
        h = candles_1h[i]["high"]
        l = candles_1h[i]["low"]
        # máximo local
        if h > candles_1h[i-1]["high"] and h > candles_1h[i-2]["high"] and \
           h > candles_1h[i+1]["high"] and h > candles_1h[i+2]["high"]:
            pivots.append({"price": h, "type": "resistance"})
        # mínimo local
        if l < candles_1h[i-1]["low"] and l < candles_1h[i-2]["low"] and \
           l < candles_1h[i+1]["low"] and l < candles_1h[i+2]["low"]:
            pivots.append({"price": l, "type": "support"})

    if not pivots:
        return []

    # agrupa pivôs próximos em zonas
    zones = []
    used = set()
    for i, p in enumerate(pivots):
        if i in used:
            continue
        zone_prices = [p["price"]]
        zone_type = p["type"]
        for j, q in enumerate(pivots):
            if j == i or j in used:
                continue
            if abs(q["price"] - p["price"]) / p["price"] <= tolerance_pct:
                zone_prices.append(q["price"])
                used.add(j)
        used.add(i)
        avg_price = sum(zone_prices) / len(zone_prices)
        zones.append({
            "price":   round(avg_price, 8),
            "touches": len(zone_prices),
            "type":    zone_type,
        })

    # só zonas com toques suficientes
    return [z for z in zones if z["touches"] >= SR_MIN_TOUCHES]


def nearest_sr_zone(
    current_price: float,
    zones: list[dict],
    tolerance_pct: float = SR_ZONE_TOLERANCE
) -> Optional[dict]:
    """
    Devolve a zona S/R mais próxima do preço actual (dentro de tolerance_pct).
    Inclui a direcção sugerida do trade.
    """
    best = None
    best_dist = float("inf")

    for z in zones:
        dist = abs(z["price"] - current_price) / current_price
        if dist <= tolerance_pct and dist < best_dist:
            best_dist = dist
            best = z.copy()

    if best:
        best["distance_pct"] = round(best_dist * 100, 3)
        # se suporte → long; se resistência → short
        best["direction"] = "LONG" if best["type"] == "support" else "SHORT"

    return best


# ── Volume ────────────────────────────────────────────────────────────────────

def calc_volume_stats(candles: list[dict], avg_periods: int = 20) -> dict:
    """
    Retorna: vol_last (volume candle actual), vol_avg (média N candles),
    vol_ratio (ratio actual/média), is_spike (>300%), is_dry (<60% da média 7d).
    """
    if len(candles) < avg_periods:
        return {"vol_last": 0, "vol_avg": 0, "vol_ratio": 0, "is_spike": False, "is_dry": False}

    vols = [c["volume"] for c in candles]
    vol_avg = sum(vols[-avg_periods:-1]) / (avg_periods - 1)
    vol_last = vols[-1]
    vol_ratio = vol_last / vol_avg if vol_avg > 0 else 0

    # média 7d = últimas 168 candles 1h (se disponíveis)
    avg_7d = sum(vols[-168:]) / min(len(vols), 168) if len(vols) >= 24 else vol_avg

    return {
        "vol_last":  round(vol_last, 2),
        "vol_avg":   round(vol_avg, 2),
        "vol_ratio": round(vol_ratio, 3),
        "is_spike":  vol_ratio >= 3.0,
        "is_dry":    vol_last < avg_7d * VOLUME_COMPRESS_RATIO,
        "is_drying": all(
            vols[-i] < vol_avg * VOLUME_DRY_RATIO
            for i in range(1, min(4, len(vols)))
        ),
    }


# ── Delta e CVD ───────────────────────────────────────────────────────────────

def calc_cvd(trades: list[dict]) -> dict:
    """
    Calcula delta e CVD a partir de trades recentes.
    Retorna: delta (buy_vol - sell_vol), cvd_trend ('absorção'|'divergência'|'neutro'),
    buy_pct, sell_pct.
    """
    if not trades:
        return {"delta": 0, "cvd_trend": "neutro", "buy_pct": 0.5, "sell_pct": 0.5, "absorbing": False}

    recent = trades[-CVD_CANDLES:]
    buy_vol  = sum(t["vol"] for t in recent if t["side"] == 1)
    sell_vol = sum(t["vol"] for t in recent if t["side"] == -1)
    total    = buy_vol + sell_vol

    delta    = buy_vol - sell_vol
    buy_pct  = buy_vol / total if total > 0 else 0.5
    sell_pct = sell_vol / total if total > 0 else 0.5

    # absorção: sell_vol dominante mas preço estável (detectado via CVD acumulado)
    # usa delta negativo como proxy de pressão vendedora
    absorbing = False
    if sell_vol > buy_vol * 1.3 and len(trades) >= CVD_CANDLES:
        # verifica se os últimos trades mostram preço a aguentar
        prices = [t["price"] for t in recent]
        price_drop = (prices[0] - prices[-1]) / prices[0] if prices[0] > 0 else 0
        absorbing = abs(price_drop) < CVD_ABSORB_RATIO  # preço não cedeu apesar de sell pressure

    # divergência: buy_vol dominante mas preço a cair (bearish divergence)
    diverging = buy_vol > sell_vol * 1.3 and len(trades) >= CVD_CANDLES

    trend = "absorção" if absorbing else ("divergência" if diverging else "neutro")

    return {
        "delta":     round(delta, 4),
        "cvd_trend": trend,
        "buy_pct":   round(buy_pct, 3),
        "sell_pct":  round(sell_pct, 3),
        "absorbing": absorbing,
    }


# ── Order Book — Bid/Ask Walls ────────────────────────────────────────────────

def find_walls(orderbook: dict, current_price: float, price_mult: float = None) -> dict:
    """
    Detecta bid e ask walls significativas no order book.
    Uma wall é um cluster de ordens > WALL_MIN_USD numa zona de WALL_ZONE_PCT do preço.
    """
    result = {
        "bid_wall": None,  # {price, usd_value, distance_pct}
        "ask_wall": None,
        "has_bid_wall": False,
        "has_ask_wall": False,
    }

    if not orderbook:
        return result

    zone_pct = WALL_ZONE_PCT

    def cluster_side(levels: list, is_bid: bool) -> Optional[dict]:
        """Agrupa níveis de preço próximos e encontra o cluster mais volumoso."""
        if not levels:
            return None

        # agrupa por zona de preço
        clusters = defaultdict(float)
        for price, qty in levels:
            # round ao nearest zone_pct
            zone_key = round(price / (current_price * zone_pct)) * (current_price * zone_pct)
            clusters[zone_key] += price * qty  # USD value

        if not clusters:
            return None

        # maior cluster
        best_zone = max(clusters, key=lambda k: clusters[k])
        best_usd  = clusters[best_zone]

        if best_usd < WALL_MIN_USD:
            return None

        return {
            "price":        round(best_zone, 8),
            "usd_value":    round(best_usd, 0),
            "distance_pct": round(abs(best_zone - current_price) / current_price * 100, 3),
        }

    bid_wall = cluster_side(orderbook.get("bids", []), is_bid=True)
    ask_wall = cluster_side(orderbook.get("asks", []), is_bid=False)

    if bid_wall:
        result["bid_wall"]      = bid_wall
        result["has_bid_wall"]  = True
    if ask_wall:
        result["ask_wall"]      = ask_wall
        result["has_ask_wall"]  = True

    return result


# ── ATR para Setup C ──────────────────────────────────────────────────────────

def check_volatility_compression(
    candles_1h: list[dict],
    atr_period: int = 14
) -> dict:
    """
    Verifica condições de compressão de volatilidade (Setup C).
    Compara ATR actual com ATR da semana anterior.
    """
    if len(candles_1h) < 168 + atr_period:
        return {"compressed": False, "atr_ratio": None, "range_pct": None}

    # ATR actual (últimas 14 candles)
    atr_now = calc_atr(candles_1h[-atr_period - 1:], period=atr_period)

    # ATR semana anterior (candles de -168 a -168+atr_period)
    week_slice = candles_1h[-168 - atr_period: -168 + atr_period]
    atr_week = calc_atr(week_slice, period=atr_period)

    if not atr_now or not atr_week or atr_week == 0:
        return {"compressed": False, "atr_ratio": None, "range_pct": None}

    atr_ratio = atr_now / atr_week

    # range dos últimos 4 candles
    last4 = candles_1h[-4:]
    high4 = max(c["high"] for c in last4)
    low4  = min(c["low"]  for c in last4)
    price = candles_1h[-1]["close"]
    range_pct = (high4 - low4) / price if price > 0 else 0

    compressed = (
        atr_ratio < ATR_COMPRESS_RATIO and
        range_pct < RANGE_COMPRESS_PCT
    )

    return {
        "compressed": compressed,
        "atr_ratio":  round(atr_ratio, 3),
        "atr_now":    round(atr_now, 8),
        "atr_week":   round(atr_week, 8),
        "range_pct":  round(range_pct * 100, 3),
    }


# ── Proxy Liquidações via OI ──────────────────────────────────────────────────

def detect_oi_cascade(oi_history: list[float]) -> dict:
    """
    Detecta cascade de liquidações via queda súbita de OI.
    oi_history: lista de OI snapshots (mais antigo → mais recente).
    Devolve: cascade detectado, magnitude da queda, direcção sugerida.
    """
    if len(oi_history) < 2:
        return {"cascade": False, "oi_drop_pct": 0, "direction": None}

    oi_start = oi_history[0]
    oi_end   = oi_history[-1]

    if oi_start <= 0:
        return {"cascade": False, "oi_drop_pct": 0, "direction": None}

    oi_change = (oi_end - oi_start) / oi_start  # positivo = aumento, negativo = queda

    cascade = abs(oi_change) >= 0.02  # queda ou subida > 2% = liquidações

    return {
        "cascade":     cascade,
        "oi_drop_pct": round(abs(oi_change) * 100, 3),
        "oi_change":   round(oi_change * 100, 3),  # negativo = longs liquidados, positivo = shorts
        "direction":   "LONG" if oi_change < 0 else "SHORT",  # bounce na direcção oposta
    }
