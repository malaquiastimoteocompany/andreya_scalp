"""
scanner_scalp.py — Loop principal CSA v1.0 (Railway)

Processo contínuo — scan a cada ~2.5 minutos por ciclo completo.
Detecta setups A, B, C e envia alertas para o canal Telegram Scalp.
"""

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Optional

import aiohttp

from config import (
    SCAN_INTERVAL_SEC, REQUEST_DELAY,
    MIN_VOLUME_24H, MIN_OI, MAX_SPREAD_PCT, MIN_CANDLES_1H,
    OI_HISTORY_MINS,
)
from mexc_client import MexcClient
from signals_scalp import (
    calc_rsi, calc_atr, find_sr_zones, nearest_sr_zone,
    calc_volume_stats, calc_cvd, find_walls,
    check_volatility_compression, detect_oi_cascade,
)
from scoring_scalp import calcular_score
from notificacoes_scalp import enviar_alerta_scalp, enviar_status_scalp
from notion_scalp import log_alerta_csa

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CSA] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Estado do scanner ─────────────────────────────────────────────────────────

# historial de OI por token para detectar cascades
# {symbol: [oi_t0, oi_t1, ...]} (últimos OI_HISTORY_MINS snapshots)
_oi_history: dict[str, list[float]] = {}

# cooldown de alertas: {symbol: timestamp_último_alerta}
_alert_cooldown: dict[str, float] = {}
ALERT_COOLDOWN_SECS = 900  # 15 min — não repetir alerta no mesmo token

# estado CFI (partilhado com CFI via state.json se disponível)
_cfi_states: dict[str, str] = {}


def _load_cfi_states() -> dict[str, str]:
    """Carrega estados CFI do state.json se disponível."""
    import json
    import os
    path = os.path.join(os.path.dirname(__file__), "..", "andreya_2.0", "state.json")
    if not os.path.exists(path):
        # fallback: sem confluência CFI
        return {}
    try:
        with open(path) as f:
            data = json.load(f)
        states = {}
        for token_data in data.get("tokens", {}).values():
            sym = token_data.get("symbol", "")
            state = token_data.get("state", "E1")
            if sym:
                states[sym] = state
        return states
    except Exception as e:
        logger.debug(f"Não foi possível carregar state.json CFI: {e}")
        return {}


# ── Filtro de liquidez ────────────────────────────────────────────────────────

def _passes_liquidity_filter(ticker: dict) -> tuple[bool, str]:
    """Verifica se o token passa os filtros de liquidez para scalp."""
    try:
        vol24 = float(ticker.get("volume24", 0)) * float(ticker.get("lastPrice", 0))
    except (ValueError, TypeError):
        return False, "volume inválido"

    if vol24 < MIN_VOLUME_24H:
        return False, f"vol24h ${vol24:,.0f} < ${MIN_VOLUME_24H:,.0f}"

    try:
        bid = float(ticker.get("bid1", 0))
        ask = float(ticker.get("ask1", 0))
        mid = (bid + ask) / 2
        spread = (ask - bid) / mid if mid > 0 else 1
    except (ValueError, TypeError):
        return False, "spread inválido"

    if spread > MAX_SPREAD_PCT:
        return False, f"spread {spread*100:.3f}% > {MAX_SPREAD_PCT*100:.1f}%"

    return True, "ok"


# ── Análise de um token ───────────────────────────────────────────────────────

async def _analyze_token(
    client: MexcClient,
    symbol: str,
    ticker: dict,
    session: aiohttp.ClientSession,
) -> Optional[dict]:
    """
    Analisa um token e retorna resultado de scoring ou None se score insuficiente.
    """

    current_price = float(ticker.get("lastPrice", 0))
    if current_price <= 0:
        return None

    # ── Dados base ────────────────────────────────────────────────────────────
    candles_1h = await client.get_candles(symbol, "Min60", limit=200)
    await asyncio.sleep(REQUEST_DELAY)

    if len(candles_1h) < MIN_CANDLES_1H:
        return None  # histórico insuficiente

    candles_5m = await client.get_candles(symbol, "Min5", limit=50)
    await asyncio.sleep(REQUEST_DELAY)

    # ── Sinais técnicos ───────────────────────────────────────────────────────
    rsi_1h = calc_rsi(candles_1h, period=14)
    rsi_5m = calc_rsi(candles_5m, period=14) if candles_5m else None
    atr_1h = calc_atr(candles_1h, period=14)

    vol_stats = calc_volume_stats(candles_1h, avg_periods=20)

    # S/R zones (usa últimas 100 candles 1h)
    sr_zones = find_sr_zones(candles_1h[-100:])
    compression = check_volatility_compression(candles_1h)

    # ── Order book ────────────────────────────────────────────────────────────
    orderbook = await client.get_orderbook(symbol, depth=50)
    await asyncio.sleep(REQUEST_DELAY)
    walls = find_walls(orderbook, current_price) if orderbook else {
        "bid_wall": None, "ask_wall": None,
        "has_bid_wall": False, "has_ask_wall": False,
    }

    # ── CVD ───────────────────────────────────────────────────────────────────
    trades = await client.get_recent_trades(symbol, limit=100)
    await asyncio.sleep(REQUEST_DELAY)
    cvd = calc_cvd(trades)

    # ── OI + cascade detector ─────────────────────────────────────────────────
    oi_now = await client.get_open_interest(symbol)
    await asyncio.sleep(REQUEST_DELAY)

    if oi_now and oi_now > 0:
        hist = _oi_history.setdefault(symbol, [])
        hist.append(oi_now)
        # manter apenas últimas OI_HISTORY_MINS entradas
        # (uma entrada por ciclo de ~2.5 min → ~6 por 15 min)
        max_entries = max(2, OI_HISTORY_MINS // 3)
        if len(hist) > max_entries:
            hist.pop(0)
    else:
        oi_now = 0

    oi_cascade = detect_oi_cascade(_oi_history.get(symbol, [oi_now]))

    # ── Filtro OI mínimo ──────────────────────────────────────────────────────
    if oi_now < MIN_OI:
        return None

    # ── Determinar direcção candidata ─────────────────────────────────────────
    # Prioridade: zone S/R → cascade → RSI
    directions_to_test = []

    # Setup B: direcção baseada no cascade
    if oi_cascade.get("cascade"):
        directions_to_test.append(oi_cascade["direction"])

    # Setup A: direcção baseada na zona S/R mais próxima
    sr_zone = nearest_sr_zone(current_price, sr_zones)
    if sr_zone and sr_zone["direction"] not in directions_to_test:
        directions_to_test.append(sr_zone["direction"])

    # Setup C: direcção baseada em RSI ou neutra (testa ambas)
    if compression and compression.get("compressed"):
        for d in ("LONG", "SHORT"):
            if d not in directions_to_test:
                directions_to_test.append(d)

    # se nenhuma direcção candidata, testa ambas com RSI
    if not directions_to_test:
        if rsi_1h and rsi_1h < 35:
            directions_to_test.append("LONG")
        elif rsi_1h and rsi_1h > 65:
            directions_to_test.append("SHORT")

    if not directions_to_test:
        return None

    # ── Funding rate ──────────────────────────────────────────────────────────
    funding_rate = await client.get_funding_rate(symbol)
    await asyncio.sleep(REQUEST_DELAY)

    # ── Estado CFI ────────────────────────────────────────────────────────────
    cfi_state = _cfi_states.get(symbol, "E1")

    # ── Score por direcção — usa o melhor ─────────────────────────────────────
    best_result = None
    best_score  = 0

    for direction in directions_to_test:
        # para Setup A, usa a zona correcta para esta direcção
        sr_for_dir = sr_zone if sr_zone and sr_zone["direction"] == direction else None

        result = calcular_score(
            direction=direction,
            sr_zone=sr_for_dir,
            rsi_1h=rsi_1h,
            rsi_5m=rsi_5m,
            walls=walls,
            cvd=cvd,
            vol_stats=vol_stats,
            oi_cascade=oi_cascade,
            cfi_state=cfi_state,
            compression=compression,
        )

        if result["score"] > best_score:
            best_score  = result["score"]
            best_result = {
                "direction":    direction,
                "scoring":      result,
                "sr_zone":      sr_for_dir,
                "rsi_1h":       rsi_1h,
                "funding_rate": funding_rate,
                "cfi_state":    cfi_state,
                "price":        current_price,
            }

    return best_result if (best_result and best_result["scoring"]["send"]) else None


# ── Ciclo principal ───────────────────────────────────────────────────────────

async def run_scanner():
    """Loop principal do CSA — corre indefinidamente no Railway."""

    logger.info("=" * 50)
    logger.info("CSA v1.0 — Crypto Scalp Alerts — A arrancar")
    logger.info("=" * 50)

    # carrega estados CFI na startup
    global _cfi_states
    _cfi_states = _load_cfi_states()
    logger.info(f"Estados CFI carregados: {len(_cfi_states)} tokens")

    connector = aiohttp.TCPConnector(limit=20, limit_per_host=10)

    async with aiohttp.ClientSession(connector=connector) as session:
        client = MexcClient(session)

        # ── Notificação de arranque ───────────────────────────────────────────
        await enviar_status_scalp(
            session,
            f"CSA v1.0 activo\n"
            f"Scan interval: {SCAN_INTERVAL_SEC}s\n"
            f"Threshold: score >= 6/10\n"
            f"Universo: ~500 tokens MEXC"
        )

        cycle = 0
        while True:
            cycle += 1
            t_start = time.time()
            now_str = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
            logger.info(f"── Ciclo #{cycle} | {now_str} ──────────────────────")

            # actualiza estados CFI a cada 10 ciclos (~25 min)
            if cycle % 10 == 0:
                _cfi_states = _load_cfi_states()
                logger.info(f"Estados CFI actualizados: {len(_cfi_states)} tokens")

            # ── Tickers ───────────────────────────────────────────────────────
            tickers = await client.get_all_tickers()
            if not tickers:
                logger.warning("Sem tickers — aguardar próximo ciclo")
                await asyncio.sleep(30)
                continue

            # ── Filtro de liquidez ────────────────────────────────────────────
            elegíveis = []
            for t in tickers:
                passes, reason = _passes_liquidity_filter(t)
                if passes:
                    elegíveis.append(t)

            logger.info(f"Tokens elegíveis para scalp: {len(elegíveis)}/{len(tickers)}")

            # ── Priorização: E3 > E2 > volume ────────────────────────────────
            def priority_key(t):
                sym   = t.get("symbol", "")
                state = _cfi_states.get(sym, "E1")
                state_score = {"E3": 3, "E2": 2, "E1": 1}.get(state, 0)
                try:
                    vol = float(t.get("volume24", 0)) * float(t.get("lastPrice", 0))
                except Exception:
                    vol = 0
                return (state_score, vol)

            elegíveis.sort(key=priority_key, reverse=True)

            # ── Análise ───────────────────────────────────────────────────────
            alertas_enviados = 0
            tokens_analisados = 0

            for ticker in elegíveis:
                symbol = ticker.get("symbol", "")
                if not symbol:
                    continue

                # cooldown: não repetir alerta recente
                last_alert = _alert_cooldown.get(symbol, 0)
                if time.time() - last_alert < ALERT_COOLDOWN_SECS:
                    continue

                tokens_analisados += 1

                try:
                    result = await _analyze_token(client, symbol, ticker, session)
                except Exception as e:
                    logger.error(f"Erro a analisar {symbol}: {e}")
                    continue

                if result is None:
                    continue

                score    = result["scoring"]["score"]
                priority = result["scoring"]["priority"]
                direction = result["direction"]

                logger.info(
                    f"🎯 {symbol} {direction} | Score: {score}/10"
                    f"{' 🔥' if priority else ''}"
                    f" | Setup {result['scoring']['setup_type'] or '?'}"
                )

                # ── Enviar alerta ─────────────────────────────────────────────
                sent = await enviar_alerta_scalp(
                    session=session,
                    symbol=symbol,
                    direction=direction,
                    price=result["price"],
                    scoring=result["scoring"],
                    sr_zone=result["sr_zone"],
                    rsi_1h=result["rsi_1h"],
                    funding_rate=result["funding_rate"],
                    cfi_state=result["cfi_state"],
                )

                if sent:
                    alertas_enviados += 1
                    _alert_cooldown[symbol] = time.time()

                    # ── Log Notion ────────────────────────────────────────────
                    await log_alerta_csa(
                        session=session,
                        symbol=symbol,
                        direction=direction,
                        price=result["price"],
                        score=score,
                        setup_type=result["scoring"]["setup_type"],
                        sr_zone=result["sr_zone"],
                        rsi_1h=result["rsi_1h"],
                        funding_rate=result["funding_rate"],
                        cfi_state=result["cfi_state"],
                        priority=priority,
                    )

                # limite de alertas por ciclo para não fazer flood
                if alertas_enviados >= 3:
                    logger.info("Limite de 3 alertas por ciclo atingido")
                    break

            # ── Sumário do ciclo ──────────────────────────────────────────────
            elapsed = time.time() - t_start
            logger.info(
                f"Ciclo #{cycle} concluído | "
                f"{tokens_analisados} analisados | "
                f"{alertas_enviados} alertas | "
                f"{elapsed:.1f}s"
            )

            # ── Aguardar próximo ciclo ────────────────────────────────────────
            wait = max(0, SCAN_INTERVAL_SEC - elapsed)
            if wait > 0:
                await asyncio.sleep(wait)


# ── Entrypoint ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    asyncio.run(run_scanner())
