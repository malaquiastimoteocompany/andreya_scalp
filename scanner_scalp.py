"""
scanner_scalp.py — Loop principal CSA v1.0 (Railway)

Processo contínuo — scan em duas fases:
  Fase 1 (rápida): candles 1h + RSI + S/R → filtra candidatos
  Fase 2 (completa): orderbook + trades + OI → scoring final

Só tokens que passam a Fase 1 avançam para a Fase 2.
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
    OI_HISTORY_MINS, RSI_LONG_MAX, RSI_SHORT_MIN,
    SR_ZONE_TOLERANCE, SYMBOL_BLACKLIST,
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
from monitor_alertas import monitor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CSA] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Estado do scanner ─────────────────────────────────────────────────────────

_oi_history: dict[str, list[float]] = {}
_alert_cooldown: dict[str, float] = {}
ALERT_COOLDOWN_SECS = 900  # 15 min por token

_cfi_states: dict[str, str] = {}


def _load_cfi_states() -> dict[str, str]:
    import json, os, urllib.request, base64

    github_token = os.environ.get("GITHUB_TOKEN", "")
    github_repo  = os.environ.get("GITHUB_REPO", "malaquiastimoteocompany/andreya_2.0")

    if not github_token:
        return {}

    url = f"https://api.github.com/repos/{github_repo}/contents/state.json"
    req = urllib.request.Request(url, headers={
        "Authorization": f"token {github_token}",
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "andreya-scalp",
    })
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            resp    = json.loads(r.read())
            content = base64.b64decode(resp["content"]).decode()
            data    = json.loads(content)
        states = {}
        for sym, campos in data.items():
            estado = campos.get("estado", 1)
            states[sym] = f"E{estado}"
        logger.info(f"Estados CFI carregados: {len(states)} tokens")
        return states
    except Exception as e:
        logger.debug(f"Não foi possível carregar state.json CFI: {e}")
        return {}


# ── Filtro de liquidez ────────────────────────────────────────────────────────

def _calcular_spread(ticker: dict) -> Optional[float]:
    """Spread bid-ask em fracção (0.001 = 0.1%) — mesma fórmula usada em
    _passes_liquidity_filter, extraída para reaproveitar no registo do
    alerta (12/07/2026)."""
    try:
        bid = float(ticker.get("bid1", 0))
        ask = float(ticker.get("ask1", 0))
        mid = (bid + ask) / 2
        if mid <= 0:
            return None
        return (ask - bid) / mid
    except (ValueError, TypeError):
        return None


def _passes_liquidity_filter(ticker: dict) -> bool:
    try:
        # CORRIGIDO 07/07/2026: volume24 da MEXC já vem em USD (confirmado
        # com dados reais — ex. SIREN_USDT tinha volume24=1.110.781, que já
        # é ~$1.1M, não uma quantidade de moedas). Multiplicar por lastPrice
        # estragava a conta: esmagava tokens baratos (excluía-os por engano)
        # e inflacionava tokens caros (incluía-os por engano) — o total
        # ficava parecido, mas o conjunto de tokens elegíveis estava errado.
        vol24 = float(ticker.get("volume24", 0))
        if vol24 < MIN_VOLUME_24H:
            return False
        bid = float(ticker.get("bid1", 0))
        ask = float(ticker.get("ask1", 0))
        mid = (bid + ask) / 2
        if mid <= 0:
            return False
        spread = (ask - bid) / mid
        return spread <= MAX_SPREAD_PCT
    except (ValueError, TypeError):
        return False


# ── FASE 1 — Pre-filtro rápido ────────────────────────────────────────────────

async def _prefilter_token(
    client: MexcClient,
    symbol: str,
    ticker: dict,
) -> Optional[dict]:
    """
    Análise rápida: só candles 1h.
    Devolve dict com dados base se o token é candidato, None caso contrário.
    Critério: tem zona S/R próxima OU RSI extremo OU compressão ATR.
    """
    current_price = float(ticker.get("lastPrice", 0))
    if current_price <= 0:
        return None

    candles_1h = await client.get_candles(symbol, "Min60", limit=200)
    await asyncio.sleep(REQUEST_DELAY)

    if len(candles_1h) < MIN_CANDLES_1H:
        return None

    rsi_1h      = calc_rsi(candles_1h, period=14)
    vol_stats   = calc_volume_stats(candles_1h, avg_periods=20)
    sr_zones    = find_sr_zones(candles_1h[-100:])
    sr_zone     = nearest_sr_zone(current_price, sr_zones)
    compression = check_volatility_compression(candles_1h)
    atr_1h      = calc_atr(candles_1h, period=14)

    # critérios de pré-selecção (pelo menos 1 deve passar)
    has_sr          = sr_zone is not None
    has_rsi_long    = rsi_1h is not None and rsi_1h < RSI_LONG_MAX
    has_rsi_short   = rsi_1h is not None and rsi_1h > RSI_SHORT_MIN
    has_compression = compression.get("compressed", False)
    has_vol_spike   = vol_stats.get("is_spike", False)

    if not any([has_sr, has_rsi_long, has_rsi_short, has_compression, has_vol_spike]):
        return None

    return {
        "price":       current_price,
        "candles_1h":  candles_1h,
        "rsi_1h":      rsi_1h,
        "vol_stats":   vol_stats,
        "sr_zones":    sr_zones,
        "sr_zone":     sr_zone,
        "compression": compression,
        "atr_1h":      atr_1h,
    }


# ── FASE 2 — Análise completa ─────────────────────────────────────────────────

async def _analyze_candidate(
    client: MexcClient,
    symbol: str,
    base: dict,
) -> Optional[dict]:
    """
    Análise completa de um candidato da Fase 1.
    Busca: orderbook, trades recentes, OI, funding rate.
    Devolve sempre o melhor resultado (mesmo abaixo do threshold),
    para permitir logging de diagnóstico no ciclo principal.
    """
    current_price = base["price"]
    candles_1h    = base["candles_1h"]
    rsi_1h        = base["rsi_1h"]
    vol_stats     = base["vol_stats"]
    sr_zone       = base["sr_zone"]
    compression   = base["compression"]
    atr_1h        = base.get("atr_1h")

    # orderbook
    orderbook = await client.get_orderbook(symbol, depth=50)
    await asyncio.sleep(REQUEST_DELAY)
    walls = find_walls(orderbook, current_price) if orderbook else {
        "bid_wall": None, "ask_wall": None,
        "has_bid_wall": False, "has_ask_wall": False,
        "depth_ratio": None,
    }

    # trades → CVD
    trades = await client.get_recent_trades(symbol, limit=100)
    await asyncio.sleep(REQUEST_DELAY)
    cvd = calc_cvd(trades)

    # OI → cascade detector
    oi_now = await client.get_open_interest(symbol)
    await asyncio.sleep(REQUEST_DELAY)

    if oi_now and oi_now > 0:
        if oi_now < MIN_OI:
            return None  # OI insuficiente para scalp
        hist = _oi_history.setdefault(symbol, [])
        hist.append(oi_now)
        max_entries = max(2, OI_HISTORY_MINS // 3)
        if len(hist) > max_entries:
            hist.pop(0)
    else:
        oi_now = 0

    oi_cascade = detect_oi_cascade(_oi_history.get(symbol, [oi_now] if oi_now else [0]))

    # funding rate
    funding_rate = await client.get_funding_rate(symbol)
    await asyncio.sleep(REQUEST_DELAY)

    # estado CFI
    cfi_state = _cfi_states.get(symbol, "E1")

    # determinar direcções candidatas
    directions_to_test = []
    if oi_cascade.get("cascade"):
        directions_to_test.append(oi_cascade["direction"])
    if sr_zone and sr_zone["direction"] not in directions_to_test:
        directions_to_test.append(sr_zone["direction"])
    if compression and compression.get("compressed"):
        for d in ("LONG", "SHORT"):
            if d not in directions_to_test:
                directions_to_test.append(d)
    if not directions_to_test:
        if rsi_1h and rsi_1h < RSI_LONG_MAX:
            directions_to_test.append("LONG")
        elif rsi_1h and rsi_1h > RSI_SHORT_MIN:
            directions_to_test.append("SHORT")
    if not directions_to_test:
        return None

    # scoring — melhor direcção
    best_result = None
    best_score  = 0

    for direction in directions_to_test:
        sr_for_dir = sr_zone if sr_zone and sr_zone["direction"] == direction else None
        result = calcular_score(
            direction=direction,
            sr_zone=sr_for_dir,
            rsi_1h=rsi_1h,
            rsi_5m=None,
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
                "atr_1h":       atr_1h,
                "oi_usd":       oi_now,
            }

    # devolve sempre o melhor resultado (filtro de threshold feito no ciclo principal)
    return best_result


# ── Ciclo principal ───────────────────────────────────────────────────────────

async def run_scanner():
    global _cfi_states

    logger.info("=" * 50)
    logger.info("CSA v1.0 — Crypto Scalp Alerts — A arrancar")
    logger.info("=" * 50)

    _cfi_states = _load_cfi_states()
    logger.info(f"Estados CFI carregados: {len(_cfi_states)} tokens")

    connector = aiohttp.TCPConnector(limit=20, limit_per_host=10)

    async with aiohttp.ClientSession(connector=connector) as session:
        client = MexcClient(session)

        # Universo real, calculado agora — antes disto (07/07/2026) a
        # mensagem dizia sempre "~500 tokens MEXC", texto fixo que nunca
        # reflectiu o filtro de facto (ver bug corrigido em
        # _passes_liquidity_filter — volume24 já vem em USD da MEXC).
        tickers_iniciais = await client.get_all_tickers()
        n_elegiveis = sum(
            1 for t in (tickers_iniciais or [])
            if _passes_liquidity_filter(t)
            and t.get("symbol", "") not in SYMBOL_BLACKLIST
            and not t.get("symbol", "").endswith("STOCK_USDT")
        )

        await enviar_status_scalp(
            session,
            f"CSA v1.0 activo\n"
            f"Scan interval: {SCAN_INTERVAL_SEC}s\n"
            f"Threshold: score >= 6/10\n"
            f"Universo: {n_elegiveis} tokens elegíveis (de {len(tickers_iniciais or [])} na MEXC)"
        )

        # arrancar monitor de alertas em paralelo
        asyncio.create_task(monitor.run(session))
        logger.info("Monitor de alertas activo")

        cycle = 0
        while True:
            cycle += 1
            t_start = time.time()
            now_str = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
            logger.info(f"── Ciclo #{cycle} | {now_str} ──────────────────────")

            if cycle % 10 == 0:
                _cfi_states = _load_cfi_states()
                logger.info(f"Estados CFI actualizados: {len(_cfi_states)} tokens")

            # ── Tickers ───────────────────────────────────────────────────────
            tickers = await client.get_all_tickers()
            if not tickers:
                logger.warning("Sem tickers — aguardar 30s")
                await asyncio.sleep(30)
                continue

            # ── Filtro liquidez + blacklist ───────────────────────────────────
            elegíveis = [
                t for t in tickers
                if _passes_liquidity_filter(t)
                and t.get("symbol", "") not in SYMBOL_BLACKLIST
                and not t.get("symbol", "").endswith("STOCK_USDT")
            ]
            logger.info(f"Elegíveis: {len(elegíveis)}/{len(tickers)}")

            # ── Priorização ───────────────────────────────────────────────────
            def priority_key(t):
                sym   = t.get("symbol", "")
                state = _cfi_states.get(sym, "E1")
                prio  = {"E3": 3, "E2": 2, "E1": 1}.get(state, 0)
                try:
                    vol = float(t.get("volume24", 0)) * float(t.get("lastPrice", 0))
                except Exception:
                    vol = 0
                return (prio, vol)

            elegíveis.sort(key=priority_key, reverse=True)

            # ── FASE 1 — Pre-filtro ───────────────────────────────────────────
            candidatos = []
            for ticker in elegíveis:
                symbol = ticker.get("symbol", "")
                if not symbol:
                    continue
                if time.time() - _alert_cooldown.get(symbol, 0) < ALERT_COOLDOWN_SECS:
                    continue
                # não gerar novo alerta se este token já tem um sinal "aberto"
                # (ainda não atingiu TP, SL, nem expirou as 2h)
                if await monitor.esta_activo(symbol):
                    continue
                try:
                    base = await _prefilter_token(client, symbol, ticker)
                    if base:
                        candidatos.append((symbol, ticker, base))
                except Exception as e:
                    logger.error(f"Fase1 erro {symbol}: {e}")

            t_fase1 = time.time() - t_start
            logger.info(f"Fase 1 concluída: {len(candidatos)} candidatos | {t_fase1:.1f}s")

            # ── FASE 2 — Análise completa ─────────────────────────────────────
            alertas_enviados = 0
            _debug_scores = []  # acumula scores para log de diagnóstico

            for symbol, ticker, base in candidatos:
                try:
                    result = await _analyze_candidate(client, symbol, base)
                except Exception as e:
                    logger.error(f"Fase2 erro {symbol}: {e}")
                    continue

                if result is None:
                    continue

                score     = result["scoring"]["score"]
                direction = result["direction"]
                setup     = result["scoring"]["setup_type"]

                # acumular para diagnóstico (sempre, independente do threshold)
                _debug_scores.append((symbol, score, direction, setup,
                                      result["scoring"]["components"]))

                # filtro de threshold de registo (>=6)
                if not result["scoring"]["send"]:
                    continue

                setup_type = setup
                priority   = result["scoring"]["priority"]  # score >= 8

                logger.info(
                    f"🎯 {symbol} {direction} | Score: {score}/10"
                    f"{' 🔥' if priority else ''}"
                    f" | Setup {setup or '?'}"
                )

                # notificação Telegram: score >= 8 (priority) E dentro da janela activa
                # E com setup A/B/C definido — sem setup dominante, regista-se
                # sempre (para estudo, ver análise 12/07/2026) mas nunca conta
                # como accionável nem gera Telegram, porque testámos e os N/A
                # explícitos (mesma condição, período anterior) tiveram o pior
                # PnL médio de todos os grupos (-0.152%, n=438).
                # Janela activa UTC: 06h-07h + 10h-11h + 13h-14h
                # Fora da janela: regista Notion e monitor mas não notifica
                # Registo Notion é SEMPRE feito independentemente da hora ou do setup
                _hora_utc = datetime.now(timezone.utc).hour
                HORAS_ACTIVAS = {6, 7, 10, 11, 13, 14}
                _hora_morta = _hora_utc not in HORAS_ACTIVAS
                _tem_setup = setup_type is not None
                _enviar_telegram = priority and not _hora_morta and _tem_setup

                if _enviar_telegram:
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
                        atr_1h=result.get("atr_1h"),
                    )
                    if sent:
                        alertas_enviados += 1
                else:
                    sent = False
                    if not _tem_setup:
                        motivo = "sem setup A/B/C dominante — registo para estudo, não executável"
                    elif _hora_morta:
                        motivo = f"hora {_hora_utc}h UTC fora da janela activa"
                    else:
                        motivo = f"Score {score}/10"
                    logger.info(f"  ↳ {motivo} — registo Notion sem notificação Telegram")

                # regista Notion e monitor para todos os scores >= 6
                _alert_cooldown[symbol] = time.time()

                # calcular TP1 e SL com ATR para o monitor
                _price = result["price"]
                _dir   = result["direction"]
                _atr   = result.get("atr_1h")
                if _atr and _atr > 0:
                    if _dir == "LONG":
                        _tp1 = _price + 0.4 * _atr   # TP1: 0.4×ATR
                        _tp2 = _price + 1.0 * _atr   # TP2: 1.0×ATR
                        _sl  = _price - 1.5 * _atr
                    else:
                        _tp1 = _price - 0.4 * _atr
                        _tp2 = _price - 1.0 * _atr
                        _sl  = _price + 1.5 * _atr
                else:
                    if _dir == "LONG":
                        _tp1 = _price * 1.010
                        _tp2 = _price * 1.025
                        _sl  = _price * 0.985
                    else:
                        _tp1 = _price * 0.990
                        _tp2 = _price * 0.975
                        _sl  = _price * 1.015

                notion_page_id = await log_alerta_csa(
                    session=session,
                    symbol=symbol,
                    direction=direction,
                    price=result["price"],
                    score=score,
                    setup_type=setup_type,
                    sr_zone=result["sr_zone"],
                    rsi_1h=result["rsi_1h"],
                    funding_rate=result["funding_rate"],
                    cfi_state=result["cfi_state"],
                    priority=priority,
                    enviado=_enviar_telegram,
                    executavel=_tem_setup,
                    atr_1h=_atr,
                    tp1=_tp1,
                    tp2=_tp2,
                    sl=_sl,
                    componentes=" | ".join(
                        f"{k}={v['points']}/{v['max']}"
                        for k, v in result["scoring"]["components"].items()
                    ),
                    # gravados desde 12/07/2026 — para, com dados novos,
                    # poder testar se afinar MIN_VOLUME_24H/MAX_SPREAD_PCT/
                    # MIN_OI melhora o resultado (hoje são só thresholds
                    # binários de corte, sem o valor real registado)
                    volume_24h=float(ticker.get("volume24", 0)),
                    spread_pct=_calcular_spread(ticker),
                    oi_usd=result.get("oi_usd"),
                )

                await monitor.registar(
                    symbol=symbol,
                    direction=_dir,
                    price_entry=_price,
                    tp1=_tp1,
                    tp2=_tp2,
                    sl=_sl,
                    score=score,
                    setup_type=setup_type,
                    notion_page_id=notion_page_id,
                )

                if alertas_enviados >= 3:
                    logger.info("Limite de 3 alertas por ciclo atingido")
                    break

            # ── Log de diagnóstico — top 5 scores do ciclo ───────────────────
            if _debug_scores:
                _debug_scores.sort(key=lambda x: x[1], reverse=True)
                logger.info(f"── Top scores ciclo #{cycle} ──")
                for sym, sc, dr, st, comps in _debug_scores[:5]:
                    pts = " | ".join(
                        f"{k}={v['points']}/{v['max']}"
                        for k, v in comps.items()
                        if v["points"] > 0
                    )
                    logger.info(f"  {sym} {dr} score={sc}/10 setup={st or '?'} [{pts or 'sem pontos'}]")
            else:
                logger.info(f"── Ciclo #{cycle}: nenhum resultado na Fase 2 ──")

            # ── Sumário ───────────────────────────────────────────────────────
            elapsed = time.time() - t_start
            logger.info(
                f"Ciclo #{cycle} concluído | "
                f"{len(candidatos)} candidatos | "
                f"{alertas_enviados} alertas | "
                f"{elapsed:.1f}s"
            )

            wait = max(0, SCAN_INTERVAL_SEC - elapsed)
            if wait > 0:
                logger.info(f"Próximo ciclo em {wait:.0f}s")
                await asyncio.sleep(wait)


# ── Entrypoint ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    asyncio.run(run_scanner())
