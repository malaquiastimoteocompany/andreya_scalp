"""
notificacoes_scalp.py — Alertas Telegram para o canal Scalp (CSA v1.0)

Formato distinto dos alertas CFI para evitar confusão.
Envia para TELEGRAM_CHAT_SCALP.

SL/TP calculados com base no ATR 1h:
  SL  = 1.5× ATR (além do ruído normal)
  TP1 = 0.75× ATR (rápido, alta probabilidade)
  TP2 = 1.5× ATR (segundo objectivo, R/R 1:1)
Fallback para percentagens fixas se ATR não disponível.
"""

import logging
import aiohttp
from typing import Optional

from config import TELEGRAM_TOKEN, TELEGRAM_CHAT_SCALP
from scoring_scalp import SETUP_NAMES, SETUP_RR, SETUP_TP

logger = logging.getLogger(__name__)

TG_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"


async def enviar_alerta_scalp(
    session: aiohttp.ClientSession,
    symbol: str,
    direction: str,
    price: float,
    scoring: dict,
    sr_zone: Optional[dict],
    rsi_1h: Optional[float],
    funding_rate: Optional[float],
    cfi_state: Optional[str],
    atr_1h: Optional[float] = None,
) -> bool:
    """
    Formata e envia alerta de scalp para o canal Telegram.
    Retorna True se enviado com sucesso.
    """
    score      = scoring["score"]
    priority   = scoring["priority"]
    setup_type = scoring.get("setup_type")
    components = scoring["components"]

    # ── Cabeçalho ─────────────────────────────────────────────────────────────
    icon   = "🔥" if priority else "⚡"
    titulo = "SCALP PRIORITÁRIO" if priority else "SCALP ALERT"
    ticker = symbol.replace("_USDT", "")

    linhas = [
        f"{icon} {titulo} — {ticker}_USDT {direction}  |  Score: {score}/10",
        "",
    ]

    # ── Setup ─────────────────────────────────────────────────────────────────
    if setup_type:
        nome_setup = SETUP_NAMES.get(setup_type, setup_type)
        linhas.append(f"Setup: {nome_setup} (Setup {setup_type})")

    # ── Zona S/R ──────────────────────────────────────────────────────────────
    if sr_zone:
        tipo_zona = "Suporte" if sr_zone["type"] == "support" else "Resistência"
        linhas.append(
            f"Zona: ${sr_zone['price']:,.4f} ({tipo_zona} testado {sr_zone['touches']}x)"
        )

    linhas.append(f"Preço actual: ${price:,.4f}")
    if atr_1h:
        linhas.append(f"ATR 1h: ${atr_1h:,.4f}")
    linhas.append("")

    # ── Confluência ───────────────────────────────────────────────────────────
    linhas.append("Confluência:")
    for key, comp in components.items():
        tick = "✅" if comp["active"] else "❌"
        linhas.append(f"  {tick} {comp['detail']}")

    linhas.append("")

    # ── Níveis — calculados com ATR ───────────────────────────────────────────
    if sr_zone:
        entry_low  = sr_zone["price"] * 0.997
        entry_high = sr_zone["price"] * 1.003
        linhas.append(f"Entry sugerido: zona ${entry_low:,.4f} — ${entry_high:,.4f}")
    else:
        linhas.append(f"Entry sugerido: ${price:,.4f} (preço actual)")

    # SL e TP baseados no ATR 1h
    # SL  = 1.5× ATR — além do ruído normal do token
    # TP1 = 0.75× ATR — rápido, alta probabilidade de atingir em scalp
    # TP2 = 1.5× ATR — segundo objectivo, R/R 1:1
    # Fallback: percentagens fixas por setup se ATR não disponível
    if atr_1h and atr_1h > 0:
        sl_dist  = 1.5  * atr_1h
        tp1_dist = 0.75 * atr_1h
        tp2_dist = 1.5  * atr_1h
        sl_pct   = sl_dist  / price
        tp1_pct  = tp1_dist / price
        tp2_pct  = tp2_dist / price
        nivel_label = f"(1.5× ATR)"
    else:
        # fallback sem ATR — percentagens fixas por setup
        if setup_type == "A":
            sl_pct, tp1_pct, tp2_pct = 0.015, 0.010, 0.025
        elif setup_type == "B":
            sl_pct, tp1_pct, tp2_pct = 0.010, 0.008, 0.020
        else:  # C ou None
            sl_pct, tp1_pct, tp2_pct = 0.020, 0.015, 0.040
        nivel_label = "(% fixo — ATR indisponível)"

    if direction == "LONG":
        sl  = price * (1 - sl_pct)
        tp1 = price * (1 + tp1_pct)
        tp2 = price * (1 + tp2_pct)
    else:
        sl  = price * (1 + sl_pct)
        tp1 = price * (1 - tp1_pct)
        tp2 = price * (1 - tp2_pct)

    rr = tp1_pct / sl_pct if sl_pct > 0 else 0

    linhas.append(
        f"SL sugerido: {'>' if direction == 'SHORT' else '<'} ${sl:,.4f} {nivel_label}"
    )
    linhas.append(
        f"TP1: ${tp1:,.4f} (+{tp1_pct*100:.2f}%)  |  TP2: ${tp2:,.4f} (+{tp2_pct*100:.2f}%)"
    )
    linhas.append(f"R/R estimado: {rr:.2f}:1")
    linhas.append("")

    # ── Aviso ─────────────────────────────────────────────────────────────────
    linhas.append("⚠️ Decisão de entrada é TUA — valida antes de entrar")

    # ── Contexto CFI ──────────────────────────────────────────────────────────
    cfi_txt = cfi_state if cfi_state else "E1"
    if cfi_state in ("E2", "E3"):
        linhas.append(f"CFI: {ticker} em {cfi_txt} (confluência adicional ✅)")
    else:
        linhas.append(f"CFI: {ticker} em {cfi_txt} (sem confluência adicional)")

    # ── Funding rate ──────────────────────────────────────────────────────────
    if funding_rate is not None:
        fr_pct = funding_rate * 100
        fr_txt = f"{fr_pct:+.4f}%"
        if abs(funding_rate) > 0.0003:
            fr_txt += " ⚠️"
        linhas.append(f"Funding: {fr_txt}")

    texto = "\n".join(linhas)

    return await _enviar_mensagem(session, texto)


async def enviar_status_scalp(
    session: aiohttp.ClientSession,
    texto: str,
) -> bool:
    """Envia mensagem de status/erro para o canal Scalp."""
    return await _enviar_mensagem(session, f"ℹ️ CSA Status\n\n{texto}")


async def _enviar_mensagem(
    session: aiohttp.ClientSession,
    texto: str,
    chat_id: int = TELEGRAM_CHAT_SCALP,
    parse_mode: str = None,
) -> bool:
    """Envia mensagem Telegram. Sem parse_mode para evitar erros com caracteres especiais."""
    url = f"{TG_API}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text":    texto,
    }
    if parse_mode:
        payload["parse_mode"] = parse_mode

    try:
        async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as r:
            data = await r.json()
            if not data.get("ok"):
                logger.error(f"Telegram erro: {data.get('description')}")
                return False
            return True
    except Exception as e:
        logger.error(f"Telegram excepção: {e}")
        return False


async def enviar_mensagem_raw(
    session: aiohttp.ClientSession,
    texto: str,
) -> bool:
    """Exposto para uso externo (ex: comandos bot)."""
    return await _enviar_mensagem(session, texto)

