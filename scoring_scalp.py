"""
scoring_scalp.py — Motor de scoring CSA v1.0

Score 0-10 por componente:
  +2 Zona S/R estrutural clara (>= 2 toques 1h)
  +1 RSI em zona extrema no 1h
  +2 Concentração de liquidez na zona ±2% (proxy heatmap via depth_ratio)
  +1 Bid/ask wall no order book na zona
  +1 CVD mostrando absorção ou divergência
  +1 Volume spike ou compressão confirmada
  +1 Liquidações recentes no lado oposto (cascade detector)
  +1 Token em E2 ou E3 no CFI v2.0 (bónus)
"""

import logging
from typing import Optional

from config import (
    RSI_LONG_MAX, RSI_SHORT_MIN,
    RSI_5M_LONG_MAX, RSI_5M_SHORT_MIN,
    SCORE_MIN_ENVIO,
)

logger = logging.getLogger(__name__)


def calcular_score(
    direction: str,           # "LONG" ou "SHORT"
    sr_zone: Optional[dict],  # resultado de nearest_sr_zone
    rsi_1h: Optional[float],
    rsi_5m: Optional[float],
    walls: dict,              # resultado de find_walls (inclui depth_ratio)
    cvd: dict,                # resultado de calc_cvd
    vol_stats: dict,          # resultado de calc_volume_stats
    oi_cascade: dict,         # resultado de detect_oi_cascade
    cfi_state: Optional[str] = None,  # "E1", "E2", "E3" ou None
    compression: Optional[dict] = None,  # resultado de check_volatility_compression
) -> dict:
    """
    Calcula score de confluência e devolve resultado detalhado.

    Returns:
        {
          score: int,
          components: dict,      # cada componente com valor e pontos
          setup_type: str,       # "A", "B", "C" ou None
          send: bool,
          priority: bool,        # score >= 8
        }
    """
    score = 0
    components = {}

    # ── Componente 1: Zona S/R estrutural (+2) ────────────────────────────────
    has_sr = sr_zone is not None and sr_zone.get("touches", 0) >= 2
    sr_pts = 2 if has_sr else 0
    score += sr_pts
    components["sr_zone"] = {
        "label":  "Zona S/R estrutural clara",
        "points": sr_pts,
        "max":    2,
        "active": has_sr,
        "detail": f"Zona ${sr_zone['price']:,.4f} ({sr_zone['touches']} toques)" if has_sr else "Sem zona identificada",
    }

    # ── Componente 2: RSI extremo 1h (+1) ────────────────────────────────────
    rsi_ok = False
    rsi_detail = f"RSI 1h: {rsi_1h}" if rsi_1h else "RSI 1h indisponível"
    if rsi_1h is not None:
        if direction == "LONG" and rsi_1h < RSI_LONG_MAX:
            rsi_ok = True
            rsi_detail = f"RSI 1h: {rsi_1h} (sobrevenda)"
        elif direction == "SHORT" and rsi_1h > RSI_SHORT_MIN:
            rsi_ok = True
            rsi_detail = f"RSI 1h: {rsi_1h} (sobrecompra)"
    rsi_pts = 1 if rsi_ok else 0
    score += rsi_pts
    components["rsi_1h"] = {
        "label":  "RSI em zona extrema (1h)",
        "points": rsi_pts,
        "max":    1,
        "active": rsi_ok,
        "detail": rsi_detail,
    }

    # ── Componente 3: Concentração de liquidez na zona (+2) ───────────────────
    # Proxy de heatmap: mede a concentração de ordens numa banda ±2% do preço
    # vs o que seria esperado se o book fosse uniforme.
    # depth_ratio >= 2.0 → cluster moderado (+1)
    # depth_ratio >= 4.0 → cluster denso (+2, equivalente a heatmap brilhante)
    # Independente do Comp4 (wall): um token pode ter liquidez concentrada
    # sem ter uma wall pontual identificável, e vice-versa.
    cluster_pts = 0
    cluster_ok = False
    cluster_detail = "Sem concentração de liquidez na zona"

    depth_ratio = walls.get("depth_ratio")
    if depth_ratio is not None:
        if depth_ratio >= 4.0:
            cluster_pts = 2
            cluster_ok = True
            cluster_detail = f"Cluster denso — liquidez {depth_ratio:.1f}× concentrada na zona ±2%"
        elif depth_ratio >= 2.0:
            cluster_pts = 1
            cluster_ok = True
            cluster_detail = f"Cluster moderado — liquidez {depth_ratio:.1f}× concentrada na zona ±2%"

    score += cluster_pts
    components["cluster"] = {
        "label":  "Concentração de liquidez na zona (proxy heatmap)",
        "points": cluster_pts,
        "max":    2,
        "active": cluster_ok,
        "detail": cluster_detail,
    }

    # ── Componente 4: Bid/ask wall no order book (+1) ─────────────────────────
    wall_ok = False
    wall_detail = "Sem wall no order book"
    if direction == "LONG" and walls.get("has_bid_wall"):
        wall_ok = True
        wall_detail = f"Bid wall: ${walls['bid_wall']['usd_value']:,.0f} em ${walls['bid_wall']['price']:,.4f}"
    elif direction == "SHORT" and walls.get("has_ask_wall"):
        wall_ok = True
        wall_detail = f"Ask wall: ${walls['ask_wall']['usd_value']:,.0f} em ${walls['ask_wall']['price']:,.4f}"
    wall_pts = 1 if wall_ok else 0
    score += wall_pts
    components["wall"] = {
        "label":  "Bid/Ask wall no order book",
        "points": wall_pts,
        "max":    1,
        "active": wall_ok,
        "detail": wall_detail,
    }

    # ── Componente 5: CVD absorção ou divergência (+1) ────────────────────────
    cvd_ok = cvd.get("absorbing", False) or cvd.get("cvd_trend") == "divergência"
    cvd_pts = 1 if cvd_ok else 0
    score += cvd_pts
    components["cvd"] = {
        "label":  "CVD — absorção ou divergência",
        "points": cvd_pts,
        "max":    1,
        "active": cvd_ok,
        "detail": f"CVD: {cvd.get('cvd_trend', 'neutro')} | Buy {cvd.get('buy_pct', 0)*100:.0f}% / Sell {cvd.get('sell_pct', 0)*100:.0f}%",
    }

    # ── Componente 6: Volume spike ou compressão (+1) ─────────────────────────
    vol_ok = vol_stats.get("is_spike", False) or vol_stats.get("is_dry", False) or \
             (compression is not None and compression.get("compressed", False))
    vol_detail = "Volume neutro"
    if vol_stats.get("is_spike"):
        vol_detail = f"Volume spike {vol_stats['vol_ratio']:.1f}x da média"
    elif compression and compression.get("compressed"):
        vol_detail = f"Compressão — ATR {compression['atr_ratio']:.2f}x semanal | range {compression['range_pct']:.2f}%"
    elif vol_stats.get("is_dry"):
        vol_detail = "Volume a secar (compressão)"
    vol_pts = 1 if vol_ok else 0
    score += vol_pts
    components["volume"] = {
        "label":  "Volume spike ou compressão",
        "points": vol_pts,
        "max":    1,
        "active": vol_ok,
        "detail": vol_detail,
    }

    # ── Componente 7: Liquidações recentes lado oposto (+1) ───────────────────
    liq_ok = False
    liq_detail = "Sem liquidações relevantes recentes"
    if oi_cascade.get("cascade"):
        if direction == "LONG" and oi_cascade.get("direction") == "LONG":
            liq_ok = True
            liq_detail = f"Cascade longs: OI -{oi_cascade['oi_drop_pct']:.1f}% (bounce long esperado)"
        elif direction == "SHORT" and oi_cascade.get("direction") == "SHORT":
            liq_ok = True
            liq_detail = f"Cascade shorts: OI +{oi_cascade['oi_drop_pct']:.1f}% (bounce short esperado)"
    liq_pts = 1 if liq_ok else 0
    score += liq_pts
    components["liquidacoes"] = {
        "label":  "Liquidações recentes (lado oposto)",
        "points": liq_pts,
        "max":    1,
        "active": liq_ok,
        "detail": liq_detail,
    }

    # ── Componente 8: Bónus CFI (+1) ──────────────────────────────────────────
    cfi_ok = cfi_state in ("E2", "E3")
    cfi_pts = 1 if cfi_ok else 0
    score += cfi_pts
    components["cfi"] = {
        "label":  "Token em E2/E3 no CFI v2.0",
        "points": cfi_pts,
        "max":    1,
        "active": cfi_ok,
        "detail": f"CFI: {cfi_state}" if cfi_state else "CFI: E1 (sem confluência adicional)",
    }

    # ── Determinar tipo de setup dominante ────────────────────────────────────
    setup_type = _determine_setup(
        has_sr, rsi_ok, oi_cascade, vol_stats, compression
    )

    # ── Resultado ─────────────────────────────────────────────────────────────
    # send é só sobre o threshold de score — o filtro por setup passou a viver
    # em scanner_scalp.py (_tem_setup), que continua a registar tudo mas só
    # marca como executável e só alerta quando há setup A/B/C definido.
    # (12/07/2026 — revertido do bloqueio total de 11/07, para não perder os
    # dados dos casos sem setup, só deixar de os tratar como accionáveis.)
    send     = score >= SCORE_MIN_ENVIO
    priority = score >= 8

    return {
        "score":      score,
        "score_max":  10,
        "components": components,
        "setup_type": setup_type,
        "send":       send,
        "priority":   priority,
    }


def _determine_setup(
    has_sr: bool,
    rsi_ok: bool,
    oi_cascade: dict,
    vol_stats: dict,
    compression: Optional[dict],
) -> Optional[str]:
    """Determina o tipo de setup dominante (A, B ou C)."""
    # Setup B tem prioridade se há cascade recente
    if oi_cascade.get("cascade"):
        return "B"
    # Setup C se há compressão
    if compression and compression.get("compressed"):
        return "C"
    # Setup A se há zona S/R
    if has_sr:
        return "A"
    return None


SETUP_NAMES = {
    "A": "Bounce em Zona Estrutural",
    "B": "Squeeze de Liquidez",
    "C": "Compressão de Volatilidade",
}

SETUP_RR = {
    "A": "1.5:1",
    "B": "2:1",
    "C": "2.5:1",
}

SETUP_TP = {
    "A": "1–3%",
    "B": "1–2%",
    "C": "2–5%",
}
