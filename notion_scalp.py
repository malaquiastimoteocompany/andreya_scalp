"""
notion_scalp.py — Logging Notion para o CSA v1.0

Bases:
  - Alertas CSA: registo de cada alerta enviado
  - Trades Scalp: registo de trades executados (entrada manual via comando bot)
"""

import logging
import aiohttp
from datetime import datetime, timezone
from typing import Optional

from config import NOTION_TOKEN, NOTION_DB_ALERTAS_CSA, NOTION_DB_TRADES_SCALP
from github_sync import registar_alerta as _registar_alerta_github

logger = logging.getLogger(__name__)

NOTION_API = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"


def _headers() -> dict:
    return {
        "Authorization":  f"Bearer {NOTION_TOKEN}",
        "Content-Type":   "application/json",
        "Notion-Version": NOTION_VERSION,
    }


async def log_alerta_csa(
    session: aiohttp.ClientSession,
    symbol: str,
    direction: str,
    price: float,
    score: int,
    setup_type: Optional[str],
    sr_zone: Optional[dict],
    rsi_1h: Optional[float],
    funding_rate: Optional[float],
    cfi_state: Optional[str],
    priority: bool,
    enviado: bool = False,     # True se notificação Telegram foi enviada
    executavel: bool = True,   # False quando não há Setup A/B/C — registado
                                # só para estudo, nunca gera Telegram nem conta
                                # como trade accionável (ver scanner_scalp.py)
) -> Optional[str]:  # devolve page_id ou None
    """
    Regista alerta CSA na base 'Alertas CSA' do Notion.
    O campo Enviado indica se gerou notificação Telegram (score>=8 + hora válida).
    O campo Executável indica se o alerta tinha um setup A/B/C dominante —
    quando False, fica registado para estudo mas nunca chega a ser enviado.
    """
    if not NOTION_DB_ALERTAS_CSA:
        logger.debug("NOTION_DB_ALERTAS_CSA não configurado — skip log")
        return False

    now_iso = datetime.now(timezone.utc).isoformat()
    ticker  = symbol.replace("_USDT", "")

    props = {
        "Token": {
            "title": [{"text": {"content": ticker}}]
        },
        "Direcção": {
            "select": {"name": direction}
        },
        "Setup": {
            "select": {"name": setup_type or "N/A"}
        },
        "Score": {
            "number": score
        },
        "Preço Alerta": {
            "number": price
        },
        "Prioritário": {
            "checkbox": priority
        },
        "Enviado": {
            "checkbox": enviado      # novo campo — foi notificado via Telegram?
        },
        "Executável": {
            "checkbox": executavel   # False = sem setup A/B/C, só para estudo
        },
        "Data Alerta": {
            "date": {"start": now_iso}
        },
    }

    if rsi_1h is not None:
        props["RSI 1h"] = {"number": rsi_1h}

    if funding_rate is not None:
        props["Funding Rate"] = {"number": round(funding_rate * 100, 6)}

    if cfi_state:
        props["Estado CFI"] = {"select": {"name": cfi_state}}

    if sr_zone:
        props["Zona S/R"] = {"number": sr_zone["price"]}
        props["Toques S/R"] = {"number": sr_zone["touches"]}

    # campos de resultado — preenchidos pelo monitor_alertas.py
    props["Resultado"] = {"select": {"name": "Pendente"}}

    payload = {
        "parent": {"database_id": NOTION_DB_ALERTAS_CSA},
        "properties": props,
    }

    try:
        async with session.post(
            f"{NOTION_API}/pages",
            json=payload,
            headers=_headers(),
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            if r.status in (200, 201):
                data = await r.json()
                page_id = data.get("id")
                # Espelho em JSON no GitHub — nunca bloqueia nem derruba isto se falhar.
                await _registar_alerta_github(session, page_id, {
                    "token":          ticker,
                    "direccao":       direction,
                    "preco_alerta":   price,
                    "score":          score,
                    "setup":          setup_type or "N/A",
                    "prioritario":    priority,
                    "enviado":        enviado,
                    "executavel":     executavel,
                    "data_alerta":    now_iso,
                    "rsi_1h":         rsi_1h,
                    "funding_rate":   round(funding_rate * 100, 6) if funding_rate is not None else None,
                    "estado_cfi":     cfi_state,
                    "zona_sr":        sr_zone["price"] if sr_zone else None,
                    "toques_sr":      sr_zone["touches"] if sr_zone else None,
                    "resultado":      "Pendente",
                })
                return page_id
            text = await r.text()
            logger.error(f"Notion alertas CSA erro {r.status}: {text[:200]}")
            return None
    except Exception as e:
        logger.error(f"Notion alertas CSA excepção: {e}")
        return None


async def log_trade_scalp(
    session: aiohttp.ClientSession,
    symbol: str,
    direction: str,
    entry_price: float,
    sl_price: float,
    tp1_price: float,
    score_alerta: int,
    sizing_pct: float,
    leverage: int,
    notas: str = "",
) -> bool:
    """
    Regista trade scalp executado na base 'Trades Scalp'.
    Chamado manualmente via comando bot /scalp_trade.
    """
    if not NOTION_DB_TRADES_SCALP:
        logger.debug("NOTION_DB_TRADES_SCALP não configurado — skip log")
        return False

    now_iso = datetime.now(timezone.utc).isoformat()
    ticker  = symbol.replace("_USDT", "")

    props = {
        "Token": {
            "title": [{"text": {"content": ticker}}]
        },
        "Direcção": {
            "select": {"name": direction}
        },
        "Entry": {
            "number": entry_price
        },
        "SL": {
            "number": sl_price
        },
        "TP1": {
            "number": tp1_price
        },
        "Score Alerta": {
            "number": score_alerta
        },
        "Sizing (%)": {
            "number": sizing_pct
        },
        "Leverage": {
            "number": leverage
        },
        "Estado": {
            "select": {"name": "Aberto"}
        },
        "Data Entrada": {
            "date": {"start": now_iso}
        },
    }

    if notas:
        props["Notas"] = {
            "rich_text": [{"text": {"content": notas}}]
        }

    payload = {
        "parent": {"database_id": NOTION_DB_TRADES_SCALP},
        "properties": props,
    }

    try:
        async with session.post(
            f"{NOTION_API}/pages",
            json=payload,
            headers=_headers(),
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            if r.status in (200, 201):
                return True
            text = await r.text()
            logger.error(f"Notion trades scalp erro {r.status}: {text[:200]}")
            return False
    except Exception as e:
        logger.error(f"Notion trades scalp excepção: {e}")
        return False


async def criar_bases_notion(session: aiohttp.ClientSession, page_raiz: str) -> dict:
    """
    Cria as bases de dados Notion para o CSA se não existirem.
    Chamado uma vez no setup inicial.
    """
    bases = {}

    # ── Base Alertas CSA ──────────────────────────────────────────────────────
    alertas_schema = {
        "parent": {"page_id": page_raiz},
        "title": [{"text": {"content": "📡 Alertas CSA"}}],
        "properties": {
            "Token":         {"title": {}},
            "Direcção":      {"select": {"options": [{"name": "LONG", "color": "green"}, {"name": "SHORT", "color": "red"}]}},
            "Setup":         {"select": {"options": [{"name": "A"}, {"name": "B"}, {"name": "C"}, {"name": "N/A"}]}},
            "Score":         {"number": {"format": "number"}},
            "Preço Alerta":  {"number": {"format": "number"}},
            "Prioritário":   {"checkbox": {}},
            "Enviado":       {"checkbox": {}},
            "RSI 1h":        {"number": {"format": "number"}},
            "Funding Rate":  {"number": {"format": "percent"}},
            "Estado CFI":    {"select": {"options": [{"name": "E1"}, {"name": "E2"}, {"name": "E3"}]}},
            "Zona S/R":      {"number": {"format": "number"}},
            "Toques S/R":    {"number": {"format": "number"}},
            "Data Alerta":   {"date": {}},
            "Resultado":     {"select": {"options": [
                {"name": "Pendente",   "color": "gray"},
                {"name": "TP Hit",     "color": "green"},
                {"name": "TP2 Hit",    "color": "green"},
                {"name": "SL Hit",     "color": "red"},
                {"name": "TP Tardio",  "color": "blue"},
                {"name": "TP2 Tardio", "color": "blue"},
                {"name": "SL Tardio",  "color": "orange"},
                {"name": "Falhado",    "color": "brown"},
            ]}},
            "Preco Saida":   {"number": {"format": "number"}},
            "PnL Alerta (%)": {"number": {"format": "number"}},
            "Duracao (min)": {"number": {"format": "number"}},
            "Data Resultado": {"date": {}},
        }
    }

    # ── Base Trades Scalp ─────────────────────────────────────────────────────
    trades_schema = {
        "parent": {"page_id": page_raiz},
        "title": [{"text": {"content": "💹 Trades Scalp"}}],
        "properties": {
            "Token":        {"title": {}},
            "Direcção":     {"select": {"options": [{"name": "LONG", "color": "green"}, {"name": "SHORT", "color": "red"}]}},
            "Entry":        {"number": {"format": "number"}},
            "SL":           {"number": {"format": "number"}},
            "TP1":          {"number": {"format": "number"}},
            "Exit":         {"number": {"format": "number"}},
            "PnL (%)":      {"number": {"format": "percent"}},
            "Score Alerta": {"number": {"format": "number"}},
            "Sizing (%)":   {"number": {"format": "percent"}},
            "Leverage":     {"number": {"format": "number"}},
            "Estado":       {"select": {"options": [
                {"name": "Aberto", "color": "yellow"},
                {"name": "TP Hit", "color": "green"},
                {"name": "SL Hit", "color": "red"},
                {"name": "Manual Close", "color": "gray"},
            ]}},
            "Data Entrada": {"date": {}},
            "Data Saída":   {"date": {}},
            "Notas":        {"rich_text": {}},
        }
    }

    for nome, schema in [("alertas", alertas_schema), ("trades", trades_schema)]:
        try:
            async with session.post(
                f"{NOTION_API}/databases",
                json=schema,
                headers=_headers(),
                timeout=aiohttp.ClientTimeout(total=15),
            ) as r:
                if r.status in (200, 201):
                    data = await r.json()
                    db_id = data["id"].replace("-", "")
                    bases[nome] = db_id
                    logger.info(f"Notion base '{nome}' criada: {db_id}")
                else:
                    text = await r.text()
                    logger.error(f"Notion criar base '{nome}' erro {r.status}: {text[:200]}")
        except Exception as e:
            logger.error(f"Notion criar base '{nome}' excepção: {e}")

    return bases
