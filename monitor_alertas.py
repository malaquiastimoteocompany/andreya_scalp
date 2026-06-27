"""
monitor_alertas.py — Monitorização automática de resultados CSA v1.0

Após cada alerta enviado, monitoriza o preço e regista o resultado no Notion:
  - TP1 ou SL atingido em <= 30 min → "TP Hit" / "SL Hit"
  - TP1 ou SL atingido entre 30 min e 2h → "TP Tardio" / "SL Tardio"
  - Nada atingido em 2h → "Falhado"

Corre em paralelo com o scanner no mesmo processo Railway.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import aiohttp

from notion_scalp import _headers, NOTION_API

logger = logging.getLogger(__name__)

# ── Constantes ────────────────────────────────────────────────────────────────
JANELA_SCALP_SECS = 30 * 60      # 30 minutos — janela de scalp válido
JANELA_MAX_SECS   = 2 * 60 * 60  # 2 horas — janela máxima
CHECK_INTERVAL    = 60            # verifica preço a cada 60 segundos


# ── Estrutura de alerta monitorizado ─────────────────────────────────────────

@dataclass
class AlertaMonitorizado:
    symbol:         str
    direction:      str            # "LONG" ou "SHORT"
    price_entry:    float          # preço no momento do alerta
    tp1:            float
    sl:             float
    score:          int
    setup_type:     Optional[str]
    notion_page_id: Optional[str]  # ID da página Notion a actualizar
    timestamp:      float = field(default_factory=time.time)

    @property
    def idade_secs(self) -> float:
        return time.time() - self.timestamp

    @property
    def expirado(self) -> bool:
        return self.idade_secs > JANELA_MAX_SECS

    @property
    def em_janela_scalp(self) -> bool:
        return self.idade_secs <= JANELA_SCALP_SECS

    def check_resultado(self, preco_actual: float) -> Optional[str]:
        """
        Verifica se TP1 ou SL foi atingido.
        Devolve resultado ou None se ainda activo.
        """
        if self.direction == "LONG":
            tp_atingido = preco_actual >= self.tp1
            sl_atingido = preco_actual <= self.sl
        else:  # SHORT
            tp_atingido = preco_actual <= self.tp1
            sl_atingido = preco_actual >= self.sl

        if tp_atingido:
            return "TP Hit" if self.em_janela_scalp else "TP Tardio"
        if sl_atingido:
            return "SL Hit" if self.em_janela_scalp else "SL Tardio"
        if self.expirado:
            return "Falhado"
        return None  # ainda activo


# ── Monitor ───────────────────────────────────────────────────────────────────

class MonitorAlertas:
    """
    Mantém uma fila de alertas activos e monitoriza resultados.
    Thread-safe para uso com asyncio.
    """

    def __init__(self):
        self._alertas: list[AlertaMonitorizado] = []
        self._lock = asyncio.Lock()

    async def registar(
        self,
        symbol: str,
        direction: str,
        price_entry: float,
        tp1: float,
        sl: float,
        score: int,
        setup_type: Optional[str],
        notion_page_id: Optional[str] = None,
    ):
        """Adiciona alerta à fila de monitorização."""
        alerta = AlertaMonitorizado(
            symbol=symbol,
            direction=direction,
            price_entry=price_entry,
            tp1=tp1,
            sl=sl,
            score=score,
            setup_type=setup_type,
            notion_page_id=notion_page_id,
        )
        async with self._lock:
            self._alertas.append(alerta)
        logger.info(
            f"Monitor: a monitorizar {symbol} {direction} | "
            f"Entry ${price_entry:,.4f} | TP1 ${tp1:,.4f} | SL ${sl:,.4f}"
        )

    async def run(self, session: aiohttp.ClientSession):
        """Loop de monitorização — corre indefinidamente em paralelo."""
        from mexc_client import MexcClient
        client = MexcClient(session)

        logger.info("Monitor de alertas iniciado")

        while True:
            await asyncio.sleep(CHECK_INTERVAL)

            async with self._lock:
                alertas_activos = list(self._alertas)

            if not alertas_activos:
                continue

            logger.debug(f"Monitor: {len(alertas_activos)} alertas activos")

            concluídos = []

            for alerta in alertas_activos:
                try:
                    ticker = await client.get_ticker(alerta.symbol)
                    if not ticker:
                        continue

                    preco = float(ticker.get("lastPrice", 0))
                    if preco <= 0:
                        continue

                    resultado = alerta.check_resultado(preco)

                    if resultado:
                        logger.info(
                            f"Monitor: {alerta.symbol} → {resultado} | "
                            f"Preço ${preco:,.4f} | "
                            f"Entry ${alerta.price_entry:,.4f} | "
                            f"Idade {alerta.idade_secs/60:.1f}min"
                        )
                        await _registar_resultado_notion(
                            session=session,
                            alerta=alerta,
                            resultado=resultado,
                            preco_saida=preco,
                        )
                        concluídos.append(alerta)

                    await asyncio.sleep(0.1)

                except Exception as e:
                    logger.error(f"Monitor erro {alerta.symbol}: {e}")

            # remove alertas concluídos
            if concluídos:
                async with self._lock:
                    for a in concluídos:
                        if a in self._alertas:
                            self._alertas.remove(a)


# ── Actualizar Notion ─────────────────────────────────────────────────────────

async def _registar_resultado_notion(
    session: aiohttp.ClientSession,
    alerta: AlertaMonitorizado,
    resultado: str,
    preco_saida: float,
) -> bool:
    """
    Actualiza a página do alerta no Notion com o resultado.
    Nomes de campos sem acentos — consistente com o schema de criação.
    """
    from config import NOTION_DB_ALERTAS_CSA
    if not NOTION_DB_ALERTAS_CSA:
        return False

    # calcular PnL do alerta (independente de entrada real)
    if alerta.direction == "LONG":
        pnl_pct = (preco_saida - alerta.price_entry) / alerta.price_entry * 100
    else:
        pnl_pct = (alerta.price_entry - preco_saida) / alerta.price_entry * 100

    agora    = datetime.now(timezone.utc).isoformat()
    idade_min = round(alerta.idade_secs / 60, 1)

    if not alerta.notion_page_id:
        logger.debug(f"Monitor: sem notion_page_id para {alerta.symbol} — resultado não registado")
        return False

    payload = {
        "properties": {
            "Resultado": {
                "select": {"name": resultado}
            },
            "Preco Saida": {          # sem acento — igual ao schema de criação
                "number": preco_saida
            },
            "PnL Alerta (%)": {
                "number": round(pnl_pct, 3)
            },
            "Duracao (min)": {        # sem acento — igual ao schema de criação
                "number": idade_min
            },
            "Data Resultado": {
                "date": {"start": agora}
            },
        }
    }

    try:
        async with session.patch(
            f"{NOTION_API}/pages/{alerta.notion_page_id}",
            json=payload,
            headers=_headers(),
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            if r.status == 200:
                logger.info(f"Notion actualizado: {alerta.symbol} → {resultado} | PnL {pnl_pct:+.2f}%")
                return True
            text = await r.text()
            logger.error(f"Notion update erro {r.status}: {text[:200]}")
            return False
    except Exception as e:
        logger.error(f"Notion update excepção: {e}")
        return False


# ── Instância global ──────────────────────────────────────────────────────────
# Importada pelo scanner_scalp.py
monitor = MonitorAlertas()
