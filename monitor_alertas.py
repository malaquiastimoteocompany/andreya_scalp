"""
monitor_alertas.py — Monitorização automática de resultados CSA v1.0

Após cada alerta enviado, monitoriza o preço e regista o resultado no Notion.
Fecho em duas metades (50/50), igual ao manual CMF v1.6 Secção 11.2:

  - TP1 atingido → fecha 50% da posição, SL da 2ª metade sobe para
    break-even. Grava interinamente "TP Hit" (<=30min) ou "TP Tardio"
    (30min-2h) — esta linha é reescrita quando a 2ª metade fechar.
  - 2ª metade, depois do TP1:
      - TP2 atingido → "TP2 Hit" (<=30min desde a entrada) / "TP2 Tardio"
      - Break-even atingido → "BE Hit" / "BE Tardio" (a 1ª metade já
        garantiu o ganho do TP1 — isto não é uma perda)
      - Nada atingido em 2h → "Falhado" (a 1ª metade continua a contar
        para o PnL final — não é um "falhanço" total, só a 2ª metade
        ficou pelo caminho)
  - SL original atingido ANTES do TP1 → "SL Hit" (<=30min) / "SL Tardio"
    (30min-2h) — perda real, posição inteira, sem 50/50 (nunca chegou a
    fechar a 1ª metade).
  - Nada atingido em 2h, sem nunca ter tocado o TP1 → "Falhado" — posição
    inteira, sem 50/50.

  PnL Alerta (%) final = média das duas metades quando o TP1 foi atingido;
  ou o cálculo de posição inteira quando nunca chegou ao TP1 (SL Hit/Tardio
  ou Falhado sem TP1).

CORREÇÃO 04/07/2026: até aqui, depois do TP1 a posição ficava toda aberta
e sem SL nenhum (bug introduzido em 01/07 ao adicionar o TP2) — produziu
perdas de -6% a -15.8% registadas como "Falhado" em vez de perda real.
Corrigido para fecho 50/50 com SL de break-even na 2ª metade — pedido do
Malaquias, 04/07/2026, para reflectir a prática já usada manualmente no
CMF: "para isso temos de fechar também metade da posição no TP1".

Nota operacional: as etiquetas "BE Hit"/"BE Tardio" são novas — se o campo
"Resultado" no Notion for select com opções fixas, pode ser preciso
adicioná-las lá (ou a 1ª escrita cria a opção automaticamente, dependendo
das permissões da integração).

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
    tp2:            float          # 1.0×ATR — segundo objectivo
    sl:             float          # SL activo — original até TP1, depois break-even
    score:          int
    setup_type:     Optional[str]
    notion_page_id: Optional[str]  # ID da página Notion a actualizar
    timestamp:      float = field(default_factory=time.time)
    tp1_atingido:   bool  = False  # True após TP1 ser atingido (50% fechado)
    pnl_metade1:    Optional[float] = None  # % da 1ª metade, fixado no TP1

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
        Verifica TP1, TP2 ou SL. O SL é sempre verificado — antes do TP1 é
        o SL original (1.5×ATR, posição inteira), depois do TP1 é o
        break-even da 2ª metade (ver __TP1__ no loop, que actualiza
        self.sl e fixa self.pnl_metade1). Nunca fica sem stop nenhum.
        Devolve resultado ou None se ainda activo.
        """
        if self.direction == "LONG":
            sl_atingido  = preco_actual <= self.sl
            tp1_atingido = preco_actual >= self.tp1
            tp2_atingido = preco_actual >= self.tp2
        else:  # SHORT
            sl_atingido  = preco_actual >= self.sl
            tp1_atingido = preco_actual <= self.tp1
            tp2_atingido = preco_actual <= self.tp2

        # SL verifica-se SEMPRE — antes do TP1 é perda real (posição
        # inteira), depois do TP1 é só a 2ª metade a voltar ao break-even
        # (a 1ª metade já garantiu o ganho do TP1).
        if sl_atingido:
            if not self.tp1_atingido:
                return "SL Hit" if self.em_janela_scalp else "SL Tardio"
            return "BE Hit" if self.em_janela_scalp else "BE Tardio"

        # Fase 2 — após TP1 (50% fechado, SL da 2ª metade em break-even)
        if self.tp1_atingido:
            if tp2_atingido:
                return "TP2 Hit" if self.em_janela_scalp else "TP2 Tardio"
            if self.expirado:
                return "Falhado"  # 2ª metade ficou entre break-even e TP2 até expirar
            return None

        # Fase 1 — aguardar TP1 (posição ainda inteira)
        if tp1_atingido:
            # não fecha aqui — marca tp1_atingido, fixa pnl_metade1 e continua
            return "__TP1__"      # sinal interno — tratado no loop

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

    async def esta_activo(self, symbol: str) -> bool:
        """
        True se já existe um alerta deste symbol em monitorização
        (ainda não atingiu TP2, SL, nem expirou).
        Usado pelo scanner para evitar registar o mesmo token duas vezes
        enquanto o sinal anterior ainda está "aberto".
        """
        async with self._lock:
            return any(a.symbol == symbol for a in self._alertas)

    async def registar(
        self,
        symbol: str,
        direction: str,
        price_entry: float,
        tp1: float,
        tp2: float,
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
            tp2=tp2,
            sl=sl,
            score=score,
            setup_type=setup_type,
            notion_page_id=notion_page_id,
        )
        async with self._lock:
            self._alertas.append(alerta)
        logger.info(
            f"Monitor: a monitorizar {symbol} {direction} | "
            f"Entry ${price_entry:,.4f} | TP1 ${tp1:,.4f} | TP2 ${tp2:,.4f} | SL ${sl:,.4f}"
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

                    if resultado == "__TP1__":
                        # TP1 atingido — fecha 50%, fixa o ganho dessa metade,
                        # sobe SL da 2ª metade para break-even, notifica e
                        # continua a monitorizar a 2ª metade (agora protegida).
                        if not alerta.tp1_atingido:
                            alerta.tp1_atingido = True
                            if alerta.direction == "LONG":
                                alerta.pnl_metade1 = (preco - alerta.price_entry) / alerta.price_entry * 100
                            else:
                                alerta.pnl_metade1 = (alerta.price_entry - preco) / alerta.price_entry * 100
                            alerta.sl = alerta.price_entry  # break-even da 2ª metade — CORREÇÃO 04/07
                            resultado_tp1 = "TP Hit" if alerta.em_janela_scalp else "TP Tardio"
                            logger.info(
                                f"Monitor: {alerta.symbol} → {resultado_tp1} (TP1, 50%) | "
                                f"Preço ${preco:,.4f} | Entry ${alerta.price_entry:,.4f} | "
                                f"1ª metade: {alerta.pnl_metade1:+.2f}% | "
                                f"Idade {alerta.idade_secs/60:.1f}min — SL da 2ª metade em break-even"
                            )
                            await _registar_resultado_notion(
                                session=session,
                                alerta=alerta,
                                resultado=resultado_tp1,
                                preco_saida=preco,
                                interino=True,
                            )
                        # não adiciona a concluídos — continua a monitorizar

                    elif resultado:
                        # TP2, SL ou Falhado — fecha o alerta
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
    interino: bool = False,
) -> bool:
    """
    Actualiza a página do alerta no Notion com o resultado.
    Nomes de campos sem acentos — consistente com o schema de criação.

    interino=True: escrita feita no momento do TP1 (50% fechado) — grava
    só a % dessa metade, esta linha vai ser reescrita quando a 2ª metade
    fechar (BE/TP2/Falhado). interino=False (default): escrita final —
    combina pnl_metade1 (fixado no TP1) com a % desta 2ª metade, ou usa o
    cálculo de posição inteira se nunca chegou a atingir o TP1
    (SL Hit/Tardio, ou Falhado sem TP1).
    """
    from config import NOTION_DB_ALERTAS_CSA
    if not NOTION_DB_ALERTAS_CSA:
        return False

    if alerta.direction == "LONG":
        pnl_desta_perna = (preco_saida - alerta.price_entry) / alerta.price_entry * 100
    else:
        pnl_desta_perna = (alerta.price_entry - preco_saida) / alerta.price_entry * 100

    if interino or alerta.pnl_metade1 is None:
        # TP1 acabou de acontecer (interino), ou nunca chegou a atingir o
        # TP1 (posição inteira, sem 50/50) — usa a perna sozinha.
        pnl_pct = pnl_desta_perna
    else:
        # 2ª metade fechou — combina com a 1ª metade já fixada no TP1.
        pnl_pct = (alerta.pnl_metade1 + pnl_desta_perna) / 2

    agora     = datetime.now(timezone.utc).isoformat()
    idade_min = round(alerta.idade_secs / 60, 1)

    if not alerta.notion_page_id:
        logger.debug(f"Monitor: sem notion_page_id para {alerta.symbol} — resultado não registado")
        return False

    payload = {
        "properties": {
            "Resultado": {
                "select": {"name": resultado}
            },
            "Preco Saida": {
                "number": preco_saida
            },
            "PnL Alerta (%)": {
                "number": round(pnl_pct, 3)
            },
            "Duracao (min)": {
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
