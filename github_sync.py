"""
github_sync.py — Espelho em JSON no GitHub dos alertas CSA, ao lado do Notion.

Motivo (07/07/2026, pedido do Malaquias): este workspace do Notion não
permite queries em bloco via API (exige plano Business) — toda a análise
estatística até agora dependeu de exports manuais de CSV. Este módulo
duplica cada alerta para um ficheiro JSON no repo, tal como já se faz no
CFI para o S2b (s2b_outcomes_v2.json), dando acesso directo aos dados sem
exportação manual.

O Notion continua a ser a fonte visual principal — isto é só um espelho,
nunca deve bloquear nem derrubar o fluxo principal se falhar. Qualquer
erro aqui fica só em log.

Ficheiro no repo: csa_alertas.json (array de registos, um por alerta,
identificado pelo page_id do Notion para permitir actualização depois).

NOTA DE ARRANQUE: precisa de duas variáveis de ambiente novas no Railway,
que ainda não existiam neste serviço — GITHUB_TOKEN (com permissão de
escrita no repo) e GITHUB_REPO (ex.: "malaquiastimoteocompany/andreya_scalp").
Sem elas, este módulo fica em no-op silencioso (não regista, não falha).

NOTA DE VOLUME: o CSA gera muito mais alertas por dia que o S2b (até
~1000/dia, dezenas em simultâneo) — cada criação e cada fecho de alerta é
um commit ao ficheiro. Um asyncio.Lock() serializa as escritas dentro
deste processo para evitar conflitos de SHA quando duas corrotinas tentam
escrever ao mesmo tempo. Se o ficheiro crescer demasiado (dezenas de
milhares de registos) e os commits começarem a abrandar, a solução é
separar por dia (csa_alertas_2026-07-07.json) — não implementado agora,
para manter isto simples enquanto validamos que funciona.
"""

import asyncio
import base64
import json
import logging
from typing import Optional

import aiohttp

from config import GITHUB_TOKEN, GITHUB_REPO

logger = logging.getLogger(__name__)

GITHUB_API = "https://api.github.com"
ALERTAS_JSON_PATH = "csa_alertas.json"

# Serializa leituras/escritas dentro deste processo — o monitor e o scanner
# podem chamar isto a partir de corrotinas diferentes ao mesmo tempo, e o
# GitHub usa concorrência optimista por SHA (falha se o ficheiro mudou
# entre o GET e o PUT). Sem isto, duas escritas próximas no tempo podiam
# pisar-se uma à outra.
_lock = asyncio.Lock()


def _configurado() -> bool:
    return bool(GITHUB_TOKEN and GITHUB_REPO)


def _headers() -> dict:
    return {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "andreya-scalp-sync",
    }


async def _carregar(session: aiohttp.ClientSession) -> tuple[list[dict], Optional[str]]:
    url = f"{GITHUB_API}/repos/{GITHUB_REPO}/contents/{ALERTAS_JSON_PATH}"
    async with session.get(url, headers=_headers(), timeout=aiohttp.ClientTimeout(total=15)) as r:
        if r.status == 404:
            return [], None
        if r.status != 200:
            texto = await r.text()
            raise RuntimeError(f"GitHub GET {r.status}: {texto[:200]}")
        dados = await r.json()
        sha = dados["sha"]
        conteudo_b64 = dados.get("content")

    # A Contents API não devolve 'content' inline para ficheiros >~1MB (vem
    # string vazia, encoding "none") — foi isto que parou o sync em silêncio
    # a partir de 08/07/2026 quando o ficheiro cruzou 1MB (mesma limitação já
    # corrigida do lado do S2b, s2b_outcomes_v2.json). Correcção 12/07/2026:
    # nesse caso, ir buscar via Git Data API (blobs), sem o mesmo limite.
    if not conteudo_b64:
        blob_url = f"{GITHUB_API}/repos/{GITHUB_REPO}/git/blobs/{sha}"
        async with session.get(
            blob_url, headers=_headers(), timeout=aiohttp.ClientTimeout(total=30)
        ) as rb:
            if rb.status != 200:
                texto = await rb.text()
                raise RuntimeError(f"GitHub GET blob {rb.status}: {texto[:200]}")
            dados_blob = await rb.json()
            conteudo_b64 = dados_blob["content"]

    conteudo = base64.b64decode(conteudo_b64).decode()
    return json.loads(conteudo), sha


async def _guardar(
    session: aiohttp.ClientSession, registos: list[dict], sha: Optional[str], mensagem: str
) -> None:
    url = f"{GITHUB_API}/repos/{GITHUB_REPO}/contents/{ALERTAS_JSON_PATH}"
    conteudo_b64 = base64.b64encode(
        json.dumps(registos, indent=2, ensure_ascii=False).encode()
    ).decode()
    payload = {"message": mensagem, "content": conteudo_b64}
    if sha:
        payload["sha"] = sha
    async with session.put(
        url, json=payload, headers=_headers(), timeout=aiohttp.ClientTimeout(total=15)
    ) as r:
        if r.status not in (200, 201):
            texto = await r.text()
            raise RuntimeError(f"GitHub PUT {r.status}: {texto[:200]}")


async def registar_alerta(session: aiohttp.ClientSession, page_id: str, registo: dict) -> None:
    """
    Acrescenta um novo alerta ao espelho JSON. Chamado a seguir a criar a
    página no Notion, com o mesmo page_id para permitir actualizar depois.
    Nunca propaga excepção — o Notion já é a fonte de verdade, isto é
    só um espelho.
    """
    if not _configurado():
        return
    try:
        async with _lock:
            registos, sha = await _carregar(session)
            registos.append({"page_id": page_id, **registo})
            await _guardar(session, registos, sha, f"CSA: novo alerta {registo.get('token', '?')}")
    except Exception as e:
        logger.error(f"github_sync: falha a registar alerta {page_id}: {e}")


async def actualizar_alerta(session: aiohttp.ClientSession, page_id: str, updates: dict) -> None:
    """
    Actualiza o alerta correspondente a page_id no espelho JSON — chamado a
    par de cada escrita no Notion (TP1 interino ou fecho final). Mesma
    filosofia de falha silenciosa.
    """
    if not _configurado():
        return
    try:
        async with _lock:
            registos, sha = await _carregar(session)
            encontrado = False
            for r in registos:
                if r.get("page_id") == page_id:
                    r.update(updates)
                    encontrado = True
                    break
            if not encontrado:
                logger.debug(f"github_sync: page_id {page_id} não encontrado no espelho — ignorado")
                return
            await _guardar(session, registos, sha, f"CSA: actualizar alerta {page_id[:8]}")
    except Exception as e:
        logger.error(f"github_sync: falha a actualizar alerta {page_id}: {e}")
