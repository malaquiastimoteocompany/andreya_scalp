# andreya_scalp — CSA v1.0 (Crypto Scalp Alerts)

Sistema de alertas de scalp complementar ao CFI v2.0.
Detecta oportunidades de scalp de minutos nos futuros perpétuos MEXC.
**Semi-automático — gera alertas, decisão de entrada é sempre humana.**

---

## Variáveis de ambiente (Railway)

| Variável | Descrição |
|---|---|
| `TELEGRAM_TOKEN` | Token do bot Andreya (mesmo do CFI) |
| `TELEGRAM_CHAT_SCALP` | Chat ID do canal Scalp (`-1003968883049`) |
| `NOTION_TOKEN` | Token da integração Notion |
| `NOTION_DB_ALERTAS_CSA` | ID da base Alertas CSA (preencher após criar) |
| `NOTION_DB_TRADES_SCALP` | ID da base Trades Scalp (preencher após criar) |

---

## Estrutura

```
scanner_scalp.py       # loop principal Railway
signals_scalp.py       # S/R, RSI, ATR, CVD, walls
mexc_client.py         # cliente MEXC assíncrono
scoring_scalp.py       # motor de scoring 0-10
notificacoes_scalp.py  # alertas Telegram canal Scalp
notion_scalp.py        # logging Notion
config.py              # constantes e env vars
```

---

## Lógica de scoring (0-10)

| Componente | Pontos |
|---|---|
| Zona S/R estrutural (>= 2 toques 1h) | +2 |
| RSI extremo 1h | +1 |
| Cluster de liquidez (bid wall proxy) | +2 |
| Bid/Ask wall no order book | +1 |
| CVD absorção ou divergência | +1 |
| Volume spike ou compressão | +1 |
| Liquidações recentes (proxy OI) | +1 |
| Token em E2/E3 no CFI v2.0 | +1 |

- Score < 6 → não envia
- Score 6-7 → alerta standard ⚡
- Score 8+ → alerta prioritário 🔥

---

## Setups detectados

- **Setup A** — Bounce em Zona Estrutural (R/R 1.5:1, TP 1-3%)
- **Setup B** — Squeeze de Liquidez (R/R 2:1, TP 1-2%)
- **Setup C** — Compressão de Volatilidade (R/R 2.5:1, TP 2-5%)

---

## Regras de risco (não negociáveis)

1. SL obrigatório antes de entrar
2. Leverage máxima 10x
3. Sizing máximo 5% da banca por trade
4. Sem revenge trades
5. Janela de entrada: 5 minutos após alerta
6. Máximo 2 trades scalp simultâneos

---

## Deploy Railway

1. Criar novo serviço Railway a partir deste repo
2. Configurar variáveis de ambiente
3. Railway detecta `Procfile` e executa `python scanner_scalp.py`
4. O processo corre indefinidamente (loop assíncrono)
