import os

# ── Telegram ──────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_SCALP = int(os.environ.get("TELEGRAM_CHAT_SCALP", "-1003968883049"))

# ── MEXC API ──────────────────────────────────────────────────────────────────
MEXC_BASE = "https://contract.mexc.com"

# ── Notion ────────────────────────────────────────────────────────────────────
NOTION_TOKEN = os.environ.get("NOTION_TOKEN", "")
NOTION_DB_ALERTAS_CSA  = os.environ.get("NOTION_DB_ALERTAS_CSA", "")   # preencher após criar
NOTION_DB_TRADES_SCALP = os.environ.get("NOTION_DB_TRADES_SCALP", "")  # preencher após criar

# ── Filtros de liquidez para scalp ───────────────────────────────────────────
MIN_VOLUME_24H    = 5_000_000   # USD
MIN_OI            = 500_000     # USD
MAX_SPREAD_PCT    = 0.001       # 0.1%
MIN_CANDLES_1H    = 168         # 7 dias de histórico

# ── Scoring ───────────────────────────────────────────────────────────────────
SCORE_MIN_ENVIO   = 6           # abaixo → não envia

# ── Setup A — Bounce em Zona Estrutural ──────────────────────────────────────
SR_ZONE_TOLERANCE = 0.005       # preço dentro de 0.5% da zona
SR_MIN_TOUCHES    = 2           # mínimo de toques no 1h
RSI_LONG_MAX      = 35          # RSI 1h sobrevenda
RSI_SHORT_MIN     = 65          # RSI 1h sobrecompra
VOLUME_DRY_RATIO  = 0.7         # volume últimos 3 candles < 70% da média

# ── Setup B — Squeeze de Liquidez ────────────────────────────────────────────
LIQ_CASCADE_MIN   = 500_000     # USD de liquidações em 15 min (proxy OI)
PRICE_MOVE_15M    = 0.02        # movimento > 2% em 15 min
VOLUME_SPIKE_X    = 3.0         # volume > 300% da média 1h
RSI_5M_LONG_MAX   = 20
RSI_5M_SHORT_MIN  = 80

# ── Setup C — Compressão de Volatilidade ─────────────────────────────────────
ATR_COMPRESS_RATIO   = 0.5      # ATR 1h < 50% do ATR semanal
RANGE_COMPRESS_PCT   = 0.015    # range 4 candles < 1.5%
VOLUME_COMPRESS_RATIO = 0.6     # volume < 60% da média 7d

# ── Bid/Ask wall ─────────────────────────────────────────────────────────────
WALL_MIN_USD      = 200_000     # mínimo USD numa zona de preço para contar como wall
WALL_ZONE_PCT     = 0.005       # agrupa ordens dentro de 0.5% do preço

# ── CVD ───────────────────────────────────────────────────────────────────────
CVD_CANDLES       = 20          # número de trades recentes para calcular delta
CVD_ABSORB_RATIO  = 0.3         # delta negativo mas preço não cede > 0.3% → absorção

# ── OI proxy liquidações ──────────────────────────────────────────────────────
OI_DROP_CASCADE   = 0.02        # queda OI > 2% em 5 min = cascade detectado
OI_HISTORY_MINS   = 15          # janela de monitorização

# ── Scanner ───────────────────────────────────────────────────────────────────
SCAN_INTERVAL_SEC = 150         # 2.5 minutos entre ciclos completos
REQUEST_DELAY     = 0.08        # delay entre requests MEXC (ms → s)
MAX_TOKENS_SCALP  = 500         # universo máximo

# ── Alerta validade ───────────────────────────────────────────────────────────
ALERT_VALID_MINS  = 5           # janela de entrada após alerta
