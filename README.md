# GemVPS Trading Bot

GemVPS is an experimental cryptocurrency trading bot built around a collection of modular watchers and analysis engines.  It consumes on‑chain and off‑chain data, aggregates trading signals and exposes a small API and Telegram bot for control.

## Requirements

- Python 3.10+
- A small VPS (2 vCPU / 2 GB RAM is sufficient for the default modules)

## Installation

1. Install Python dependencies
   ```bash
   pip install -r requirements.txt
   ```
2. Copy `.env.example` to `.env` and fill in your API keys and other settings.
3. Start the application
   ```bash
   python src/main.py
   ```

## Environment variables

Copy `.env.example` to `.env` and populate the following variables:

- `TELEGRAM_BOT_TOKEN` – token for your Telegram bot
- `TELEGRAM_CHAT_ID` – chat ID that receives alerts
- `WEB3_PROVIDER_URL` – HTTPS provider for on-chain data
- `ALCHEMY_WEBSOCKET_URL` – WebSocket endpoint for real-time events
- `SHYFT_API_KEY` – Shyft API key
- `NEWS_API_KEY` – news API key (optional)
- `SANTIMENT_API_KEY` – Santiment API key (optional)
- `VPS_PUBLIC_URL` – public base URL for webhook callbacks
- `POSTGRES_USER` – PostgreSQL username
- `POSTGRES_PASSWORD` – PostgreSQL password
- `POSTGRES_DB` – PostgreSQL database name
- `POSTGRES_HOST` – PostgreSQL host or service name
- `POSTGRES_PORT` – PostgreSQL port
- `REDIS_HOST` – Redis host
- `REDIS_PORT` – Redis port
- `POSTGRES_SHARED_BUFFERS` – amount of memory allocated to shared buffers
- `POSTGRES_WORK_MEM` – working memory per sort operation
- `POSTGRES_MAX_CONNECTIONS` – maximum PostgreSQL connections
- `ENABLE_AUTO_TRADING` – set to `True` to allow automatic trade execution
- `STOCK_LIMIT` – number of correlated stocks to track

## VPS considerations

The bot can run on a 2 vCPU/2 GB instance but heavy analysis modules may need to be scheduled less frequently or disabled when memory is limited.
Optional components such as the `WeightOptimizer` or the experimental `CorrelationEngine` consume additional CPU and RAM.  Users on very small servers can comment them out in `src/main.py` to reduce load.
