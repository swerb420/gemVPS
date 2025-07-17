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

## VPS considerations

The bot can run on a 2 vCPU/2 GB instance but resource intensive modules (like correlation analysis or weight optimisation) may need to be scheduled less frequently or disabled when memory is limited.

