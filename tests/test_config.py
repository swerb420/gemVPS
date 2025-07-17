import os
import sys
import types
from pathlib import Path

# Add package roots to sys.path so imports work when running tests directly
ROOT = Path(__file__).resolve().parents[1] / "gemVPS"
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

from utils.config import Settings

# Environment variables for tests
REQUIRED_ENV = {
    "TELEGRAM_BOT_TOKEN": "token",
    "TELEGRAM_CHAT_ID": "123",
    "WEB3_PROVIDER_URL": "http://localhost",
    "ALCHEMY_WEBSOCKET_URL": "wss://example.com/ws",
    "POLYGON_RPC_URL": "http://localhost:8545",
    "SHYFT_API_KEY": "shyft",
    "EXCHANGE_API_KEY": "exapikey",
    "EXCHANGE_SECRET_KEY": "exsecret",
    "SHYFT_WEBHOOK_SECRET": "secret",
    "VPS_PUBLIC_URL": "http://localhost",
}

def setup_env(monkeypatch):
    for key, value in REQUIRED_ENV.items():
        monkeypatch.setenv(key, value)


def test_load_config(monkeypatch):
    setup_env(monkeypatch)
    config = Settings.model_validate({k: v for k, v in os.environ.items()})
    assert config.TELEGRAM_CHAT_ID == "123"
    assert str(config.WEB3_PROVIDER_URL).startswith("http://localhost")
