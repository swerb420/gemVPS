import os
import sys
import types
from pathlib import Path

# Ensure package modules can be imported in the tests
ROOT = Path(__file__).resolve().parents[1] / "gemVPS"
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

from utils.logger import setup_logging_directory
from utils.config import Settings

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
    for k, v in REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)


def test_aggregator_instantiation(monkeypatch):
    setup_env(monkeypatch)

    # Create logging directory so file handler doesn't fail
    setup_logging_directory()

    # Stub database.db_manager before importing aggregator
    dummy_db_module = types.ModuleType("database.db_manager")
    class DummyDBManager:
        pass
    dummy_db_module.DBManager = DummyDBManager
    monkeypatch.setitem(sys.modules, "database.db_manager", dummy_db_module)

    from signals.signal_aggregator import AdvancedSignalAggregator

    config = Settings.model_validate({k: v for k, v in os.environ.items()})
    agg = AdvancedSignalAggregator(config, db=None, redis_client=None)
    assert agg.config == config
