# src/execution/trade_executor.py
import asyncio
import json
import ccxt.async_support as ccxt
from typing import Dict, Any, Optional
from utils.logger import get_logger
from utils.config import Settings
from signals.signal_aggregator import AdvancedSignalAggregator
from database.db_manager import DBManager # Import DBManager for logging paper trades

logger = get_logger(__name__)

class TradeExecutor:
    """
    Handles the execution of trades on a centralized exchange, supporting both
    live and paper trading modes. This module subscribes to high-priority signals
    and translates them into either live or simulated orders.
    """
    def __init__(self, config: Settings, signal_aggregator: AdvancedSignalAggregator, db: DBManager, redis_client: Any):
        self.config = config
        self.signal_aggregator = signal_aggregator
        self.db = db
        self.redis = redis_client
        self.exchange: Optional[ccxt.Exchange] = None
        
        # Determine trading mode from config, defaulting to 'paper' for safety.
        self.trade_mode = getattr(config, 'TRADE_MODE', 'paper').lower()
        self.is_enabled = config.ENABLE_AUTO_TRADING

        # --- Paper Trading State ---
        self.paper_balance_usd = 10000.0 # Starting virtual balance
        self.paper_positions = {} # To track open paper positions

        if self.is_enabled:
            self._initialize_exchange()

    def _initialize_exchange(self):
        """Initializes the CCXT exchange instance. Required for both live price data and live trading."""
        try:
            api_key = None
            secret_key = None
            if self.trade_mode == 'live':
                api_key = getattr(self.config, "EXCHANGE_API_KEY", None)
                secret_key = getattr(self.config, "EXCHANGE_SECRET_KEY", None)
                if not api_key or not secret_key:
                    logger.warning(
                        "Exchange API credentials missing; disabling live trading and skipping exchange initialization."
                    )
                    self.is_enabled = False
                    return

            exchange_class = getattr(ccxt, 'binance')
            # For live trading, validate that keys are provided before proceeding.
            if self.trade_mode == 'live':
                api_key = self.config.EXCHANGE_API_KEY.get_secret_value().strip()
                secret_key = self.config.EXCHANGE_SECRET_KEY.get_secret_value().strip()
                if not api_key or not secret_key:
                    logger.warning(
                        "Exchange API keys are not configured. Disabling trading module."
                    )
                    self.is_enabled = False
                    return
                api_keys = {'apiKey': api_key, 'secret': secret_key}
            else:
                api_keys = {'apiKey': 'PAPER_KEY', 'secret': 'PAPER_SECRET'}
            self.exchange = exchange_class({
                **api_keys,
                'options': {'defaultType': 'future'},
            })
            if self.trade_mode == 'paper':
                logger.info("Exchange interface initialized in read-only mode for paper trading.")
            logger.info(f"✅ Trade Executor initialized for {self.exchange.id} in '{self.trade_mode}' mode.")
        except Exception as e:
            logger.critical(f"❌ Failed to initialize exchange for Trade Executor: {e}", exc_info=True)
            self.is_enabled = False

    async def _handle_signal(self, signal: Dict[str, Any]):
        """Processes a high-priority signal and executes a trade based on the current mode."""
        logger.critical(f"Received high-priority trade signal: {signal}")
        
        if self.trade_mode == 'live':
            await self._execute_live_trade(signal)
        else:
            await self._execute_paper_trade(signal)

    async def _execute_paper_trade(self, signal: Dict[str, Any]):
        """Simulates a trade and records it to the database without executing a live order."""
        if signal.get('type') == 'CEX_LISTING_ARBITRAGE':
            symbol = signal.get('asset')
            market_symbol = f"{symbol}/USDT"
            
            try:
                # 1. Fetch live price to make the simulation realistic
                ticker = await self.exchange.fetch_ticker(market_symbol)
                price = ticker.get("last")
                if not price:
                    logger.error(f"[PAPER TRADE] Could not fetch live price for {market_symbol}. Aborting.")
                    return

                # 2. Define trade parameters
                trade_size_usd = 500.0
                if self.paper_balance_usd < trade_size_usd:
                    logger.warning(f"[PAPER TRADE] Insufficient virtual funds to execute trade.")
                    return
                
                amount_to_buy = trade_size_usd / price
                
                # 3. Simulate the trade
                self.paper_balance_usd -= trade_size_usd
                self.paper_positions[market_symbol] = {
                    "amount": amount_to_buy, "entry_price": price
                }
                
                logger.critical(f"✅ [PAPER TRADE] EXECUTED: Bought {amount_to_buy:.4f} {symbol} at ~${price}")
                
                # 4. Log the paper trade to the database for analysis
                await self.db.execute_with_retry(
                    """
                    INSERT INTO paper_trades (market_symbol, trade_direction, entry_price, amount, trade_size_usd, status, entry_signal_type)
                    VALUES ($1, 'long', $2, $3, $4, 'open', $5)
                    """,
                    market_symbol, price, amount_to_buy, trade_size_usd, signal.get('type')
                )

            except Exception as e:
                logger.error(f"An unexpected error occurred during paper trade execution: {e}", exc_info=True)

    async def _execute_live_trade(self, signal: Dict[str, Any]):
        """Executes a real trade on the exchange. WARNING: HIGH RISK."""
        logger.warning(f"Executing LIVE trade for signal: {signal}")
        # The original live trading logic would go here.
        # ...

    async def run_loop(self):
        """The main loop. Subscribes to the Redis channel and listens for signals."""
        if not self.is_enabled:
            logger.warning("Trade Executor is disabled by configuration.")
            return

        if not self.redis or not self.exchange:
            logger.error("Trade Executor cannot start due to missing Redis or Exchange client.")
            return

        logger.critical(f"🚨 TRADE EXECUTOR IS LIVE in '{self.trade_mode.upper()}' MODE. 🚨")
        
        try:
            pubsub = self.redis.pubsub()
            await pubsub.subscribe("high-priority-signals")
            
            while True:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=None)
                if message and message.get('type') == 'message':
                    try:
                        signal_data = json.loads(message['data'])
                        asyncio.create_task(self._handle_signal(signal_data))
                    except json.JSONDecodeError:
                        logger.error(f"Could not decode signal from Redis: {message['data']}")
        except asyncio.CancelledError:
            logger.info("Trade Executor loop cancelled.")
        except Exception as e:
            logger.error(f"An error occurred in the Trade Executor Redis listener: {e}", exc_info=True)
        finally:
            if 'pubsub' in locals(): await pubsub.close()
            if self.exchange: await self.exchange.close()
