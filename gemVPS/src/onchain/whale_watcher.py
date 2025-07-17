# src/onchain/whale_watcher.py
import asyncio
import json
import websockets
from typing import Dict, Any, Optional, Callable, List, Coroutine
from collections import defaultdict
from utils.logger import get_logger
from utils.config import Settings
from database.db_manager import DBManager
from signals.signal_aggregator import AdvancedSignalAggregator
from api.server import webhook_queue # Import the shared queue for webhooks

logger = get_logger(__name__)

class AdvancedWhaleWatcher:
    """
    The core on-chain event processor. Listens for and processes real-time 
    blockchain events to detect significant whale and protocol activity.
    """
    def __init__(self, config: Settings, db: DBManager, signal_aggregator: AdvancedSignalAggregator, telegram_bot: Any):
        self.config = config
        self.db = db
        self.signal_aggregator = signal_aggregator
        self.telegram_bot = telegram_bot
        self.alchemy_ws_url = config.ALCHEMY_WEBSOCKET_URL
        
        # A dictionary of protocol contract addresses to monitor in the mempool.
        self.tracked_protocols = {
            "0x7a250d5630b4cf539739df2c5dacb4c659f2488d": "Uniswap V2 Router",
            # ... other major DEX routers, lending protocols, etc.
        }
        
        # ADVANCED: Event listener registry for other modules to hook into.
        # This allows for a clean, decoupled architecture.
        self.event_listeners: Dict[str, List[Callable[[Dict], Coroutine]]] = defaultdict(list)

    def register_event_listener(self, event_type: str, callback: Callable[[Dict], Coroutine]):
        """
        Public method for other modules to register a callback for a specific event type.
        Example: FirstMoverDetector can register for 'PairCreated' events.
        """
        self.event_listeners[event_type].append(callback)
        logger.info(f"Registered a new listener for event type: '{event_type}'")

    async def _dispatch_event(self, event_type: str, data: Dict[str, Any]):
        """Dispatches an event to all registered listeners for that event type."""
        if event_type in self.event_listeners:
            logger.debug(f"Dispatching event '{event_type}' to {len(self.event_listeners[event_type])} listener(s).")
            # Run all listener callbacks concurrently
            await asyncio.gather(*(callback(data) for callback in self.event_listeners[event_type]))

    async def run_loop(self):
        """The main entry point to start all watcher tasks concurrently."""
        logger.info("🐋 Whale Watcher (Upgraded) is starting...")
        mempool_task = asyncio.create_task(self.listen_to_mempool(), name="MempoolListener")
        callback_task = asyncio.create_task(self.process_callback_queue(), name="ShyftCallbackProcessor")
        await asyncio.gather(mempool_task, callback_task)

    async def listen_to_mempool(self):
        """
        Connects to Alchemy's websocket and listens for pending transactions,
        providing a pre-confirmation "heads-up" on significant on-chain activity.
        """
        logger.info(f"Connecting to Alchemy mempool websocket...")
        payload = {
            "jsonrpc": "2.0", "id": 1, "method": "eth_subscribe",
            "params": ["alchemy_pendingTransactions", {"toAddress": list(self.tracked_protocols.keys())}]
        }
        while True:
            try:
                async with websockets.connect(self.alchemy_ws_url, ping_interval=60, ping_timeout=120) as websocket:
                    await websocket.send(json.dumps(payload))
                    await websocket.recv() # Consume subscription confirmation
                    logger.info("✅ Successfully subscribed to Alchemy mempool stream.")
                    
                    while True:
                        message = await websocket.recv()
                        tx = json.loads(message)['params']['result']
                        protocol_name = self.tracked_protocols.get(tx.get('to', '').lower())
                        if protocol_name:
                            logger.info(f"🚀 Mempool Hit! Activity at {protocol_name} (tx: {tx.get('hash')[:12]}...).")
                            # This is a lightweight "heads-up". Full analysis happens on the confirmed tx.
            except (websockets.ConnectionClosed, asyncio.TimeoutError) as e:
                logger.warning(f"Websocket connection lost: {e}. Reconnecting in 5 seconds...")
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"Mempool listener encountered a critical error: {e}. Retrying...", exc_info=True)
                await asyncio.sleep(15)

    async def process_callback_queue(self):
        """
        Consumes and processes confirmed transaction data received from the Shyft webhook.
        """
        logger.info("Shyft callback processor is running and waiting for data.")
        while True:
            try:
                item = await webhook_queue.get()
                if item.get("source") == "shyft":
                    parsed_signals = self._parse_shyft_payload(item['payload'])
                    if parsed_signals:
                        # Submit all generated signals concurrently
                        await asyncio.gather(*(self.signal_aggregator.submit_signal(s) for s in parsed_signals))
                webhook_queue.task_done()
            except asyncio.CancelledError:
                logger.info("Callback processor task cancelled.")
                break
            except Exception as e:
                logger.error(f"Error processing item from webhook queue: {e}", exc_info=True)

    def _parse_shyft_payload(self, payload: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Parses the detailed JSON payload from Shyft into one or more standardized signals.
        This is where the raw, confirmed data is turned into structured intelligence.
        """
        signals = []
        try:
            if not isinstance(payload, list) or not payload: return []
            tx_details = payload[0]
            
            for action in tx_details.get('actions', []):
                action_type = action.get('type')
                info = action.get('info', {})
                
                # Example 1: A large token swap
                if action_type == 'TOKEN_SWAP' and info.get('amount_in_usd', 0) > 50000:
                    signals.append({
                        "type": "WHALE_TRADE",
                        "asset": info['token_in']['symbol'],
                        "strength": min(info['amount_in_usd'] / 100000, 0.9),
                        "direction": "bearish", # Selling token_in
                        "metadata": { "tx_hash": tx_details.get('transaction_hash'), "wallet": info['swapper'], "amount_usd": info['amount_in_usd'] }
                    })
                
                # Example 2: A Uniswap V2 PairCreated event for the FirstMoverDetector
                elif action_type == 'CREATE_POOL' and 'Uniswap' in info.get('protocol', ''):
                    asyncio.create_task(self._dispatch_event("PairCreated", {
                        "transaction_hash": tx_details.get('transaction_hash'),
                        "pair_address": info.get('pool_address'),
                        "token0": info.get('token0', {}).get('symbol'),
                        "token1": info.get('token1', {}).get('symbol'),
                    }))
            return signals
        except Exception as e:
            logger.error(f"Failed to parse Shyft payload: {e}", exc_info=True)
            return []
