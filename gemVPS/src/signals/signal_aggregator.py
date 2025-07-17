# src/signals/signal_aggregator.py
import asyncio
import json
from typing import Dict, Any
from collections import defaultdict
from utils.logger import get_logger
from utils.config import Settings
from database.db_manager import DBManager
# In a full app, you would import your aioredis client here.

logger = get_logger(__name__)

class AdvancedSignalAggregator:
    """
    The central hub for all trading signals. This class receives raw signals
    from various modules, processes them through a multi-stage pipeline,
    and generates a final, high-conviction composite signal.
    """
    def __init__(self, config: Settings, db: DBManager, redis_client: Any):
        self.config = config
        self.db = db
        self.redis = redis_client
        self.signal_queue = asyncio.Queue()

        # Default weights for each signal source. These will be dynamically
        # updated by the WeightOptimizer module.
        self.signal_weights = defaultdict(lambda: 0.1, {
            "WHALE_TRADE": 0.25,
            "CEX_LISTING_ARBITRAGE": 1.0, # Max weight, acts as an override
            "NARRATIVE_ROTATION": 0.15,
            "LIQUIDITY_FLOW": 0.20,
            "DERIVATIVES_FEAR": 0.10,
            "STABLECOIN_DEPEG_RISK": 0.8, # High impact risk-off signal
            "GAS_PRICE_ANOMALY": 0.05,
            # ... other signal types
        })

        # A buffer to hold recent signals for confirmation logic
        self.recent_signals_buffer = defaultdict(list)

    async def submit_signal(self, signal: Dict[str, Any]):
        """
        Public method for any module to submit a raw signal for processing.

        Args:
            signal (Dict[str, Any]): A dictionary representing the signal.
                                     Must include 'type', 'asset', 'strength', 'direction'.
        """
        await self.signal_queue.put(signal)

    async def submit_high_priority_signal(self, signal: Dict[str, Any]):
        """
        Public method for time-critical signals (e.g., CEX listing).
        This bypasses the normal queue and publishes directly to Redis for the
        TradeExecutor to consume instantly.
        """
        if self.redis:
            try:
                await self.redis.publish("high-priority-signals", json.dumps(signal))
                logger.critical(f"Published HIGH PRIORITY signal to Redis: {signal['type']} for {signal['asset']}")
            except Exception as e:
                logger.error(f"Failed to publish high priority signal to Redis: {e}")
        else:
            logger.warning("Redis client not configured. High priority signal cannot be sent.")


    def update_weights(self, new_weights: Dict[str, float]):
        """
        Allows the WeightOptimizer module to update the signal weights dynamically.
        """
        logger.info(f"Dynamically updating signal weights. New weights: {new_weights}")
        self.signal_weights.update(new_weights)

    async def _process_signal(self, signal: Dict[str, Any]):
        """
        The core processing pipeline for a single raw signal.
        """
        logger.info(f"Processing signal: {signal['type']} for {signal['asset']} ({signal['direction']} @ {signal['strength']:.2f})")
        
        # 1. Store the raw signal for backtesting and analysis
        # await self.db.execute_with_retry("INSERT INTO raw_signals ...", ...)

        # 2. Apply dynamic weights
        weighted_strength = signal['strength'] * self.signal_weights.get(signal['type'], 0.1)

        # 3. Check for multi-source confirmation
        # Example: If we get a bullish whale trade, check if we also have
        # bullish liquidity flow signals for the same asset recently.
        is_confirmed = self._check_for_confirmation(signal)
        if is_confirmed:
            weighted_strength *= 1.25 # Boost strength by 25% on confirmation
            logger.info(f"Signal for {signal['asset']} confirmed by other sources. Boosting strength.")

        # 4. Generate or update the composite signal for the asset
        await self._update_composite_signal(signal['asset'], weighted_strength, signal['direction'])

    def _check_for_confirmation(self, signal: Dict[str, Any]) -> bool:
        """
        Checks a buffer of recent signals to see if the new signal is corroborated
        by other sources.
        """
        # This is a simplified example. A real implementation would be more complex.
        asset = signal['asset']
        direction = signal['direction']
        
        # Add current signal to buffer (and keep buffer size limited)
        self.recent_signals_buffer[asset].append(signal)
        self.recent_signals_buffer[asset] = self.recent_signals_buffer[asset][-10:]

        # Check for other recent signals with the same direction
        for recent_signal in self.recent_signals_buffer[asset][:-1]:
            if recent_signal['direction'] == direction:
                return True
        return False

    async def _update_composite_signal(self, asset: str, weighted_strength: float, direction: str):
        """
        Updates the final composite signal score for a given asset.
        This score is what would ultimately be displayed or acted upon.
        """
        # In a real system, this would involve fetching the current composite score
        # from Redis, updating it, and writing it back.
        logger.debug(f"Updating composite signal for {asset}. Contribution: {weighted_strength:.2f} ({direction})")
        # ... logic to update a score in Redis or a database ...

    async def run_aggregator_loop(self):
        """

        The main, perpetual loop that consumes from the signal queue and processes signals.
        """
        logger.info("🚦 Signal Aggregator is running and waiting for signals.")
        while True:
            try:
                # Wait for a signal to arrive from any module
                signal = await self.signal_queue.get()
                
                # Process the signal through the pipeline
                await self._process_signal(signal)
                
                # Mark the task as done
                self.signal_queue.task_done()
            except asyncio.CancelledError:
                logger.info("Signal Aggregator loop cancelled.")
                break
            except Exception as e:
                logger.error(f"An error occurred in the signal aggregator loop: {e}", exc_info=True)
                # Avoid crashing the loop on a single bad signal
                await asyncio.sleep(1)
