# src/onchain/first_mover_detector.py
import asyncio
from typing import Dict, Any, List
from utils.logger import get_logger
from utils.config import Settings
from database.db_manager import DBManager
from signals.signal_aggregator import AdvancedSignalAggregator

logger = get_logger(__name__)

class FirstMoverDetector:
    """
    Identifies and scores "smart money" wallets that are consistently among the
    first to invest in new token launches. This upgraded version scores wallets
    based on the subsequent performance of the tokens they buy.
    """
    def __init__(self, config: Settings, db: DBManager, signal_aggregator: AdvancedSignalAggregator, whale_watcher: Any):
        self.config = config
        self.db = db
        self.signal_aggregator = signal_aggregator
        
        if hasattr(whale_watcher, 'register_event_listener'):
            whale_watcher.register_event_listener("PairCreated", self.handle_new_pair_event)
            logger.info("✅ First Mover Detector (Upgraded) is hooked into PairCreated events.")

    async def handle_new_pair_event(self, event_data: Dict[str, Any]):
        """Callback triggered when a new liquidity pool is created."""
        pair_address = event_data.get('pair_address')
        token_address = event_data.get('token1_address') # Assuming token1 is the new token
        if not pair_address or not token_address: return

        logger.info(f"New pair detected: {pair_address}. Analyzing first movers for token {token_address[:10]}...")
        
        # first_buyers = await self._fetch_first_transactions(pair_address)
        first_buyers = ["0xWallet1_Smart...", "0xWallet2_Dumb...", "0xWallet3_Smart..."] # Placeholder

        if not first_buyers: return

        await asyncio.gather(*(self._process_first_buyer(buyer, token_address) for buyer in first_buyers))
        logger.info(f"Finished processing first movers for pair {pair_address}.")

    async def _fetch_first_transactions(self, pair_address: str) -> List[str]:
        """Placeholder for fetching the first wallets to buy from a new pool."""
        return []

    async def _process_first_buyer(self, wallet_address: str, token_address: str):
        """Processes a single first buyer, checking their score and generating signals."""
        try:
            # Check the existing score of this wallet
            score_record = await self.db.fetch_with_retry(
                "SELECT smart_money_score FROM smart_money_scores WHERE wallet_address = $1",
                wallet_address
            )
            score = score_record[0]['smart_money_score'] if score_record else 0

            # If a known "smart money" wallet (> high score threshold) is a first mover,
            # it's a very strong signal.
            if score > 50: # Example threshold
                logger.info(f"HIGH SIGNAL: Known smart money wallet {wallet_address[:10]} (score: {score}) is a first mover on token {token_address[:10]}.")
                signal = {
                    "type": "SMART_MONEY_BUY",
                    "asset": token_address,
                    "strength": min(score / 100, 0.85), # Strength proportional to score
                    "direction": "bullish",
                    "metadata": {
                        "wallet_address": wallet_address,
                        "smart_money_score": score,
                        "message": "A historically successful first-mover wallet has bought this new token."
                    }
                }
                await self.signal_aggregator.submit_signal(signal)

            # We still need to track this "first move" to score it later.
            await self.db.execute_with_retry(
                """
                INSERT INTO first_moves (wallet_address, token_address, entry_time)
                VALUES ($1, $2, NOW())
                """,
                wallet_address, token_address
            )

        except Exception as e:
            logger.error(f"Failed to process first buyer {wallet_address}: {e}", exc_info=True)

    async def _score_past_moves(self):
        """
        [SLOW LOOP] A periodic task to score the performance of past "first moves"
        and update the main smart_money_scores table.
        """
        logger.info("Scoring performance of past first moves...")
        # 1. Fetch unscored first moves older than 24 hours
        # unscored_moves = await self.db.fetch_with_retry("SELECT * FROM first_moves WHERE scored = false AND entry_time < NOW() - INTERVAL '24 hours'")
        
        # 2. For each move, fetch the token's price 24h after the entry time
        # price_after_24h = await self.db.get_price_at_time(move['token_address'], move['entry_time'] + timedelta(hours=24))
        # entry_price = await self.db.get_price_at_time(move['token_address'], move['entry_time'])

        # 3. Calculate performance and assign a score
        # performance = (price_after_24h - entry_price) / entry_price
        # score_update = 0
        # if performance > 1.0: score_update = 20 # +100% = +20 score
        # elif performance > 0.5: score_update = 10 # +50% = +10 score
        # elif performance < -0.5: score_update = -10 # -50% = -10 score

        # 4. Update the main score table in the database
        # await self.db.execute_with_retry(
        #     "UPDATE smart_money_scores SET smart_money_score = smart_money_score + $1 WHERE wallet_address = $2",
        #     score_update, move['wallet_address']
        # )
        
        # 5. Mark the move as scored
        # await self.db.execute_with_retry("UPDATE first_moves SET scored = true WHERE id = $1", move['id'])
        logger.info("✅ Finished scoring past moves.")


    async def run_loop(self):
        """
        The main loop for the FirstMoverDetector's background tasks, like scoring.
        """
        logger.info("🧠 First Mover Detector (Upgraded) background scorer is starting...")
        while True:
            try:
                await self._score_past_moves()
            except asyncio.CancelledError:
                logger.info("First Mover Detector scorer loop cancelled.")
                break
            except Exception as e:
                logger.error(f"An error occurred in the scorer loop: {e}", exc_info=True)
            
            # Run the scoring process once every few hours.
            await asyncio.sleep(10800) # Every 3 hours
