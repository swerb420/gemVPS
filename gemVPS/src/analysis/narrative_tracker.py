# src/analysis/narrative_tracker.py
import asyncio
import aiohttp
from typing import Dict, Any, Optional
from collections import defaultdict
from utils.logger import get_logger
from utils.config import Settings
from signals.signal_aggregator import AdvancedSignalAggregator

logger = get_logger(__name__)

# ADVANCED: Multi-chain configuration for fetching trending tokens
DEXSCREENER_ENDPOINTS = {
    "solana": "https://api.dexscreener.com/api/v6/pairs/solana/trending",
    "ethereum": "https://api.dexscreener.com/api/v6/pairs/eth/trending",
    "base": "https://api.dexscreener.com/api/v6/pairs/base/trending",
}

# Expanded keyword dictionary for more accurate classification
NARRATIVE_KEYWORDS = {
    "AI": ["ai", "gpt", "artific", "intelligence", "claude", "render", "fetch", "singularity"],
    "MEME": ["pepe", "dog", "cat", "shib", "wojak", "bobo", "doge", "bonk", "mog", "wif"],
    "GAMEFI": ["game", "play", "metaverse", "nft", "gaming", "axie", "gala", "sandbox"],
    "RWA": ["real", "world", "asset", "tokenized", "ondo", "polytrade", "blackrock"],
    "POLIFI": ["trump", "biden", "boden", "tremp", "usa", "maga", "jeo"],
    "DEPIN": ["depin", "decentralized", "physical", "infrastructure", "hivemapper", "helium", "iot"]
}

class NarrativeTracker:
    """
    Identifies emerging on-chain narratives by analyzing the momentum of top
    trending tokens across multiple DEXs and blockchains. This upgraded version
    uses a sophisticated scoring model for higher-quality signals.
    """
    def __init__(self, config: Settings, signal_aggregator: AdvancedSignalAggregator):
        self.signal_aggregator = signal_aggregator
        self.session: Optional[aiohttp.ClientSession] = None
        
        # State to track the dominant narrative over time
        self.dominant_narrative_history = []
        self.last_dominant_narrative: Optional[str] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session

    def _classify_narrative(self, name: str, symbol: str) -> Optional[str]:
        """Classifies a token into a predefined narrative."""
        text_to_search = f"{name} {symbol}".lower()
        for narrative, keywords in NARRATIVE_KEYWORDS.items():
            if any(keyword in text_to_search for keyword in keywords):
                return narrative
        return None

    def _calculate_momentum_score(self, volume: float, traders: int, price_change: float) -> float:
        """Calculates a weighted score to represent a narrative's true momentum."""
        # Weights can be tuned based on backtesting
        score = (volume * 0.5) + (traders * 0.3) + (price_change * 0.2)
        return score

    async def _scan_chain(self, chain: str, url: str) -> Dict[str, Dict[str, float]]:
        """Scans a single chain's trending endpoint and aggregates narrative data."""
        momentum_data = defaultdict(lambda: {"volume": 0.0, "traders": 0, "price_change_sum": 0.0, "pairs": 0})
        try:
            session = await self._get_session()
            async with session.get(url) as response:
                response.raise_for_status()
                data = await response.json()
                
                for pair in data.get('pairs', [])[:25]: # Analyze top 25 trending
                    narrative = self._classify_narrative(
                        pair.get('baseToken', {}).get('name', ''),
                        pair.get('baseToken', {}).get('symbol', '')
                    )
                    if narrative:
                        txns = pair.get('txns', {}).get('h24', {})
                        traders = txns.get('buys', 0) + txns.get('sells', 0)
                        volume = float(pair.get('volume', {}).get('h24', 0))
                        price_change = float(pair.get('priceChange', {}).get('h24', 0))

                        momentum_data[narrative]['volume'] += volume
                        momentum_data[narrative]['traders'] += traders
                        momentum_data[narrative]['price_change_sum'] += price_change
                        momentum_data[narrative]['pairs'] += 1
        except Exception as e:
            logger.error(f"Failed to scan narrative trends on {chain}: {e}", exc_info=True)
        return momentum_data

    async def run_loop(self):
        """The main loop for the NarrativeTracker."""
        logger.info("📈 Narrative & Meme Momentum Engine (Upgraded) is starting...")
        while True:
            try:
                # Concurrently scan all configured chains
                chain_scan_tasks = [self._scan_chain(chain, url) for chain, url in DEXSCREENER_ENDPOINTS.items()]
                results = await asyncio.gather(*chain_scan_tasks)

                # Aggregate results from all chains
                total_momentum = defaultdict(lambda: {"volume": 0.0, "traders": 0, "price_change_sum": 0.0, "pairs": 0})
                for chain_result in results:
                    for narrative, data in chain_result.items():
                        total_momentum[narrative]['volume'] += data['volume']
                        total_momentum[narrative]['traders'] += data['traders']
                        total_momentum[narrative]['price_change_sum'] += data['price_change_sum']
                        total_momentum[narrative]['pairs'] += data['pairs']
                
                if not total_momentum:
                    await asyncio.sleep(600)
                    continue

                # Calculate momentum score for each narrative
                scored_narratives = {}
                for narrative, data in total_momentum.items():
                    avg_price_change = data['price_change_sum'] / data['pairs'] if data['pairs'] > 0 else 0
                    score = self._calculate_momentum_score(data['volume'], data['traders'], avg_price_change)
                    scored_narratives[narrative] = score

                dominant_narrative = max(scored_narratives, key=scored_narratives.get)
                
                # Update trend history
                if dominant_narrative == self.last_dominant_narrative:
                    self.dominant_narrative_history.append(dominant_narrative)
                else:
                    self.dominant_narrative_history = [dominant_narrative] # Reset on change
                self.last_dominant_narrative = dominant_narrative
                trend_duration_minutes = len(self.dominant_narrative_history) * 10

                # Generate signal
                dominant_data = total_momentum[dominant_narrative]
                if dominant_data['volume'] > 250000: # Higher threshold for multi-chain volume
                    logger.info(f"Dominant Narrative Detected: {dominant_narrative} (Trend Duration: {trend_duration_minutes}m)")
                    signal = {
                        "type": "NARRATIVE_ROTATION", "asset": "MARKET_WIDE",
                        "strength": 0.75, "direction": "bullish_for_narrative",
                        "metadata": {
                            "dominant_narrative": dominant_narrative,
                            "aggregate_volume": f"${dominant_data['volume']:,.0f}",
                            "total_traders": dominant_data['traders'],
                            "trend_duration_minutes": trend_duration_minutes
                        }
                    }
                    await self.signal_aggregator.submit_signal(signal)

            except asyncio.CancelledError:
                logger.info("Narrative Tracker loop cancelled.")
                if self.session: await self.session.close()
                break
            except Exception as e:
                logger.error(f"An error occurred in the Narrative Tracker loop: {e}", exc_info=True)
            
            await asyncio.sleep(600)
