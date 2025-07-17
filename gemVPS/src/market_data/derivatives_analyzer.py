# src/market_data/derivatives_analyzer.py
import asyncio
import aiohttp
from typing import Dict, Any
from utils.logger import get_logger
from utils.config import Settings
from signals.signal_aggregator import AdvancedSignalAggregator

logger = get_logger(__name__)

class DerivativesAnalyzer:
    """
    Analyzes the crypto derivatives market (primarily options on Deribit)
    to gauge institutional sentiment and forward-looking volatility expectations.
    """
    def __init__(self, config: Settings, signal_aggregator: AdvancedSignalAggregator):
        self.signal_aggregator = signal_aggregator
        self.deribit_base_url = "https://www.deribit.com/api/v2/public"
        self.assets_to_track = ["BTC", "ETH"]
        self.session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        """Initializes and returns a persistent aiohttp session."""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session

    async def _fetch_market_summary(self, currency: str) -> Dict[str, Any] | None:
        """
        Fetches a summary of the options market for a given currency from Deribit.
        This includes open interest, volume, and implied volatility.
        """
        url = f"{self.deribit_base_url}/get_book_summary_by_currency?currency={currency}&kind=option"
        try:
            session = await self._get_session()
            async with session.get(url) as response:
                response.raise_for_status()
                data = await response.json()
                
                if not data.get('result'):
                    return None

                # Aggregate the key metrics from the detailed response
                summary = {
                    "open_interest_usd": sum(item['open_interest'] * item.get('underlying_price', 0) for item in data['result']),
                    "put_volume_24h": sum(item['volume'] for item in data['result'] if 'P' in item['instrument_name']),
                    "call_volume_24h": sum(item['volume'] for item in data['result'] if 'C' in item['instrument_name']),
                    "iv_avg": sum(item['mark_iv'] for item in data['result']) / len(data['result']) if data['result'] else 0,
                }
                
                # Calculate the Put/Call Ratio, a classic sentiment indicator
                if summary["call_volume_24h"] > 0:
                    summary["put_call_ratio"] = summary["put_volume_24h"] / summary["call_volume_24h"]
                else:
                    summary["put_call_ratio"] = 0 # Avoid division by zero
                
                return summary
        except Exception as e:
            logger.error(f"Error fetching Deribit data for {currency}: {e}", exc_info=True)
            return None

    async def _analyze_and_generate_signals(self, asset: str, summary: Dict[str, Any]):
        """
        Analyzes the market summary and generates signals based on predefined heuristics.
        """
        pcr = summary['put_call_ratio']
        iv = summary['iv_avg']
        
        # Heuristic 1: High Put/Call Ratio suggests bearish sentiment or high demand for downside protection.
        if pcr > 1.0:
            logger.info(f"Derivatives signal: High Put/Call Ratio for {asset} ({pcr:.2f}) indicates bearish sentiment.")
            signal = {
                "type": "DERIVATIVES_FEAR",
                "asset": asset,
                "strength": min(pcr / 1.5, 0.8), # Normalize strength
                "direction": "bearish",
                "metadata": {"put_call_ratio": f"{pcr:.2f}", "message": "High demand for puts."}
            }
            await self.signal_aggregator.submit_signal(signal)
            
        # Heuristic 2: Low Put/Call Ratio suggests bullish sentiment and high demand for calls.
        elif pcr < 0.6 and pcr > 0:
            logger.info(f"Derivatives signal: Low Put/Call Ratio for {asset} ({pcr:.2f}) indicates bullish sentiment.")
            signal = {
                "type": "DERIVATIVES_GREED",
                "asset": asset,
                "strength": min(1 - pcr, 0.8),
                "direction": "bullish",
                "metadata": {"put_call_ratio": f"{pcr:.2f}", "message": "High demand for calls."}
            }
            await self.signal_aggregator.submit_signal(signal)

        # Heuristic 3: High Implied Volatility suggests market expects large price swings.
        if iv > 85:
            logger.info(f"Derivatives signal: High Implied Volatility for {asset} ({iv:.1f}%) suggests upcoming volatility.")
            signal = {
                "type": "VOLATILITY_EXPANSION_EXPECTED",
                "asset": asset,
                "strength": min(iv / 100, 0.7),
                "direction": "neutral_volatile",
                "metadata": {"implied_volatility": f"{iv:.1f}%", "message": "Market is pricing in large price swings."}
            }
            await self.signal_aggregator.submit_signal(signal)

    async def run_loop(self):
        """
        The main loop for the DerivativesAnalyzer. It periodically fetches and
        analyzes the options market for each tracked asset.
        """
        logger.info("📈 Derivatives Market Analyzer is starting...")
        while True:
            try:
                for asset in self.assets_to_track:
                    market_summary = await self._fetch_market_summary(asset)
                    if market_summary:
                        await self._analyze_and_generate_signals(asset, market_summary)
                    await asyncio.sleep(10) # Stagger requests to the API
            except asyncio.CancelledError:
                logger.info("Derivatives Analyzer loop cancelled.")
                if self.session: await self.session.close()
                break
            except Exception as e:
                logger.error(f"An error occurred in the Derivatives Analyzer loop: {e}", exc_info=True)
            
            # Analyze the derivatives market every 15 minutes.
            await asyncio.sleep(900)
