# src/market_data/stablecoin_monitor.py
import asyncio
import aiohttp
from web3 import Web3
from typing import Dict, Any, Optional
from utils.logger import get_logger
from utils.config import Settings
from signals.signal_aggregator import AdvancedSignalAggregator

logger = get_logger(__name__)

# ADVANCED: Multi-source configuration for price and supply data
STABLECOIN_CONFIG = {
    "USDC": {
        "coingecko_id": "usd-coin",
        "contract_address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "decimals": 6,
        "chainlink_oracle": "0x8fFfFfd4AfB6115b954Bd326cbe7B4BA576818f6", # USDC/USD
        "depeg_threshold": 0.995
    },
    "USDT": {
        "coingecko_id": "tether",
        "contract_address": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
        "decimals": 6,
        "chainlink_oracle": "0x3E7d1eAB13ad0104d2750B8863b489D65364e32D", # USDT/USD
        "depeg_threshold": 0.995
    }
}

CHAINLINK_ABI = '[{"inputs":[],"name":"latestRoundData","outputs":[{"internalType":"uint80","name":"roundId","type":"uint80"},{"internalType":"int256","name":"answer","type":"int256"},{"internalType":"uint256","name":"startedAt","type":"uint256"},{"internalType":"uint256","name":"updatedAt","type":"uint256"},{"internalType":"uint80","name":"answeredInRound","type":"uint80"}],"stateMutability":"view","type":"function"}]'
ERC20_TOTALSUPPLY_ABI = '[{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"}]'


class StablecoinMonitor:
    """
    Monitors the health and supply of critical stablecoins using multi-source
    data for enhanced reliability. Provides a high-level, market-wide risk assessment.
    """
    def __init__(self, config: Settings, signal_aggregator: AdvancedSignalAggregator):
        self.signal_aggregator = signal_aggregator
        self.w3 = Web3(Web3.HTTPProvider(str(config.WEB3_PROVIDER_URL)))
        self.session: Optional[aiohttp.ClientSession] = None
        self.last_alert_times: Dict[str, float] = {}
        self.last_known_supply: Dict[str, int] = {}

    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session

    async def _get_coingecko_price(self, coingecko_id: str) -> Optional[float]:
        """Fetches price from CoinGecko API."""
        url = f"https://api.coingecko.com/api/v3/simple/price?ids={coingecko_id}&vs_currencies=usd"
        try:
            session = await self._get_session()
            async with session.get(url) as response:
                response.raise_for_status()
                data = await response.json()
                return data.get(coingecko_id, {}).get('usd')
        except Exception as e:
            logger.warning(f"Could not fetch CoinGecko price for {coingecko_id}: {e}")
            return None

    def _get_chainlink_price(self, oracle_address: str) -> Optional[float]:
        """Fetches price directly from a Chainlink on-chain price oracle."""
        try:
            oracle_contract = self.w3.eth.contract(address=Web3.to_checksum_address(oracle_address), abi=CHAINLINK_ABI)
            latest_data = oracle_contract.functions.latestRoundData().call()
            # The price is returned with 8 decimals
            return latest_data[1] / (10**8)
        except Exception as e:
            logger.warning(f"Could not fetch Chainlink price for oracle {oracle_address}: {e}")
            return None

    async def _check_peg_health(self, symbol: str, params: Dict):
        """Checks peg health using multiple sources for confirmation."""
        cg_price = await self._get_coingecko_price(params['coingecko_id'])
        cl_price = self._get_chainlink_price(params['chainlink_oracle'])

        prices = [p for p in [cg_price, cl_price] if p is not None]
        if not prices:
            logger.warning(f"Could not retrieve any price for {symbol}.")
            return

        # Check if the average price is below the threshold
        avg_price = sum(prices) / len(prices)
        if avg_price < params['depeg_threshold']:
            current_time = asyncio.get_event_loop().time()
            if current_time - self.last_alert_times.get(f"{symbol}_depeg", 0) > 3600: # 1-hour cooldown
                self.last_alert_times[f"{symbol}_depeg"] = current_time
                logger.critical(f"🚨 STABLECOIN DE-PEG CONFIRMED: {symbol} at ${avg_price:.4f}")
                
                signal = {
                    "type": "STABLECOIN_DEPEG_RISK", "asset": "MARKET_WIDE",
                    "strength": 0.9, "direction": "risk-off",
                    "metadata": {
                        "stablecoin": symbol, "average_price": f"${avg_price:.4f}",
                        "confirmed_by_sources": len(prices),
                        "message": "Confirmed de-peg detected, potential market instability."
                    }
                }
                await self.signal_aggregator.submit_signal(signal)

    async def _check_supply_changes(self, symbol: str, params: Dict):
        """Checks on-chain total supply for a stablecoin."""
        try:
            contract = self.w3.eth.contract(address=Web3.to_checksum_address(params['contract_address']), abi=ERC20_TOTALSUPPLY_ABI)
            total_supply = contract.functions.totalSupply().call()
            
            last_supply = self.last_known_supply.get(symbol)
            self.last_known_supply[symbol] = total_supply

            if last_supply:
                net_change = (total_supply - last_supply) / (10**params['decimals'])
                if abs(net_change) > 500_000_000: # Signal on > $500M net change
                    direction = "INFLOW" if net_change > 0 else "OUTFLOW"
                    logger.info(f"Capital {direction}: ${abs(net_change):,.0f} net change in {symbol} supply.")
                    signal = {
                        "type": f"CAPITAL_{direction}", "asset": "MARKET_WIDE",
                        "strength": 0.7, "direction": "bullish" if direction == "INFLOW" else "bearish",
                        "metadata": {"stablecoin": symbol, "net_change_usd": f"${net_change:,.0f}"}
                    }
                    await self.signal_aggregator.submit_signal(signal)
        except Exception as e:
            logger.error(f"Failed to check supply for {symbol}: {e}", exc_info=True)

    async def run_loop(self):
        """The main loop for the StablecoinMonitor."""
        logger.info("🪙 Stablecoin Health Monitor (Upgraded) is starting...")
        loop_count = 0
        while True:
            try:
                tasks = []
                for symbol, params in STABLECOIN_CONFIG.items():
                    # Check peg health frequently
                    tasks.append(self._check_peg_health(symbol, params))
                    # Check supply changes less frequently
                    if loop_count % 20 == 0:
                        tasks.append(self._check_supply_changes(symbol, params))
                
                await asyncio.gather(*tasks)
                loop_count += 1
            except asyncio.CancelledError:
                logger.info("Stablecoin Monitor loop cancelled.")
                if self.session: await self.session.close()
                break
            except Exception as e:
                logger.error(f"An error occurred in the Stablecoin Monitor main loop: {e}", exc_info=True)
            
            await asyncio.sleep(180)
