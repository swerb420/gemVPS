# src/onchain/dex_analyzer.py
import asyncio
import aiohttp
from typing import Dict, Any, Optional
from utils.logger import get_logger
from utils.config import Settings
from signals.signal_aggregator import AdvancedSignalAggregator

logger = get_logger(__name__)

# --- ADVANCED DATA SOURCES ---
# A more structured and extensive list of data sources, supporting multiple chains and DEXs.
# This structure makes it easy to add new chains or DEXs in the future.
DEX_ENDPOINTS = {
    "ethereum": {
        "uniswap_v3": "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3",
    },
    "arbitrum": {
        "uniswap_v3": "https://api.thegraph.com/subgraphs/name/ianlapham/arbitrum-dev",
    },
    "polygon": {
        "uniswap_v3": "https://api.thegraph.com/subgraphs/name/ianlapham/uniswap-v3-polygon",
    }
}

TRACKED_POOLS = {
    "ethereum_uniswap_v3": {
        "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640": "WETH/USDC 0.05%",
        "0x8ad599c3a061a0c9f842735779e087221b13db47": "WBTC/WETH 0.3%",
        "0x3416cf6c708da44db2624d63ea0aaef7113527c6": "USDT/WETH 0.3%",
        "0x11b815efb8f581194ae79006d24e0d814b7697f6": "LINK/WETH 0.3%",
    },
    "arbitrum_uniswap_v3": {
        "0xc31e54c7a869b9fcbecc14363cf510d1c41fa441": "WETH/USDC 0.05%",
        "0x831a1f0851a74355675e324155105a28a384b172": "ARB/WETH 0.3%",
    }
    # To expand, simply add new keys like "bsc_pancakeswap" and their pools.
}

class DEXAnalyzer:
    """
    Monitors significant liquidity and volume changes in key DEX pools across
    multiple chains. This upgraded version provides a broader market view and
    generates more nuanced signals based on richer data.
    """
    def __init__(self, config: Settings, signal_aggregator: AdvancedSignalAggregator):
        self.signal_aggregator = signal_aggregator
        self.session: Optional[aiohttp.ClientSession] = None
        
        # A dictionary to store the last known state (liquidity, volume) for each pool.
        # The key is a unique identifier like 'chain_dex_poolId' for multi-chain support.
        self.pool_state: Dict[str, Dict[str, float]] = {}

    async def _get_session(self) -> aiohttp.ClientSession:
        """Initializes and returns a persistent aiohttp session."""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session

    async def _query_pool_data(self, endpoint_url: str, pool_id: str) -> Optional[Dict[str, Any]]:
        """
        Queries a GraphQL endpoint to get richer data for a specific pool,
        including TVL, volume, and transaction count for more nuanced signals.
        """
        # ADVANCED QUERY: Fetches more than just TVL for better signal generation.
        query = """
        query getPoolData($poolId: ID!) {
            pool(id: $poolId) {
                totalValueLockedUSD
                volumeUSD
                txCount
            }
        }
        """
        variables = {"poolId": pool_id}
        try:
            session = await self._get_session()
            async with session.post(endpoint_url, json={'query': query, 'variables': variables}) as response:
                response.raise_for_status()
                data = await response.json()
                if data.get('data') and data['data'].get('pool'):
                    pool_data = data['data']['pool']
                    # Convert all relevant fields to float for consistent calculations
                    return {
                        "tvl": float(pool_data.get('totalValueLockedUSD', 0)),
                        "volume": float(pool_data.get('volumeUSD', 0)),
                        "tx_count": int(pool_data.get('txCount', 0))
                    }
                else:
                    logger.warning(f"No data returned for pool {pool_id} from {endpoint_url}. Response: {data}")
                    return None
        except Exception as e:
            logger.error(f"Error querying GraphQL endpoint {endpoint_url} for pool {pool_id}: {e}", exc_info=True)
            return None

    def _generate_signal_from_changes(self, pool_name: str, prev_state: Dict, current_state: Dict) -> Optional[Dict]:
        """Analyzes changes in pool state and generates a signal if significant."""
        # Avoid division by zero if a pool is new or had zero liquidity/volume
        if prev_state['tvl'] > 0:
            tvl_change_pct = ((current_state['tvl'] - prev_state['tvl']) / prev_state['tvl']) * 100
        else:
            tvl_change_pct = 0.0
        
        if prev_state['volume'] > 0:
            volume_change_pct = ((current_state['volume'] - prev_state['volume']) / prev_state['volume']) * 100
        else:
            volume_change_pct = 0.0

        # ADVANCED SIGNAL LOGIC: Trigger on a large TVL change OR a moderate TVL change confirmed by a volume surge.
        if abs(tvl_change_pct) > 7.5 or (abs(tvl_change_pct) > 3 and volume_change_pct > 50):
            direction = "INFLOW" if tvl_change_pct > 0 else "OUTFLOW"
            asset = pool_name.split('/')[0] # Use the base asset of the pair for the signal
            
            logger.info(
                f"Significant DEX activity in {pool_name}: "
                f"TVL Change: {tvl_change_pct:+.2f}%, Volume Change: {volume_change_pct:+.2f}%"
            )
            
            return {
                "type": "LIQUIDITY_FLOW",
                "asset": asset,
                "strength": min(abs(tvl_change_pct) / 10, 0.8), # Normalize strength
                "direction": "bullish" if direction == "INFLOW" else "bearish",
                "metadata": {
                    "pool_name": pool_name,
                    "tvl_change_pct": f"{tvl_change_pct:.2f}%",
                    "volume_change_pct": f"{volume_change_pct:.2f}%",
                    "current_tvl_usd": f"${current_state['tvl']:,.0f}",
                    "message": f"Significant liquidity {direction.lower()} detected with volume confirmation."
                }
            }
        return None

    async def run_loop(self):
        """
        The main loop for the DEXAnalyzer. It now iterates through multiple
        chains and DEXs to provide a comprehensive market view.
        """
        logger.info("💧 DEX Liquidity Analyzer (Multi-Chain) is starting...")
        while True:
            signals_to_send = []
            try:
                # Iterate through each chain and DEX defined in the data sources
                for key, pools in TRACKED_POOLS.items():
                    chain, dex = key.split('_')
                    endpoint = DEX_ENDPOINTS.get(chain, {}).get(dex)
                    if not endpoint:
                        continue
                    
                    logger.debug(f"Scanning {len(pools)} pools on {chain.capitalize()} {dex.capitalize()}...")
                    for pool_id, pool_name in pools.items():
                        current_state = await self._query_pool_data(endpoint, pool_id)
                        if current_state is None:
                            continue

                        state_key = f"{key}_{pool_id}"
                        previous_state = self.pool_state.get(state_key)
                        
                        self.pool_state[state_key] = current_state

                        if previous_state:
                            signal = self._generate_signal_from_changes(pool_name, previous_state, current_state)
                            if signal:
                                signals_to_send.append(self.signal_aggregator.submit_signal(signal))
                        
                        await asyncio.sleep(5) # Stagger API calls to TheGraph to be a good citizen

                if signals_to_send:
                    await asyncio.gather(*signals_to_send)

            except asyncio.CancelledError:
                logger.info("DEX Liquidity Analyzer loop cancelled.")
                if self.session: await self.session.close()
                break
            except Exception as e:
                logger.error(f"An error occurred in the DEX Analyzer main loop: {e}", exc_info=True)
            
            # Re-run the full analysis every 30 minutes for a faster, more responsive update cycle.
            await asyncio.sleep(1800)
