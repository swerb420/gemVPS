# src/onchain/gas_analyzer.py
import asyncio
import numpy as np
from collections import deque, defaultdict
from web3 import Web3
from typing import Dict, Any
from utils.logger import get_logger
from utils.config import Settings
from signals.signal_aggregator import AdvancedSignalAggregator

logger = get_logger(__name__)

# ADVANCED: Configuration for multi-chain gas monitoring
GAS_MONITOR_CONFIG = {
    "ethereum": {
        "rpc_url": "WEB3_PROVIDER_URL", # Will be fetched from main config
        "anomaly_threshold_sigma": 3.0, # Standard threshold for a mature chain
        "sample_interval_seconds": 30,
    },
    "polygon": {
        "rpc_url": "POLYGON_RPC_URL", # Assumes this exists in your .env
        "anomaly_threshold_sigma": 3.5, # Higher threshold for a chain with more volatile gas
        "sample_interval_seconds": 60,
    }
}

class GasAnalyzer:
    """
    Monitors gas prices in real-time across multiple EVM-compatible chains to
    detect anomalous spikes, which can be powerful leading indicators of
    significant on-chain events and market-wide volatility.
    """
    def __init__(self, config: Settings, signal_aggregator: AdvancedSignalAggregator):
        self.signal_aggregator = signal_aggregator
        self.config = config
        
        # A dictionary to hold Web3 instances for each configured chain
        self.web3_instances: Dict[str, Web3] = {}
        # A dictionary to hold the historical gas price data for each chain
        self.gas_price_histories: Dict[str, deque] = defaultdict(lambda: deque(maxlen=120))
        self.last_anomaly_times: Dict[str, float] = defaultdict(float)

        self._initialize_web3_instances()

    def _initialize_web3_instances(self):
        """Initializes Web3 instances for all chains defined in the config."""
        for chain, params in GAS_MONITOR_CONFIG.items():
            # Get the RPC URL from the main config object.
            rpc_url_key = params["rpc_url"]

            if chain == "polygon":
                rpc_url = getattr(self.config, "POLYGON_RPC_URL", None)
                if not rpc_url:
                    logger.warning(
                        "POLYGON_RPC_URL not provided. Polygon monitoring will be disabled."
                    )
                    continue
            else:
                rpc_url = getattr(self.config, rpc_url_key, None)
            
            if rpc_url:
                try:
                    w3 = Web3(Web3.HTTPProvider(str(rpc_url)))
                    if w3.is_connected():
                        self.web3_instances[chain] = w3
                        logger.info(f"✅ Initialized Web3 provider for {chain.capitalize()}.")
                    else:
                        logger.error(f"❌ Failed to connect to Web3 provider for {chain.capitalize()}.")
                except Exception as e:
                    logger.error(f"Error initializing Web3 for {chain.capitalize()}: {e}")
            else:
                logger.warning(f"RPC URL '{rpc_url_key}' not found in config for {chain.capitalize()}. Skipping.")

    async def _monitor_chain(self, chain: str):
        """The core monitoring logic for a single blockchain."""
        w3 = self.web3_instances[chain]
        params = GAS_MONITOR_CONFIG[chain]
        
        while True:
            try:
                gas_price_wei = w3.eth.gas_price
                gas_price_gwei = float(w3.from_wei(gas_price_wei, 'gwei'))
                
                history = self.gas_price_histories[chain]
                history.append(gas_price_gwei)

                if len(history) > 30: # Wait for enough data to build a baseline
                    await self._analyze_for_anomalies(chain, gas_price_gwei)

            except asyncio.CancelledError:
                logger.info(f"Gas Analyzer for {chain.capitalize()} cancelled.")
                break
            except Exception as e:
                logger.error(f"Error in Gas Analyzer for {chain.capitalize()}: {e}", exc_info=True)
            
            await asyncio.sleep(params["sample_interval_seconds"])

    async def _analyze_for_anomalies(self, chain: str, current_gas_gwei: float):
        """Performs a statistical check for anomalies on a specific chain."""
        history = self.gas_price_histories[chain]
        history_np = np.array(history, dtype=float)
        mean = np.mean(history_np)
        std_dev = np.std(history_np)
        threshold_sigma = GAS_MONITOR_CONFIG[chain]["anomaly_threshold_sigma"]

        if std_dev > 0.1 and current_gas_gwei > mean + (threshold_sigma * std_dev):
            current_time = asyncio.get_event_loop().time()
            if current_time - self.last_anomaly_times[chain] > 900: # 15-minute cooldown per chain
                self.last_anomaly_times[chain] = current_time
                
                logger.warning(
                    f"🔥 GAS PRICE ANOMALY on {chain.upper()}: Spike to {current_gas_gwei:.1f} Gwei "
                    f"(Mean: {mean:.1f}, StdDev: {std_dev:.1f})"
                )
                
                signal = {
                    "type": "GAS_PRICE_ANOMALY",
                    "asset": f"{chain.upper()}_NETWORK",
                    "strength": 0.65,
                    "direction": "neutral_investigate",
                    "metadata": {
                        "chain": chain,
                        "current_gwei": f"{current_gas_gwei:.1f}",
                        "mean_gwei": f"{mean:.1f}",
                        "std_dev_gwei": f"{std_dev:.1f}",
                        "sigma_event": f"{(current_gas_gwei - mean) / std_dev:.1f}",
                        "message": "Potential high-impact on-chain event in progress."
                    }
                }
                await self.signal_aggregator.submit_signal(signal)

    async def run_loop(self):
        """
        The main entry point. It creates and runs a separate monitoring task
        for each configured and successfully initialized blockchain.
        """
        if not self.web3_instances:
            logger.error("Gas Analyzer cannot start as no Web3 providers were successfully initialized.")
            return

        logger.info(f"⛽ Gas Price Anomaly Detector (Multi-Chain) is starting for chains: {list(self.web3_instances.keys())}...")
        
        tasks = [self._monitor_chain(chain) for chain in self.web3_instances]
        await asyncio.gather(*tasks)
