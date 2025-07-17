# src/onchain/vc_watcher.py
import asyncio
import aiohttp
from typing import Dict, Any, Set
from collections import defaultdict
from utils.logger import get_logger
from utils.config import Settings
from signals.signal_aggregator import AdvancedSignalAggregator

logger = get_logger(__name__)

# ADVANCED: Multi-chain, curated list of high-signal wallets.
# This structure allows tracking the same fund across different ecosystems.
CURATED_WALLETS = {
    "a16z": {
        "ethereum": "0x...",
        "solana": "..."
    },
    "paradigm": {
        "ethereum": "0x...",
    },
    "dragonfly": {
        "ethereum": "0x...",
        "solana": "..."
    }
    # This list should be populated with ~50-100 verified addresses.
}

# Base URLs for Shyft's multi-chain API
SHYFT_API_URLS = {
    "ethereum": "https://api.shyft.to/sol/v1/wallet/transaction_history", # Placeholder, use correct endpoint
    "solana": "https://api.shyft.to/sol/v1/wallet/transaction_history",
}


class VCWatcher:
    """
    Performs on-chain footprinting of elite VC and 'smart money' wallets
    across multiple blockchains. It detects early investment signals by identifying
    when multiple top-tier funds converge on a new, un-tokenized protocol.
    """
    def __init__(self, config: Settings, signal_aggregator: AdvancedSignalAggregator):
        self.config = config
        self.signal_aggregator = signal_aggregator
        self.shyft_api_key = config.SHYFT_API_KEY.get_secret_value()
        self.headers = {"x-api-key": self.shyft_api_key}
        self.session: aiohttp.ClientSession | None = None
        
        # ADVANCED: State to track which protocols are being touched by which VCs
        # Structure: { "protocol_address": {"vc_name_1", "vc_name_2"} }
        self.protocol_touch_state: Dict[str, Set[str]] = defaultdict(set)
        self.convergence_threshold = 3 # Signal when 3 or more distinct VCs touch a protocol

    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session

    async def _footprint_wallet(self, vc_name: str, chain: str, address: str):
        """
        Fetches and analyzes the recent activity of a single high-signal wallet.
        """
        api_base = SHYFT_API_URLS.get(chain)
        if not api_base: return

        params = {"network": "mainnet-beta" if chain == "solana" else "mainnet", "wallet_address": address, "tx_num": 10}
        try:
            session = await self._get_session()
            async with session.get(api_base, params=params, headers=self.headers) as response:
                if response.status != 200:
                    logger.warning(f"Failed to fetch data for {vc_name} on {chain}. Status: {response.status}")
                    return

                transactions = await response.json()
                for tx in transactions.get('result', []):
                    await self._analyze_transaction(tx, vc_name)

        except Exception as e:
            logger.error(f"Error footprinting {vc_name} on {chain}: {e}", exc_info=True)

    async def _analyze_transaction(self, tx: Dict[str, Any], vc_name: str):
        """
        Analyzes a transaction's actions to find high-signal interactions,
        specifically looking for interactions with new or unknown protocols.
        """
        for action in tx.get('actions', []):
            # We are interested in actions that imply investment or staking in a protocol
            if action['type'] in ['DEPOSIT', 'STAKE_TOKEN', 'ADD_LIQUIDITY']:
                protocol_address = action.get('info', {}).get('protocol_address')
                if protocol_address:
                    # Add the VC to the set of wallets that have touched this protocol
                    self.protocol_touch_state[protocol_address].add(vc_name)
                    logger.debug(f"{vc_name} interacted with protocol {protocol_address[:10]}...")
                    
                    # Check if we've met the convergence threshold
                    if len(self.protocol_touch_state[protocol_address]) >= self.convergence_threshold:
                        await self._generate_convergence_signal(protocol_address)

    async def _generate_convergence_signal(self, protocol_address: str):
        """Generates a high-quality signal when multiple VCs converge on one protocol."""
        converged_vcs = list(self.protocol_touch_state[protocol_address])
        logger.critical(f"VC CONVERGENCE DETECTED on protocol {protocol_address}! Touched by: {converged_vcs}")
        
        signal = {
            "type": "VC_CONVERGENCE",
            "asset": "POTENTIAL_NEW_TOKEN",
            "strength": 0.85, # This is a high-confidence alpha signal
            "direction": "bullish_long_term",
            "metadata": {
                "protocol_address": protocol_address,
                "converged_vcs": converged_vcs,
                "vc_count": len(converged_vcs),
                "message": "Multiple elite funds are interacting with this new protocol. High potential for future token launch."
            }
        }
        await self.signal_aggregator.submit_signal(signal)
        
        # Clear the state for this protocol to avoid sending duplicate alerts immediately
        del self.protocol_touch_state[protocol_address]

    async def run_loop(self):
        """
        The main loop. It methodically cycles through the curated list of wallets
        across all configured chains.
        """
        logger.info("👁️ VC & Smart Money Footprinting Service (Upgraded) is starting...")
        while True:
            try:
                logger.info("Starting a new cycle of multi-chain VC wallet footprinting...")
                # Flatten the wallet list for easy iteration
                tasks_to_run = []
                for vc_name, chains in CURATED_WALLETS.items():
                    for chain, address in chains.items():
                        tasks_to_run.append(self._footprint_wallet(vc_name, chain, address))
                
                # Run all footprinting tasks concurrently
                await asyncio.gather(*tasks_to_run)
                
                logger.info("Completed a full footprinting cycle. Waiting for next cycle.")
                # Run a full cycle approximately twice per day to stay up-to-date.
                await asyncio.sleep(43200) 
            except asyncio.CancelledError:
                logger.info("VC Watcher loop cancelled.")
                if self.session: await self.session.close()
                break
            except Exception as e:
                logger.error(f"An error occurred in the VC Watcher main loop: {e}", exc_info=True)
                await asyncio.sleep(600)
