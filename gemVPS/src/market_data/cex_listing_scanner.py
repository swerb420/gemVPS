# src/market_data/cex_listing_scanner.py
import asyncio
import aiohttp
import re
from bs4 import BeautifulSoup
from typing import Dict, Any, Set, Optional, Callable, Coroutine
from utils.logger import get_logger
from utils.config import Settings
from signals.signal_aggregator import AdvancedSignalAggregator
from web3 import Web3

logger = get_logger(__name__)

class CEXListingScanner:
    """
    A low-latency, multi-source arbitrage detection engine that monitors a wide
    array of major centralized exchanges for new listing announcements. This greatly
    expanded version provides maximum coverage of listing-related arbitrage opportunities.
    """
    def __init__(self, config: Settings, signal_aggregator: AdvancedSignalAggregator):
        self.signal_aggregator = signal_aggregator
        self.w3 = Web3(Web3.HTTPProvider(str(config.WEB3_PROVIDER_URL)))
        self.session: Optional[aiohttp.ClientSession] = None
        self.known_announcements: Set[str] = set()

        # GREATLY EXPANDED: Structured targets for the top 12 CEXs.
        # Each target has a dedicated parser function for maintainability.
        self.targets: list[dict[str, Any]] = [
            {"exchange": "Binance", "url": "https://www.binance.com/en/support/announcement/new-cryptocurrency-listing", "parser": self._parse_binance_html},
            {"exchange": "Coinbase", "url": "https://www.coinbase.com/blog/products", "parser": self._parse_generic_blog_html},
            {"exchange": "KuCoin", "url": "https://www.kucoin.com/announcement/new-listings", "parser": self._parse_kucoin_html},
            {"exchange": "Bybit", "url": "https://announcements.bybit.com/en-US/?category=new_crypto", "parser": self._parse_generic_blog_html},
            {"exchange": "OKX", "url": "https://www.okx.com/support/hc/en-us/sections/360000030652-New-Token-Listing", "parser": self._parse_generic_blog_html},
            {"exchange": "Gate.io", "url": "https://www.gate.io/articlelist/ann", "parser": self._parse_gateio_html},
            {"exchange": "Kraken", "url": "https://blog.kraken.com/tag/listing/", "parser": self._parse_generic_blog_html},
            {"exchange": "Bitget", "url": "https://www.bitget.com/support/sections/5311864583577", "parser": self._parse_generic_blog_html},
            {"exchange": "HTX", "url": "https://www.htx.com/support/en-us/list/360000039481", "parser": self._parse_generic_blog_html},
            {"exchange": "MEXC", "url": "https://www.mexc.com/support/sections/360002254431", "parser": self._parse_generic_blog_html},
            {"exchange": "Bitstamp", "url": "https://www.bitstamp.net/faq/asset-listing/", "parser": self._parse_generic_blog_html},
            {"exchange": "Crypto.com", "url": "https://crypto.com/product-news/crypto_com_exchange", "parser": self._parse_generic_blog_html},
        ]

    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'})
        return self.session

    async def _calculate_priority_gas(self) -> Dict[str, int]:
        """Calculates an aggressive gas fee to front-run other arbitrage bots."""
        try:
            latest_block = self.w3.eth.get_block('latest')
            base_fee = latest_block.get('baseFeePerGas', 0)
            priority_tip = self.w3.to_wei(15, 'gwei') 
            max_fee_per_gas = base_fee * 2 + priority_tip
            return {"maxPriorityFeePerGas": priority_tip, "maxFeePerGas": max_fee_per_gas}
        except Exception as e:
            logger.error(f"Failed to calculate priority gas: {e}", exc_info=True)
            return {}

    def _extract_symbol(self, title: str) -> Optional[str]:
        """UPGRADED: Extracts a cryptocurrency symbol using multiple regex patterns."""
        patterns = [
            r'\(([A-Z]{2,6})\)',           # Matches (SYMBOL)
            r'Lists ([A-Z]{2,6})',         # Matches "Lists SYMBOL"
            r'adds support for ([A-Z]{2,6})', # Matches "adds support for SYMBOL"
            r'Trading for ([A-Z]{2,6})',   # Matches "Trading for SYMBOL"
        ]
        for pattern in patterns:
            match = re.search(pattern, title, re.IGNORECASE)
            if match:
                return match.group(1).upper()
        return None

    async def _process_announcement(self, exchange: str, title: str):
        """Centralized logic to process a potential listing announcement."""
        if title in self.known_announcements:
            return
        
        keywords = ["list", "trading for", "opens trading", "available on", "support for"]
        if any(keyword in title.lower() for keyword in keywords):
            symbol = self._extract_symbol(title)
            if symbol:
                self.known_announcements.add(title)
                logger.critical(f"🚨🚨 CEX LISTING DETECTED: {symbol} on {exchange}!")
                
                priority_gas = await self._calculate_priority_gas()
                signal = {
                    "type": "CEX_LISTING_ARBITRAGE", "asset": symbol,
                    "strength": 1.0, "direction": "bullish",
                    "metadata": {
                        "exchange": exchange, "announcement_title": title,
                        "recommended_gas": priority_gas, "action": "IMMEDIATE_DEX_BUY_PRIORITY"
                    }
                }
                await self.signal_aggregator.submit_high_priority_signal(signal)

    # --- Exchange Specific Parsers ---
    # NOTE: These parsers require manual implementation by inspecting the target websites'
    # HTML structure or finding their official APIs. They are provided as templates.

    async def _parse_binance_html(self, text: str, exchange: str):
        soup = BeautifulSoup(text, 'html.parser')
        # This selector needs to be verified and maintained.
        for link in soup.find_all('a', class_='css-1ej4h8i', limit=5):
            await self._process_announcement(exchange, link.text)

    async def _parse_gateio_html(self, text: str, exchange: str):
        soup = BeautifulSoup(text, 'html.parser')
        # This selector needs to be verified and maintained.
        for link in soup.find_all('a', class_='latitle', limit=5):
            await self._process_announcement(exchange, link.text)

    async def _parse_kucoin_html(self, text: str, exchange: str):
        soup = BeautifulSoup(text, 'html.parser')
        # This selector needs to be verified and maintained.
        for link in soup.find_all('a', href=re.compile(r'/announcement/'), limit=5):
             await self._process_announcement(exchange, link.text)

    async def _parse_generic_blog_html(self, text: str, exchange: str):
        """A generic parser for blog-style announcement pages."""
        soup = BeautifulSoup(text, 'html.parser')
        # This looks for common heading tags. It's a best-effort approach.
        for tag in soup.find_all(['h1', 'h2', 'h3'], limit=10):
            await self._process_announcement(exchange, tag.text)

    async def _scan_target(self, target: Dict[str, Any]):
        """Scans a single configured target using its dedicated parser."""
        exchange, url, parser = target["exchange"], target["url"], target["parser"]
        try:
            session = await self._get_session()
            async with session.get(url, timeout=10) as response:
                response.raise_for_status()
                content = await response.text()
                await parser(content, exchange)
        except Exception as e:
            logger.error(f"Error scanning target {exchange}: {e}", exc_info=True)

    async def run_loop(self):
        """The main scanner loop, running all target scans concurrently."""
        logger.info("📈 CEX Listing Arbitrage Scanner (Greatly Expanded) is starting...")
        while True:
            try:
                scan_tasks = [self._scan_target(target) for target in self.targets]
                await asyncio.gather(*scan_tasks)
            except asyncio.CancelledError:
                logger.info("CEX Listing Scanner loop cancelled.")
                if self.session: await self.session.close()
                break
            except Exception as e:
                logger.error(f"An error occurred in the CEX scanner main loop: {e}", exc_info=True)
            
            await asyncio.sleep(10)
