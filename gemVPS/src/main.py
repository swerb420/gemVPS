# src/main.py
import asyncio
import sys
from utils.logger import setup_logging_directory, get_logger
from utils.config import load_config, Settings
from database.db_manager import DBManager
from api.server import run_api_server
from signals.signal_aggregator import AdvancedSignalAggregator
from telegram.bot import AdvancedTelegramBot

# Import all analysis and data modules
from onchain.whale_watcher import AdvancedWhaleWatcher
from onchain.vc_watcher import VCWatcher
from onchain.gas_analyzer import GasAnalyzer
from market_data.cex_listing_scanner import CEXListingScanner
from market_data.stablecoin_monitor import StablecoinMonitor
from analysis.narrative_tracker import NarrativeTracker
from analysis.weight_optimizer import WeightOptimizer

# Ensure the logs directory exists before any loggers are created
setup_logging_directory()
logger = get_logger(__name__)

async def main(config: Settings):
    """
    Initializes and runs all system components concurrently.
    This is the master conductor of the entire application.
    """
    tasks = []
    
    try:
        # --- Core Component Initialization ---
        db_manager = await DBManager.create(config)
        redis_client = None # In a full app, initialize an aioredis client here.
        
        signal_aggregator = AdvancedSignalAggregator(config, db_manager, redis_client)
        telegram_bot = AdvancedTelegramBot(config, signal_aggregator, db_manager)

        # --- Initialize All Data & Analysis Modules ---
        # Each module is instantiated with the components it needs to interact with.
        modules = {
            "whale_watcher": AdvancedWhaleWatcher(config, db_manager, signal_aggregator, telegram_bot),
            "vc_watcher": VCWatcher(config, signal_aggregator, db_manager),
            "gas_analyzer": GasAnalyzer(config, signal_aggregator),
            "cex_scanner": CEXListingScanner(config, signal_aggregator),
            "stablecoin_monitor": StablecoinMonitor(config, signal_aggregator),
            "narrative_tracker": NarrativeTracker(config, signal_aggregator),
            "weight_optimizer": WeightOptimizer(config, signal_aggregator, db_manager)
        }

        # --- Schedule All Background Tasks ---
        # The API server runs as a separate, non-blocking task.
        tasks.append(asyncio.create_task(run_api_server(), name="APIServer"))
        
        # The main signal processing loop.
        tasks.append(asyncio.create_task(signal_aggregator.run_aggregator_loop(), name="SignalAggregator"))
        
        # The Telegram bot polling loop.
        tasks.append(asyncio.create_task(telegram_bot.run(), name="TelegramBot"))

        # Schedule the main run loop for each analysis module.
        for name, module in modules.items():
            if hasattr(module, 'run_loop'):
                tasks.append(asyncio.create_task(module.run_loop(), name=name.capitalize()))
        
        logger.info(f"🚀 All {len(tasks)} components initialized and scheduled. System is live.")
        
        # Keep the main function alive to supervise all tasks.
        await asyncio.gather(*tasks)

    except asyncio.CancelledError:
        logger.info("Main task cancelled. Shutting down all services.")
    except Exception as e:
        logger.critical(f"A critical error occurred in the main application loop: {e}", exc_info=True)
    finally:
        logger.info("System shutting down gracefully.")
        # Cleanly close database connections and other resources
        if 'db_manager' in locals() and db_manager:
            await db_manager.close()
        
        # Cancel all running tasks to ensure a clean exit
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*[task for task in tasks if not task.done()], return_exceptions=True)

if __name__ == "__main__":
    try:
        # Load configuration at the start
        configuration = load_config()
        logger.info("✅ System starting up... Configuration loaded.")
        asyncio.run(main(configuration))
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutdown signal received.")
    except Exception as e:
        logger.critical(f"Failed to start the application: {e}", exc_info=True)
        sys.exit(1)
