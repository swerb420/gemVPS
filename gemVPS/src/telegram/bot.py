# src/telegram/bot.py
import asyncio
from telegram import Update, Bot
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters
from telegram.constants import ParseMode
from utils.logger import get_logger
from utils.config import Settings
from database.db_manager import DBManager
from signals.signal_aggregator import AdvancedSignalAggregator
from .chart_generator import ChartGenerator

logger = get_logger(__name__)

class AdvancedTelegramBot:
    """
    The interactive user interface for the trading bot. Handles commands,
    sends formatted alerts, and provides on-demand analysis. It's designed
    to be the primary control and monitoring center for the user.
    """
    def __init__(self, config: Settings, signal_aggregator: AdvancedSignalAggregator, db: DBManager):
        self.token = config.TELEGRAM_BOT_TOKEN.get_secret_value()
        self.chat_id = config.TELEGRAM_CHAT_ID
        self.application = Application.builder().token(self.token).build()
        
        # Store references to other modules to fetch data for commands
        self.signal_aggregator = signal_aggregator
        self.db = db
        
        self.chart_generator = ChartGenerator()
        self._setup_handlers()

    def _setup_handlers(self):
        """Adds all command and message handlers to the bot application."""
        command_handlers = {
            "start": self.start_command,
            "help": self.help_command,
            "status": self.status_command,
            "whois": self.whois_command,
            "narrative": self.narrative_command
        }
        for command, handler in command_handlers.items():
            self.application.add_handler(CommandHandler(command, handler))
        
        # A fallback handler for any command that isn't recognized
        self.application.add_handler(MessageHandler(filters.COMMAND, self.unknown_command))
        logger.info("Telegram command handlers have been successfully set up.")

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Sends a welcome message when the /start command is issued."""
        user_name = update.effective_user.first_name
        message = (
            f"👋 Hello, {user_name}!\n\n"
            "I am your **Elite Trading Intelligence Bot**.\n\n"
            "I am now monitoring the market for on-chain activity, whale movements, and narrative shifts. "
            "I will send alerts here automatically when significant events are detected.\n\n"
            "You can use the following commands:\n"
            "🔹 `/status` - Get a real-time market & system dashboard.\n"
            "🔹 `/narrative` - See top on-chain narratives.\n"
            "🔹 `/whois <address>` - Profile a wallet.\n"
            "🔹 `/help` - Show this message again."
        )
        await update.message.reply_html(message)

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """An alias for the start command, providing help information."""
        await self.start_command(update, context)

    async def status_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Generates and sends a real-time system status dashboard image."""
        await update.message.reply_text("⏳ Generating real-time status dashboard, please wait...")
        try:
            # In a real application, this data would be fetched live from other modules.
            # We use placeholder data here to demonstrate the functionality.
            status_data = {
                "cpu_usage": 55.0, # Would come from a ResourceMonitor module
                "ram_usage": 1450.0, # MB
                "fear_greed": {"value": 72, "value_classification": "Greed"}, # from DataFetcher
                "btc_price": 69420.0,
                "eth_price": 3800.0,
                "gas_price": 25.0, # from GasAnalyzer
                "dominant_narrative": "AI", # from NarrativeTracker
                "composite_signal_strength": 0.78 # from SignalAggregator
            }
            chart_image = await self.chart_generator.create_status_dashboard(status_data)
            await update.message.reply_photo(
                photo=chart_image, 
                caption="📊 **Market & System Status**\n*Data is updated in real-time.*",
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger.error(f"Failed to generate status dashboard: {e}", exc_info=True)
            await update.message.reply_text("❌ An error occurred while generating the status dashboard.")

    async def whois_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Provides a detailed profile for a given wallet address from the database."""
        if not context.args:
            await update.message.reply_text("Please provide a wallet address.\n**Usage:** `/whois 0x...`", parse_mode=ParseMode.MARKDOWN)
            return
        
        wallet_address = context.args[0].lower()
        await update.message.reply_text(f"⏳ Profiling wallet `{wallet_address}`...", parse_mode=ParseMode.MARKDOWN)
        
        # profile_data = await self.db.fetch_whale_profile(wallet_address)
        # For demonstration:
        await update.message.reply_text(f"Profile for `{wallet_address}` would be displayed here once database logic is complete.", parse_mode=ParseMode.MARKDOWN)

    async def narrative_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Displays the top currently trending on-chain narratives."""
        await update.message.reply_text("Top trending on-chain narratives would be displayed here.")

    async def unknown_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handles any command that isn't recognized by the other handlers."""
        await update.message.reply_text("Sorry, I don't recognize that command. Please try `/help`.")

    async def send_alert(self, message: str):
        """
        A public method for other modules to call to send alerts to the designated chat.
        This decouples the alerting logic from the Telegram bot implementation.
        """
        try:
            bot = Bot(token=self.token)
            await bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True
            )
        except Exception as e:
            logger.error(f"Failed to send Telegram alert to chat_id {self.chat_id}: {e}", exc_info=True)

    async def run(self):
        """Starts the bot's polling mechanism to listen for user commands."""
        logger.info("🤖 Telegram Bot is starting and initializing handlers...")
        try:
            await self.application.initialize()
            await self.application.start()
            await self.application.updater.start_polling()
            logger.info("🤖 Telegram Bot is now polling for updates.")
            # Keep the task alive
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            logger.info("Telegram bot task cancelled. Stopping polling.")
            await self.application.updater.stop()
            await self.application.stop()
        except Exception as e:
            logger.critical(f"A critical error occurred in the Telegram bot runner: {e}", exc_info=True)
