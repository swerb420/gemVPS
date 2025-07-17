# src/database/db_manager.py
import asyncio
import asyncpg
from asyncpg.pool import Pool
from typing import List, Any, Optional
from utils.logger import get_logger
from utils.config import Settings

logger = get_logger(__name__)

class DBManager:
    """
    Manages the connection pool and all interactions with the TimescaleDB database.
    This class provides a centralized, robust, and efficient way to handle database
    operations, including connection management and query execution with retries.
    """
    _pool: Optional[Pool] = None

    def __init__(self, pool: Pool):
        """The constructor should not be called directly. Use the create() classmethod."""
        self._pool = pool

    @classmethod
    async def create(cls, config: Settings) -> 'DBManager':
        """
        Creates and returns a new DBManager instance with an initialized connection pool.
        This is the designated way to instantiate this class.

        Args:
            config (Settings): The application's configuration settings.

        Returns:
            DBManager: A new instance of the DBManager.
        
        Raises:
            Exception: If the database connection fails.
        """
        if cls._pool is None:
            try:
                cls._pool = await asyncpg.create_pool(
                    user=config.POSTGRES_USER,
                    password=config.POSTGRES_PASSWORD.get_secret_value(),
                    database=config.POSTGRES_DB,
                    host=config.POSTGRES_HOST,
                    port=config.POSTGRES_PORT,
                    min_size=config.DB_POOL_MIN_SIZE,
                    max_size=config.DB_POOL_MAX_SIZE
                )
                logger.info("✅ Database connection pool established successfully.")
                await cls._initialize_db_schema(cls._pool)
            except Exception as e:
                logger.critical(f"❌ CRITICAL: Failed to connect to database: {e}", exc_info=True)
                raise
        return cls(cls._pool)

    @staticmethod
    async def _initialize_db_schema(pool: Pool):
        """
        Executes the initial SQL scripts to set up the database schema,
        create tables, and enable the TimescaleDB extension.
        """
        async with pool.acquire() as connection:
            # Enable TimescaleDB Extension
            await connection.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
            
            # --- Create Tables ---
            # Example for a 'trades' table
            await connection.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    time TIMESTAMPTZ NOT NULL,
                    symbol TEXT NOT NULL,
                    price DOUBLE PRECISION NOT NULL,
                    volume DOUBLE PRECISION NOT NULL,
                    wallet_address TEXT,
                    protocol TEXT
                );
            """)
            
            # --- Create Hypertables (TimescaleDB's core feature) ---
            # This converts the standard table into a high-performance hypertable.
            # It will only run once. If the table is already a hypertable, it does nothing.
            await connection.execute("SELECT create_hypertable('trades', 'time', if_not_exists => TRUE);")
            logger.info("Database schema and hypertables initialized.")

    async def execute_with_retry(self, query: str, *args: Any, retries: int = 3, delay: int = 2) -> None:
        """
        Executes a query that does not return data (e.g., INSERT, UPDATE, DELETE) with retry logic.

        Args:
            query (str): The SQL query to execute.
            *args (Any): The arguments to pass to the query.
            retries (int): The number of times to retry on failure.
            delay (int): The delay in seconds between retries.
        """
        attempt = 0
        while attempt < retries:
            try:
                async with self._pool.acquire() as connection:
                    await connection.execute(query, *args)
                return
            except (asyncpg.exceptions.PostgresConnectionError, OSError) as e:
                attempt += 1
                logger.warning(f"DB connection error on attempt {attempt}: {e}. Retrying in {delay}s...")
                await asyncio.sleep(delay)
        logger.error(f"Failed to execute query after {retries} attempts: {query}")

    async def fetch_with_retry(self, query: str, *args: Any, retries: int = 3, delay: int = 2) -> List[asyncpg.Record]:
        """
        Executes a query that returns data (e.g., SELECT) with retry logic.

        Args:
            query (str): The SQL query to execute.
            *args (Any): The arguments to pass to the query.
            retries (int): The number of times to retry on failure.
            delay (int): The delay in seconds between retries.

        Returns:
            List[asyncpg.Record]: A list of records returned by the query, or an empty list on failure.
        """
        attempt = 0
        while attempt < retries:
            try:
                async with self._pool.acquire() as connection:
                    return await connection.fetch(query, *args)
            except (asyncpg.exceptions.PostgresConnectionError, OSError) as e:
                attempt += 1
                logger.warning(f"DB connection error on attempt {attempt}: {e}. Retrying in {delay}s...")
                await asyncio.sleep(delay)
        logger.error(f"Failed to fetch query after {retries} attempts: {query}")
        return []

    async def close(self):
        """Gracefully closes the database connection pool."""
        if self._pool:
            await self._pool.close()
            logger.info("Database connection pool closed gracefully.")
            self._pool = None
