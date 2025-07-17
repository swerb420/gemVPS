# src/utils/config.py
import os
from dotenv import load_dotenv
from pydantic import BaseModel, Field, HttpUrl, SecretStr
from typing import Optional

class Settings(BaseModel):
    """
    Pydantic model for loading and validating all environment variables for the application.
    This provides a single, reliable source of truth for all configuration.
    """
    # --- CORE SETTINGS & API KEYS ---
    TELEGRAM_BOT_TOKEN: SecretStr
    TELEGRAM_CHAT_ID: str
    WEB3_PROVIDER_URL: HttpUrl
    ALCHEMY_WEBSOCKET_URL: str # Websocket URLs are not yet supported in Pydantic's HttpUrl
    POLYGON_RPC_URL: Optional[HttpUrl] = None
    SHYFT_API_KEY: SecretStr
    NEWS_API_KEY: Optional[SecretStr] = None
    SANTIMENT_API_KEY: Optional[SecretStr] = None

    # --- WEBHOOK CONFIG ---
    VPS_PUBLIC_URL: HttpUrl

    # --- DATABASE & CACHE ---
    POSTGRES_USER: str = Field("trader", description="Default PostgreSQL username")
    POSTGRES_PASSWORD: SecretStr = Field("supersecretpassword", description="Default PostgreSQL password")
    POSTGRES_DB: str = Field("trading_data", description="Default PostgreSQL database name")
    POSTGRES_HOST: str = Field("database", description="The service name from docker-compose")
    POSTGRES_PORT: int = Field(5432, description="Default PostgreSQL port")
    
    REDIS_HOST: str = Field("cache", description="The service name from docker-compose")
    REDIS_PORT: int = Field(6379, description="Default Redis port")

    # --- VPS RESOURCE TUNING ---
    POSTGRES_SHARED_BUFFERS: str = Field("256MB", description="Memory for shared database buffers")
    POSTGRES_WORK_MEM: str = Field("4MB", description="Memory for internal sort operations")
    POSTGRES_MAX_CONNECTIONS: int = Field(30, description="Max concurrent database connections")

    # --- FEATURE TOGGLES & SETTINGS ---
    ENABLE_AUTO_TRADING: bool = Field(False, description="Master switch for automated trade execution")
    STOCK_LIMIT: int = Field(6, ge=1, le=20, description="Number of crypto-correlated stocks to track")

    class Config:
        """
        Pydantic configuration class.
        Tells Pydantic to load variables from a .env file.
        """
        env_file = ".env"
        env_file_encoding = 'utf-8'
        # This allows the model to be created even if .env is missing, for testing purposes.
        # However, it will fail validation if required fields are not set by other means.
        extra = 'ignore' 

def load_config() -> Settings:
    """
    Loads environment variables from the .env file and validates them using the Settings model.
    
    Returns:
        Settings: An immutable instance of the application settings.
    
    Raises:
        ValidationError: If any required environment variables are missing or have incorrect types.
    """
    # Load the .env file from the project root
    load_dotenv()
    
    # Pydantic will automatically read the environment variables and validate them
    return Settings()

# Example of how to use it in other modules:
# from utils.config import load_config
# config = load_config()
# api_key = config.SHYFT_API_KEY.get_secret_value()
