# src/api/server.py
import asyncio
import uvicorn
from fastapi import FastAPI, Request, Header, HTTPException, Depends
from typing import Optional, Any, Dict
from utils.logger import get_logger
from utils.config import Settings, load_config

logger = get_logger(__name__)

# This asyncio Queue is a crucial component for decoupling.
# The API server's only job is to receive data and put it on this queue.
# Another dedicated module (e.g., WhaleWatcher) will consume from this queue.
# This makes the API endpoint extremely fast and prevents it from being blocked
# by slow processing logic.
webhook_queue = asyncio.Queue()

# --- FastAPI Application Setup ---
app = FastAPI(
    title="Trading Bot Webhook API",
    description="Receives real-time data from external services like Shyft.",
    version="1.0.0"
)

# --- Dependency for Configuration ---
# This makes the config available to our endpoint logic in a clean way.
def get_app_config() -> Settings:
    return load_config()

# --- Security Dependency ---
# A simple but effective security measure to ensure webhooks are legitimate.
# In a production environment, you would use the secret provided by Shyft.
async def verify_webhook_secret(
    x_shyft_webhook_secret: Optional[str] = Header(None), 
    config: Settings = Depends(get_app_config)
):
    # This feature is disabled if no secret is set in the .env file.
    # Replace 'YOUR_SHYFT_WEBHOOK_SECRET' with the actual secret from Shyft dashboard.
    expected_secret = "YOUR_SHYFT_WEBHOOK_SECRET" 
    if expected_secret != "YOUR_SHYFT_WEBHOOK_SECRET": # Check if a secret is configured
        if x_shyft_webhook_secret is None:
            logger.warning("Missing X-Shyft-Webhook-Secret header.")
            raise HTTPException(status_code=400, detail="Missing webhook secret header")
        if x_shyft_webhook_secret != expected_secret:
            logger.warning("Invalid webhook secret received.")
            raise HTTPException(status_code=403, detail="Invalid webhook secret")

# --- Webhook Endpoint ---
@app.post(
    "/webhooks/shyft",
    summary="Shyft Callback Receiver",
    description="Receives transaction notifications from the Shyft API.",
    status_code=202 # Accepted: The request has been accepted for processing.
)
async def shyft_webhook_receiver(
    request: Request,
    # The Depends() function runs our security check before the endpoint logic.
    _security_check: None = Depends(verify_webhook_secret) 
) -> Dict[str, Any]:
    """
    This endpoint listens for POST requests from Shyft. It validates the request,
    places the data onto an internal queue for asynchronous processing, and
    immediately returns a success response to Shyft.
    """
    try:
        data = await request.json()
        if not data:
            logger.warning("Received an empty payload from Shyft webhook.")
            raise HTTPException(status_code=400, detail="Empty payload")

        # Log the reception of the event for monitoring purposes
        tx_hash = data[0].get('transaction_hash', 'N/A') if isinstance(data, list) and data else 'N/A'
        logger.info(f"Received Shyft callback for transaction: {tx_hash}. Placing in queue.")
        
        # Put the validated data onto the queue for the main application to process.
        await webhook_queue.put({"source": "shyft", "payload": data})
        
        return {"status": "success", "message": "Webhook data queued for processing."}
    
    except Exception as e:
        logger.error(f"Error processing Shyft webhook payload: {e}", exc_info=True)
        # Return a generic server error to avoid leaking implementation details.
        raise HTTPException(status_code=500, detail="Internal server error")

# --- Function to run the server ---
# This will be called as a task from main.py
async def run_api_server():
    """
    Initializes and runs the Uvicorn server for the FastAPI application.
    """
    config = uvicorn.Config(
        app, 
        host="0.0.0.0", # Listens on all available network interfaces
        port=8000, 
        log_level="warning" # Uvicorn's logging can be noisy; we use our own logger.
    )
    server = uvicorn.Server(config)
    logger.info("🚀 API Server starting on http://0.0.0.0:8000")
    await server.serve()
