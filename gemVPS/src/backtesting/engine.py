# src/backtesting/engine.py
import pandas as pd
import numpy as np
from typing import Dict, Any
from utils.logger import get_logger
from database.db_manager import DBManager
from utils.config import load_config

logger = get_logger(__name__)

class BacktestingEngine:
    """
    An offline engine for simulating trading strategies against historical data.
    This tool is essential for validating strategies and optimizing parameters
    without risking real capital.
    """
    def __init__(self, db_manager: DBManager):
        self.db = db_manager
        # --- Simulation Parameters ---
        self.initial_capital = 10000.0
        self.trade_size_usd = 500.0
        self.slippage_pct = 0.05 / 100  # 0.05% simulated slippage per trade
        self.fees_pct = 0.07 / 100      # 0.07% simulated trading fees per trade

    async def _prepare_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Fetches and prepares all necessary historical data (prices, signals)
        for the backtest period, merging them into a single DataFrame.
        """
        logger.info(f"Fetching historical data from {start_date} to {end_date}...")
        # In a real implementation, these queries would be more complex, joining
        # multiple tables to align signals with future price data.
        # signals_query = "..."
        # prices_query = "..."
        # signals_df = pd.DataFrame(await self.db.fetch_with_retry(signals_query))
        # prices_df = pd.DataFrame(await self.db.fetch_with_retry(prices_query))
        
        # For demonstration, we'll create a sample DataFrame
        data = {
            'timestamp': pd.to_datetime(['2023-01-01 10:00', '2023-01-01 11:00', '2023-01-01 12:00', '2023-01-01 13:00']),
            'price': [100.0, 102.0, 101.0, 103.0],
            'signal_type': ['CEX_LISTING_ARBITRAGE', None, 'SMART_MONEY_BUY', None],
            'signal_strength': [1.0, np.nan, 0.8, np.nan],
            'signal_direction': ['bullish', None, 'bullish', None]
        }
        df = pd.DataFrame(data).set_index('timestamp')
        logger.info(f"Successfully prepared {len(df)} data points for backtest.")
        return df

    def _calculate_performance_metrics(self, equity_curve: pd.Series) -> Dict[str, Any]:
        """Calculates key performance indicators (KPIs) from the equity curve."""
        total_return = (equity_curve.iloc[-1] / equity_curve.iloc[0] - 1) * 100
        
        # Calculate drawdown
        rolling_max = equity_curve.cummax()
        daily_drawdown = equity_curve / rolling_max - 1.0
        max_drawdown = daily_drawdown.min() * 100
        
        # Calculate Sharpe Ratio (assuming daily returns)
        daily_returns = equity_curve.pct_change().dropna()
        sharpe_ratio = (daily_returns.mean() / daily_returns.std()) * np.sqrt(365) if daily_returns.std() > 0 else 0.0
        
        return {
            "Total Return (%)": f"{total_return:.2f}",
            "Max Drawdown (%)": f"{max_drawdown:.2f}",
            "Sharpe Ratio": f"{sharpe_ratio:.2f}",
        }

    async def run_test(self, strategy_config: Dict[str, Any], start_date: str, end_date: str):
        """
        Runs a backtest for a given strategy configuration and time period.
        """
        df = await self._prepare_data(start_date, end_date)
        if df.empty:
            logger.error("Cannot run backtest: No historical data found for the period.")
            return

        logger.info(f"Running backtest for strategy: '{strategy_config.get('name', 'Unnamed')}'")

        # --- Simulation Loop ---
        capital = self.initial_capital
        position_size = 0.0
        equity = []

        for timestamp, row in df.iterrows():
            # Check for exit signals first
            # ... (logic to close positions) ...

            # Check for entry signals
            if pd.notna(row['signal_type']) and row['signal_type'] in strategy_config['active_signals']:
                if row['signal_strength'] >= strategy_config['min_strength']:
                    if row['signal_direction'] == 'bullish' and position_size == 0:
                        # Simulate BUY
                        entry_price = row['price'] * (1 + self.slippage_pct)
                        position_size = self.trade_size_usd / entry_price
                        capital -= self.trade_size_usd * (1 + self.fees_pct)
                        logger.debug(f"[{timestamp}] - ENTER LONG at {entry_price:.2f}")

            # Update equity curve
            current_value = capital + (position_size * row['price'])
            equity.append(current_value)

        # --- Generate Report ---
        equity_curve = pd.Series(equity, index=df.index)
        performance_metrics = self._calculate_performance_metrics(equity_curve)

        print("\n" + "="*50)
        print(f"BACKTEST REPORT: {strategy_config.get('name', 'Unnamed')}")
        print(f"Period: {start_date} to {end_date}")
        print("-"*50)
        for metric, value in performance_metrics.items():
            print(f"{metric:<25}: {value}")
        print("="*50 + "\n")

# --- Example of how to run the backtester ---
async def run_example_backtest():
    config = load_config()
    db = await DBManager.create(config)
    engine = BacktestingEngine(db)

    # Define the strategy to test
    strategy = {
        "name": "High-Priority Arbitrage Strategy",
        "active_signals": ["CEX_LISTING_ARBITRAGE", "SMART_MONEY_BUY"],
        "min_strength": 0.8
    }

    await engine.run_test(strategy, "2023-01-01", "2023-12-31")
    await db.close()

if __name__ == "__main__":
    # This allows running the backtester as a standalone script
    # python -m src.backtesting.engine
    asyncio.run(run_example_backtest())
