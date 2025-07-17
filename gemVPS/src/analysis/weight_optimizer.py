# src/analysis/weight_optimizer.py
import asyncio
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import MinMaxScaler
from typing import Dict, Any, Optional
from utils.logger import get_logger
from utils.config import Settings
from database.db_manager import DBManager
from signals.signal_aggregator import AdvancedSignalAggregator

logger = get_logger(__name__)

class WeightOptimizer:
    """
    A meta-learning module that dynamically optimizes signal weights based on
    their recent predictive performance within specific market regimes. This allows
    the bot to become highly adaptive to current market conditions.
    """
    def __init__(self, config: Settings, signal_aggregator: AdvancedSignalAggregator, db: DBManager):
        self.config = config
        self.signal_aggregator = signal_aggregator
        self.db = db

    async def _fetch_training_data(self) -> Optional[pd.DataFrame]:
        """
        Fetches historical data, including signals, price outcomes, and market
        regime indicators, to build a comprehensive training set.
        """
        # This advanced query would join multiple tables: raw signals, price history
        # (to calculate outcome), and a market context table (for f&g, volatility).
        # query = "..." 
        # data = await self.db.fetch_with_retry(query)
        # if not data: return None
        # return pd.DataFrame(data)
        
        # Using placeholder data for demonstration
        placeholder_data = [
            {'type': 'WHALE_TRADE', 'strength': 0.8, 'direction': 'bullish', 'outcome': 0.02, 'regime': 'Bullish'},
            {'type': 'LIQUIDITY_FLOW', 'strength': 0.6, 'direction': 'bullish', 'outcome': 0.015, 'regime': 'Bullish'},
            {'type': 'NARRATIVE_ROTATION', 'strength': 0.7, 'direction': 'bearish', 'outcome': -0.01, 'regime': 'Bearish'},
            {'type': 'DERIVATIVES_FEAR', 'strength': 0.9, 'direction': 'bearish', 'outcome': -0.03, 'regime': 'Bearish'},
            {'type': 'GAS_PRICE_ANOMALY', 'strength': 0.5, 'direction': 'neutral_investigate', 'outcome': 0.005, 'regime': 'Choppy'},
        ]
        return pd.DataFrame(placeholder_data)

    def _normalize_feature_importance(self, importances: pd.Series) -> Dict[str, float]:
        """Normalizes raw feature importances into a 0.1-1.0 scale for use as weights."""
        scaler = MinMaxScaler(feature_range=(0.1, 1.0))
        # Reshape for the scaler which expects a 2D array
        scaled_weights = scaler.fit_transform(importances.values.reshape(-1, 1))
        return {index: float(weight) for index, weight in zip(importances.index, scaled_weights.flatten())}

    async def optimize_weights(self):
        """
        The core logic: fetches data, segments it by market regime, trains a
        specialized model for each, and updates the aggregator's weights.
        """
        logger.info("🧠 Starting dynamic signal weight optimization cycle...")
        df = await self._fetch_training_data()

        if df is None or df.empty or len(df) < 50:
            logger.warning("Not enough recent signal data to perform weight optimization.")
            return

        # --- Feature Engineering ---
        df['direction_val'] = df['direction'].apply(lambda x: 1 if 'bullish' in x else -1 if 'bearish' in x else 0)
        # Target variable: 1 if the signal correctly predicted the price move, 0 otherwise.
        df['correct_prediction'] = ((df['outcome'] * df['direction_val']) > 0).astype(int)
        
        # One-hot encode the signal types
        features = pd.get_dummies(df['type'], prefix='type')
        features['strength'] = df['strength'] # Include signal strength as a feature
        
        # --- Regime-Based Training ---
        regimes = df['regime'].unique()
        all_new_weights = {}

        for regime in regimes:
            logger.info(f"Optimizing weights for market regime: {regime}")
            regime_df = df[df['regime'] == regime]
            regime_features = features.loc[regime_df.index]
            regime_target = regime_df['correct_prediction']

            if len(regime_df) < 20:
                logger.warning(f"Skipping regime '{regime}': insufficient data points ({len(regime_df)}).")
                continue

            # Use a more powerful model like RandomForest
            model = RandomForestClassifier(n_estimators=100, class_weight='balanced', random_state=42)
            model.fit(regime_features, regime_target)

            # --- Weight Extraction & Update ---
            feature_importances = pd.Series(model.feature_importances_, index=regime_features.columns)
            
            # We only care about the importance of the signal *type*, so filter for those features
            type_importances = feature_importances[feature_importances.index.str.startswith('type_')]
            type_importances.index = type_importances.index.str.replace('type_', '')
            
            new_weights = self._normalize_feature_importance(type_importances)
            all_new_weights[regime] = new_weights
        
        if not all_new_weights:
            logger.warning("Weight optimization cycle completed with no new weights generated.")
            return

        # Update the live weights in the signal aggregator
        self.signal_aggregator.update_regime_weights(all_new_weights)
        logger.info(f"✅ Signal weights re-optimized for {len(all_new_weights)} market regimes.")

    async def run_loop(self):
        """
        The main loop for the WeightOptimizer. It runs periodically to keep
        the system's brain adapted to the latest market conditions.
        """
        logger.info("🤖 Weight Optimizer (Meta-Learner, Upgraded) is starting...")
        while True:
            try:
                await self.optimize_weights()
            except asyncio.CancelledError:
                logger.info("Weight Optimizer loop cancelled.")
                break
            except Exception as e:
                logger.error(f"An error occurred in the Weight Optimizer loop: {e}", exc_info=True)
            
            # Re-optimize every 2 hours for a good balance of adaptability and stability.
            await asyncio.sleep(7200)
