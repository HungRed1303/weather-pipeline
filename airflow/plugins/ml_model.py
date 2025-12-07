"""
ML Model Module - Prophet-based Weather Forecasting
Handles model training, prediction, and evaluation
"""

import pandas as pd
import numpy as np
import logging
import pickle
import json
from pathlib import Path
from typing import Dict, Tuple, Optional
from datetime import datetime, timedelta

try:
    from prophet import Prophet
    import cmdstanpy
except ImportError:
    try:
        from fbprophet import Prophet
    except ImportError:
        raise ImportError("Please install prophet: pip install prophet")

from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import warnings
warnings.filterwarnings('ignore')

logger = logging.getLogger(__name__)


class WeatherForecastModel:
    """Prophet-based weather forecasting model"""
    
    def __init__(self, model_dir: str = "/opt/airflow/models"):
        """
        Initialize Weather Forecast Model
        
        Args:
            model_dir: Directory to save/load models
        """
        self.model_dir = Path(model_dir)
        self.model_dir.mkdir(parents=True, exist_ok=True)
        self.models = {}  # Dictionary to store models per location
        self.model_version = "prophet_v1.0"
    
    def prepare_data_for_prophet(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare data in Prophet format (ds, y columns)
        
        Args:
            df: DataFrame with timestamp and temperature columns
            
        Returns:
            DataFrame with 'ds' and 'y' columns
        """
        prophet_df = pd.DataFrame({
            'ds': pd.to_datetime(df['timestamp'], format='ISO8601'),
            'y': df['temperature']
        })
        
        # Add additional regressors if available
        if 'precipitation' in df.columns:
            prophet_df['precipitation'] = df['precipitation']
        if 'hour' in df.columns:
            prophet_df['hour'] = df['hour']
        if 'day_of_week' in df.columns:
            prophet_df['day_of_week'] = df['day_of_week']
        
        return prophet_df.sort_values('ds')
    
    def train_model(
        self, 
        df: pd.DataFrame, 
        location_id: int,
        location_name: str
    ) -> Tuple[Prophet, Dict]:
        """
        Train Prophet model for a specific location
        
        Args:
            df: Training data DataFrame
            location_id: ID of the location
            location_name: Name of the location
            
        Returns:
            Tuple of (trained model, training metrics)
        """
        logger.info(f"Training model for {location_name} (ID: {location_id})")
        
        # Prepare data
        prophet_df = self.prepare_data_for_prophet(df)
        
        if len(prophet_df) < 48:  # Need at least 2 days of data
            raise ValueError(f"Insufficient data for training: {len(prophet_df)} hours")
        
        # Initialize Prophet model with error suppression
        model = Prophet(
            yearly_seasonality=True,
            weekly_seasonality=True,
            daily_seasonality=True,
            seasonality_mode='multiplicative',
            changepoint_prior_scale=0.05,
            interval_width=0.95
        )
        
        # Suppress verbose output
        import logging as prophet_logging
        prophet_logging.getLogger('prophet').setLevel(prophet_logging.ERROR)
        prophet_logging.getLogger('cmdstanpy').setLevel(prophet_logging.ERROR)
        
        # Add additional regressors
        # if 'humidity' in prophet_df.columns:
        # #     model.add_regressor('humidity')
        if 'precipitation' in prophet_df.columns:
            model.add_regressor('precipitation')
        if 'hour' in prophet_df.columns:
            model.add_regressor('hour')
        if 'day_of_week' in prophet_df.columns:
            model.add_regressor('day_of_week')
        
        # Train model
        logger.info(f"Training on {len(prophet_df)} data points...")
        model.fit(prophet_df)
        
        # Evaluate on training data
        train_predictions = model.predict(prophet_df)
        metrics = self._calculate_metrics(
            y_true=prophet_df['y'].values,
            y_pred=train_predictions['yhat'].values
        )
        
        logger.info(f"Training complete. MAE: {metrics['mae']:.2f}, RMSE: {metrics['rmse']:.2f}")
        
        # Store model
        self.models[location_id] = model
        
        # Save model to disk
        self._save_model(model, location_id, location_name)
        
        return model, metrics
    
    def predict(
        self, 
        location_id: int, 
        hours_ahead: int = 24,
        future_regressors: Optional[pd.DataFrame] = None
    ) -> pd.DataFrame:
        """
        Make predictions for a specific location
        
        Args:
            location_id: ID of the location
            hours_ahead: Number of hours to predict ahead
            future_regressors: DataFrame with future values of regressors
            
        Returns:
            DataFrame with predictions including confidence intervals
        """
        if location_id not in self.models:
            self._load_model(location_id)
        
        model = self.models.get(location_id)
        if model is None:
            raise ValueError(f"No model found for location {location_id}")
        
        logger.info(f"Predicting {hours_ahead} hours ahead for location {location_id}")
        
        # Create future dataframe
        future = model.make_future_dataframe(
            periods=hours_ahead, 
            freq='H',
            include_history=False
        )
        
        # Add regressors if provided
        if future_regressors is not None:
            for col in future_regressors.columns:
                if col != 'ds':
                    future[col] = future_regressors[col].values[:len(future)]
        else:
            # Use reasonable defaults
            future['hour'] = future['ds'].dt.hour
            future['day_of_week'] = future['ds'].dt.dayofweek
            # if 'humidity' in model.extra_regressors:
            #     future['humidity'] = 70  # Default humidity
            if 'precipitation' in model.extra_regressors:
                future['precipitation'] = 0  # Assume no rain by default
        
        # Make predictions
        forecast = model.predict(future)
        
        # Format results
        results = pd.DataFrame({
            'timestamp': forecast['ds'],
            'predicted_temperature': forecast['yhat'],
            'lower_bound': forecast['yhat_lower'],
            'upper_bound': forecast['yhat_upper'],
            'location_id': location_id
        })
        
        return results
    
    def predict_all_locations(
        self, 
        hours_ahead: int = 24
    ) -> Dict[int, pd.DataFrame]:
        """
        Make predictions for all locations with trained models
        
        Returns:
            Dictionary mapping location_id to prediction DataFrame
        """
        predictions = {}
        
        for location_id in self.models.keys():
            try:
                pred = self.predict(location_id, hours_ahead)
                predictions[location_id] = pred
            except Exception as e:
                logger.error(f"Prediction failed for location {location_id}: {str(e)}")
        
        return predictions
    
    def _calculate_metrics(
        self, 
        y_true: np.ndarray, 
        y_pred: np.ndarray
    ) -> Dict[str, float]:
        """Calculate model performance metrics"""
        mae = mean_absolute_error(y_true, y_pred)
        rmse = np.sqrt(mean_squared_error(y_true, y_pred))
        mape = np.mean(np.abs((y_true - y_pred) / y_true)) * 100
        r2 = r2_score(y_true, y_pred)
        
        return {
            'mae': float(mae),
            'rmse': float(rmse),
            'mape': float(mape),
            'r2_score': float(r2)
        }
    
    def evaluate_predictions(
        self, 
        predictions_df: pd.DataFrame, 
        actuals_df: pd.DataFrame
    ) -> Dict[str, float]:
        """
        Evaluate predictions against actual values
        
        Args:
            predictions_df: DataFrame with predictions
            actuals_df: DataFrame with actual values
            
        Returns:
            Dictionary of evaluation metrics
        """
        # Merge on timestamp
        merged = predictions_df.merge(
            actuals_df,
            on=['timestamp', 'location_id'],
            how='inner'
        )
        
        if len(merged) == 0:
            logger.warning("No matching timestamps for evaluation")
            return {}
        
        metrics = self._calculate_metrics(
            y_true=merged['actual_temperature'].values,
            y_pred=merged['predicted_temperature'].values
        )
        
        logger.info(f"Evaluation metrics - MAE: {metrics['mae']:.2f}°C, "
                   f"RMSE: {metrics['rmse']:.2f}°C, R²: {metrics['r2_score']:.3f}")
        
        return metrics
    
    def _save_model(self, model: Prophet, location_id: int, location_name: str):
        """Save model to disk"""
        model_path = self.model_dir / f"prophet_location_{location_id}.pkl"
        
        with open(model_path, 'wb') as f:
            pickle.dump(model, f)
        
        # Save metadata
        metadata = {
            'location_id': location_id,
            'location_name': location_name,
            'model_version': self.model_version,
            'trained_at': datetime.now().isoformat(),
            'model_params': {
                'yearly_seasonality': model.yearly_seasonality,
                'weekly_seasonality': model.weekly_seasonality,
                'daily_seasonality': model.daily_seasonality,
                'seasonality_mode': model.seasonality_mode
            }
        }
        
        metadata_path = self.model_dir / f"prophet_location_{location_id}_metadata.json"
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logger.info(f"Model saved to {model_path}")
    
    def _load_model(self, location_id: int) -> Optional[Prophet]:
        """Load model from disk"""
        model_path = self.model_dir / f"prophet_location_{location_id}.pkl"
        
        if not model_path.exists():
            logger.error(f"Model file not found: {model_path}")
            return None
        
        try:
            with open(model_path, 'rb') as f:
                model = pickle.load(f)
            
            self.models[location_id] = model
            logger.info(f"Model loaded from {model_path}")
            return model
            
        except Exception as e:
            logger.error(f"Error loading model: {str(e)}")
            return None
    
    def load_all_models(self):
        """Load all available models from disk"""
        model_files = list(self.model_dir.glob("prophet_location_*.pkl"))
        
        for model_file in model_files:
            # Extract location_id from filename
            location_id = int(model_file.stem.split('_')[-1])
            self._load_model(location_id)
        
        logger.info(f"Loaded {len(self.models)} models")
    
    def get_model_info(self) -> Dict:
        """Get information about loaded models"""
        info = {
            'model_version': self.model_version,
            'loaded_models': list(self.models.keys()),
            'model_count': len(self.models)
        }
        
        # Add metadata for each model
        for location_id in self.models.keys():
            metadata_path = self.model_dir / f"prophet_location_{location_id}_metadata.json"
            if metadata_path.exists():
                with open(metadata_path, 'r') as f:
                    info[f'location_{location_id}'] = json.load(f)
        
        return info


def train_models_for_all_locations(training_data: Dict[int, pd.DataFrame]) -> Dict:
    """
    Train models for all locations
    
    Args:
        training_data: Dictionary mapping location_id to training DataFrame
        
    Returns:
        Dictionary with training results and metrics
    """
    model_manager = WeatherForecastModel()
    results = {
        'trained_models': [],
        'metrics': {},
        'errors': []
    }
    
    # Load location names
    with open('/opt/airflow/config/locations.json', 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    location_map = {city['id']: city['name'] for city in config['cities']}
    
    for location_id, df in training_data.items():
        location_name = location_map.get(location_id, f"Location_{location_id}")
        
        try:
            model, metrics = model_manager.train_model(df, location_id, location_name)
            results['trained_models'].append(location_id)
            results['metrics'][location_id] = metrics
            
        except Exception as e:
            error_msg = f"Training failed for {location_name}: {str(e)}"
            logger.error(error_msg)
            results['errors'].append(error_msg)
    
    return results