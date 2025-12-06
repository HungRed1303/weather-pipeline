"""
Data Transformation Module
Handles ETL transformations and feature engineering for weather data
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime
from typing import Dict, List, Tuple

logger = logging.getLogger(__name__)


class WeatherDataTransformer:
    """Transform and engineer features from raw weather data"""
    
    def __init__(self):
        self.weather_code_mapping = self._create_weather_mapping()
    
    @staticmethod
    def _create_weather_mapping() -> Dict[int, str]:
        """Create weather code to category mapping"""
        return {
            0: 'clear', 1: 'partly_cloudy', 2: 'cloudy', 3: 'overcast',
            45: 'fog', 48: 'rime_fog',
            51: 'light_drizzle', 53: 'moderate_drizzle', 55: 'dense_drizzle',
            61: 'light_rain', 63: 'moderate_rain', 65: 'heavy_rain',
            71: 'light_snow', 73: 'moderate_snow', 75: 'heavy_snow',
            80: 'light_showers', 81: 'moderate_showers', 82: 'violent_showers',
            95: 'thunderstorm', 96: 'thunderstorm_light_hail', 99: 'thunderstorm_heavy_hail'
        }
    
    def transform_hourly_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply all transformations to hourly weather data
        
        Args:
            df: Raw hourly weather DataFrame
            
        Returns:
            Transformed DataFrame with engineered features
        """
        logger.info(f"Transforming {len(df)} hourly records")
        
        # Make a copy to avoid modifying original
        df = df.copy()
        
        # 1. Clean data
        df = self._clean_data(df)
        
        # 2. Add time features
        df = self._add_time_features(df)
        
        # 3. Add temperature features
        df = self._add_temperature_features(df)
        
        # 4. Add precipitation features
        df = self._add_precipitation_features(df)
        
        # 5. Add weather category
        df = self._add_weather_category(df)
        
        # 6. Add lag features
        df = self._add_lag_features(df)
        
        logger.info(f"Transformation complete. Output shape: {df.shape}")
        return df
    
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and validate data"""
        logger.info("Cleaning data...")
        
        # Ensure timestamp is datetime
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        elif 'forecast_timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['forecast_timestamp'])
        
        # Remove duplicates
        initial_count = len(df)
        df = df.drop_duplicates(subset=['location_id', 'timestamp'])
        if len(df) < initial_count:
            logger.warning(f"Removed {initial_count - len(df)} duplicate records")
        
        # Handle missing values
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        for col in numeric_columns:
            if df[col].isnull().any():
                # Forward fill for time series data
                df[col] = df[col].fillna(method='ffill')
                # If still null at the start, use backward fill
                df[col] = df[col].fillna(method='bfill')
        
        # Remove outliers for temperature
        if 'temperature' in df.columns:
            df = df[(df['temperature'] >= -50) & (df['temperature'] <= 60)]
        
        # Ensure humidity is in valid range
        if 'humidity' in df.columns:
            df['humidity'] = df['humidity'].clip(0, 100)
        
        logger.info(f"Cleaned data: {len(df)} records remaining")
        return df
    
    def _add_time_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract time-based features"""
        logger.info("Adding time features...")
        
        df['hour'] = df['timestamp'].dt.hour
        df['day_of_week'] = df['timestamp'].dt.dayofweek
        df['month'] = df['timestamp'].dt.month
        df['day_of_year'] = df['timestamp'].dt.dayofyear
        df['week_of_year'] = df['timestamp'].dt.isocalendar().week
        
        # Boolean features
        df['is_weekend'] = df['day_of_week'].isin([5, 6])
        df['is_morning'] = df['hour'].between(6, 11)
        df['is_afternoon'] = df['hour'].between(12, 17)
        df['is_evening'] = df['hour'].between(18, 22)
        df['is_night'] = df['hour'].isin([23, 0, 1, 2, 3, 4, 5])
        
        # Cyclical encoding for hour (to capture 23->0 continuity)
        df['hour_sin'] = np.sin(2 * np.pi * df['hour'] / 24)
        df['hour_cos'] = np.cos(2 * np.pi * df['hour'] / 24)
        
        # Cyclical encoding for month
        df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
        df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
        
        return df
    
    def _add_temperature_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Engineer temperature-related features"""
        logger.info("Adding temperature features...")
        
        if 'temperature' not in df.columns:
            logger.warning("Temperature column not found, skipping temperature features")
            return df
        
        # Sort by location and time for proper calculation
        df = df.sort_values(['location_id', 'timestamp'])
        
        # Temperature changes
        df['temp_change_1h'] = df.groupby('location_id')['temperature'].diff(1)
        df['temp_change_3h'] = df.groupby('location_id')['temperature'].diff(3)
        
        # Rolling statistics
        df['temp_rolling_avg_3h'] = df.groupby('location_id')['temperature'].transform(
            lambda x: x.rolling(window=3, min_periods=1).mean()
        )
        df['temp_rolling_avg_6h'] = df.groupby('location_id')['temperature'].transform(
            lambda x: x.rolling(window=6, min_periods=1).mean()
        )
        df['temp_rolling_std_3h'] = df.groupby('location_id')['temperature'].transform(
            lambda x: x.rolling(window=3, min_periods=1).std()
        )
        
        # Temperature variability
        df['temp_variability'] = df['temp_rolling_std_3h'].fillna(0)
        
        # Temperature categories
        df['temp_category'] = pd.cut(
            df['temperature'],
            bins=[-np.inf, 15, 20, 25, 30, np.inf],
            labels=['cold', 'cool', 'mild', 'warm', 'hot']
        )
        
        return df
    
    def _add_precipitation_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Engineer precipitation-related features"""
        logger.info("Adding precipitation features...")
        
        if 'precipitation' not in df.columns:
            logger.warning("Precipitation column not found, skipping precipitation features")
            return df
        
        # Binary indicator
        df['is_raining'] = (df['precipitation'] > 0).astype(int)
        
        # Rain intensity categories
        df['rain_intensity'] = pd.cut(
            df['precipitation'],
            bins=[-0.1, 0, 2, 10, 50, np.inf],
            labels=['none', 'light', 'moderate', 'heavy', 'extreme']
        )
        
        # Cumulative precipitation (last 3 hours)
        df = df.sort_values(['location_id', 'timestamp'])
        df['precip_sum_3h'] = df.groupby('location_id')['precipitation'].transform(
            lambda x: x.rolling(window=3, min_periods=1).sum()
        )
        
        # Hours since last rain
        df['hours_since_rain'] = 0
        for location_id in df['location_id'].unique():
            mask = df['location_id'] == location_id
            location_df = df[mask].copy()
            
            hours_since = []
            last_rain_idx = -1
            
            for idx, row in location_df.iterrows():
                if row['precipitation'] > 0:
                    last_rain_idx = idx
                    hours_since.append(0)
                else:
                    if last_rain_idx == -1:
                        hours_since.append(999)  # No rain recorded yet
                    else:
                        hours_diff = (idx - last_rain_idx)
                        hours_since.append(min(hours_diff, 999))
            
            df.loc[mask, 'hours_since_rain'] = hours_since
        
        return df
    
    def _add_weather_category(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add weather category from weather code"""
        logger.info("Adding weather categories...")
        
        if 'weather_code' not in df.columns:
            return df
        
        df['weather_category'] = df['weather_code'].map(self.weather_code_mapping)
        df['weather_category'] = df['weather_category'].fillna('unknown')
        
        # Group into broader categories
        clear_codes = ['clear', 'partly_cloudy']
        cloudy_codes = ['cloudy', 'overcast']
        rain_codes = ['light_rain', 'moderate_rain', 'heavy_rain', 
                      'light_drizzle', 'moderate_drizzle', 'dense_drizzle',
                      'light_showers', 'moderate_showers', 'violent_showers']
        
        df['weather_group'] = 'other'
        df.loc[df['weather_category'].isin(clear_codes), 'weather_group'] = 'clear'
        df.loc[df['weather_category'].isin(cloudy_codes), 'weather_group'] = 'cloudy'
        df.loc[df['weather_category'].isin(rain_codes), 'weather_group'] = 'rainy'
        
        return df
    
    def _add_lag_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add lagged features (previous values)"""
        logger.info("Adding lag features...")
        
        if 'temperature' not in df.columns:
            return df
        
        df = df.sort_values(['location_id', 'timestamp'])
        
        # Temperature lags
        for lag in [1, 3, 6, 24]:
            df[f'temp_lag_{lag}h'] = df.groupby('location_id')['temperature'].shift(lag)
        
        # Precipitation lags
        if 'precipitation' in df.columns:
            for lag in [1, 3, 6]:
                df[f'precip_lag_{lag}h'] = df.groupby('location_id')['precipitation'].shift(lag)
        
        return df
    
    def prepare_for_ml(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """
        Prepare dataset for machine learning
        
        Returns:
            Tuple of (prepared DataFrame, list of feature column names)
        """
        logger.info("Preparing data for ML...")
        
        # Drop rows with NaN in critical columns (from lag features)
        df = df.dropna(subset=['temperature'])
        
        # Select relevant features for ML
        feature_columns = [
            'hour', 'day_of_week', 'month', 'day_of_year',
            'hour_sin', 'hour_cos', 'month_sin', 'month_cos',
            'is_weekend', 'is_morning', 'is_afternoon', 'is_evening',
            'temp_change_1h', 'temp_rolling_avg_3h', 'temp_rolling_std_3h',
            'temp_lag_1h', 'temp_lag_3h', 'temp_lag_24h',
            'is_raining', 'precip_sum_3h'
        ]
        
        # Filter to only existing columns
        feature_columns = [col for col in feature_columns if col in df.columns]
        
        # Convert boolean to int
        bool_columns = df.select_dtypes(include=['bool']).columns
        df[bool_columns] = df[bool_columns].astype(int)
        
        # Fill remaining NaN with 0 (for lag features at the start)
        df[feature_columns] = df[feature_columns].fillna(0)
        
        logger.info(f"ML preparation complete. Features: {len(feature_columns)}")
        return df, feature_columns
    
    def validate_transformed_data(self, df: pd.DataFrame) -> bool:
        """
        Validate transformed data quality
        
        Returns:
            True if validation passes, False otherwise
        """
        checks = []
        
        # Check for critical NaN values
        critical_cols = ['temperature', 'hour', 'day_of_week']
        for col in critical_cols:
            if col in df.columns:
                nan_count = df[col].isnull().sum()
                if nan_count > 0:
                    logger.error(f"Found {nan_count} NaN values in {col}")
                    checks.append(False)
                else:
                    checks.append(True)
        
        # Check temperature range
        if 'temperature' in df.columns:
            invalid_temps = df[(df['temperature'] < -50) | (df['temperature'] > 60)]
            if len(invalid_temps) > 0:
                logger.error(f"Found {len(invalid_temps)} invalid temperature values")
                checks.append(False)
            else:
                checks.append(True)
        
        # Check time features
        if 'hour' in df.columns:
            invalid_hours = df[(df['hour'] < 0) | (df['hour'] > 23)]
            if len(invalid_hours) > 0:
                logger.error(f"Found {len(invalid_hours)} invalid hour values")
                checks.append(False)
            else:
                checks.append(True)
        
        passed = all(checks)
        if passed:
            logger.info("Data validation passed ✓")
        else:
            logger.error("Data validation failed ✗")
        
        return passed


def transform_weather_batch(weather_data: Dict) -> Dict[int, pd.DataFrame]:
    """
    Transform weather data for multiple locations
    
    Args:
        weather_data: Dictionary of location_id -> raw weather data
        
    Returns:
        Dictionary of location_id -> transformed DataFrame
    """
    transformer = WeatherDataTransformer()
    transformed_data = {}
    
    for location_id, data in weather_data.items():
        try:
            # Convert hourly data to DataFrame
            hourly_records = data.get('hourly', [])
            if not hourly_records:
                logger.warning(f"No hourly data for location {location_id}")
                continue
            
            df = pd.DataFrame(hourly_records)
            df['location_id'] = location_id
            
            # Apply transformations
            df_transformed = transformer.transform_hourly_data(df)
            
            # Validate
            if transformer.validate_transformed_data(df_transformed):
                transformed_data[location_id] = df_transformed
            else:
                logger.error(f"Validation failed for location {location_id}")
                
        except Exception as e:
            logger.error(f"Error transforming location {location_id}: {str(e)}")
    
    return transformed_data