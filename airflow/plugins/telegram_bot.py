"""
Telegram Bot Module
Handles sending weather forecast notifications to Telegram
"""

import os
import logging
import requests
import json
from typing import Dict, List, Optional
from datetime import datetime
import pandas as pd

logger = logging.getLogger(__name__)


class TelegramWeatherBot:
    """Telegram bot for sending weather forecasts"""
    
    def __init__(self):
        """Initialize Telegram Bot"""
        self.bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.chat_id = os.getenv('TELEGRAM_GROUP_CHAT_ID')
        
        if not self.bot_token:
            raise ValueError("TELEGRAM_BOT_TOKEN environment variable not set")
        if not self.chat_id:
            raise ValueError("TELEGRAM_GROUP_CHAT_ID environment variable not set")
        
        self.api_url = f"https://api.telegram.org/bot{self.bot_token}"
        
        # Weather code to emoji mapping
        self.weather_emojis = {
            'clear': '☀️',
            'partly_cloudy': '⛅',
            'cloudy': '☁️',
            'overcast': '☁️',
            'fog': '🌫️',
            'light_rain': '🌦️',
            'moderate_rain': '🌧️',
            'heavy_rain': '⛈️',
            'light_drizzle': '🌦️',
            'thunderstorm': '⛈️',
            'snow': '🌨️'
        }
    
    def send_message(self, message: str, parse_mode: str = 'HTML') -> bool:
        """
        Send message to Telegram
        
        Args:
            message: Message text to send
            parse_mode: Message formatting (HTML or Markdown)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            url = f"{self.api_url}/sendMessage"
            payload = {
                'chat_id': self.chat_id,
                'text': message,
                'parse_mode': parse_mode
            }
            
            response = requests.post(url, json=payload, timeout=10)
            response.raise_for_status()
            
            logger.info("Message sent successfully to Telegram")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to send Telegram message: {str(e)}")
            return False
    
    def format_forecast_message(
        self, 
        predictions: Dict[int, pd.DataFrame],
        current_weather: Optional[Dict] = None
    ) -> str:
        """
        Format weather forecast into a nice Telegram message
        
        Args:
            predictions: Dictionary of location_id -> prediction DataFrame
            current_weather: Optional current weather data
            
        Returns:
            Formatted message string
        """
        # Load location names
        try:
            with open('/opt/airflow/config/locations.json', 'r', encoding='utf-8') as f:
                config = json.load(f)
            location_map = {city['id']: city['name'] for city in config['cities']}
        except:
            location_map = {}
        
        # Header
        message_parts = [
            "🌤️ <b>DỰ BÁO THỜI TIẾT NGÀY MAI</b>",
            ""
        ]
        
        # Add forecast for each location
        for location_id, forecast_df in predictions.items():
            location_name = location_map.get(location_id, f"Location {location_id}")
            
            # Get tomorrow's forecast (assuming first row is next hour, we want ~24 hours ahead)
            if len(forecast_df) > 0:
                # Get forecast for tomorrow same time
                tomorrow_forecast = forecast_df.iloc[min(23, len(forecast_df)-1)]
                
                temp = tomorrow_forecast['predicted_temperature']
                lower = tomorrow_forecast['lower_bound']
                upper = tomorrow_forecast['upper_bound']
                uncertainty = (upper - lower) / 2
                
                # Get weather emoji (random for now, you can enhance this)
                weather_emoji = self._get_weather_emoji(temp)
                
                # Calculate trend (if current weather available)
                trend = self._calculate_trend(location_id, temp, current_weather)
                
                location_message = [
                    f"━━━━━━━━━━━━━━━━━━━",
                    f"📍 <b>{location_name}</b>",
                    f"🌡️ Nhiệt độ: <b>{temp:.1f}°C</b> (±{uncertainty:.1f}°C)",
                    f"{weather_emoji} Trạng thái: {self._get_weather_status(temp)}",
                    ""
                ]
                
                if trend:
                    location_message.append(f"📊 Xu hướng: {trend}")
                    location_message.append("")
                
                message_parts.extend(location_message)
        
        # Footer
        message_parts.extend([
            "━━━━━━━━━━━━━━━━━━━",
            f"⏰ Cập nhật: {datetime.now().strftime('%H:%M %d/%m/%Y')}",
            "🤖 Powered by Prophet ML Model"
        ])
        
        return "\n".join(message_parts)
    
    def _get_weather_emoji(self, temperature: float) -> str:
        """Get appropriate weather emoji based on temperature"""
        if temperature < 20:
            return "🌥️"
        elif temperature < 25:
            return "⛅"
        elif temperature < 30:
            return "☀️"
        else:
            return "🔥"
    
    def _get_weather_status(self, temperature: float) -> str:
        """Get weather status description"""
        if temperature < 15:
            return "Lạnh"
        elif temperature < 20:
            return "Mát mẻ"
        elif temperature < 25:
            return "Dễ chịu"
        elif temperature < 30:
            return "Ấm áp"
        elif temperature < 35:
            return "Nóng"
        else:
            return "Rất nóng"
    
    def _calculate_trend(
        self, 
        location_id: int, 
        predicted_temp: float,
        current_weather: Optional[Dict]
    ) -> str:
        """Calculate temperature trend compared to today"""
        if not current_weather or location_id not in current_weather:
            return ""
        
        current_temp = current_weather[location_id].get('current', {}).get('temperature')
        if current_temp is None:
            return ""
        
        diff = predicted_temp - current_temp
        
        if abs(diff) < 0.5:
            return "Tương tự hôm nay"
        elif diff > 0:
            return f"Nóng hơn hôm nay {abs(diff):.1f}°C 📈"
        else:
            return f"Mát hơn hôm nay {abs(diff):.1f}°C 📉"
    
    def send_forecast(
        self, 
        predictions: Dict[int, pd.DataFrame],
        current_weather: Optional[Dict] = None
    ) -> bool:
        """
        Send weather forecast to Telegram
        
        Args:
            predictions: Prediction data
            current_weather: Current weather data
            
        Returns:
            True if successful
        """
        message = self.format_forecast_message(predictions, current_weather)
        return self.send_message(message)
    
    def send_alert(self, alert_message: str) -> bool:
        """
        Send weather alert/warning
        
        Args:
            alert_message: Alert text
            
        Returns:
            True if successful
        """
        formatted_alert = f"⚠️ <b>CẢNH BÁO THỜI TIẾT</b>\n\n{alert_message}"
        return self.send_message(formatted_alert)
    
    def send_training_report(self, training_results: Dict) -> bool:
        """
        Send model training report
        
        Args:
            training_results: Results from model training
            
        Returns:
            True if successful
        """
        # Load location names
        try:
            with open('/opt/airflow/config/locations.json', 'r', encoding='utf-8') as f:
                config = json.load(f)
            location_map = {city['id']: city['name'] for city in config['cities']}
        except:
            location_map = {}
        
        message_parts = [
            "🤖 <b>BÁO CÁO HUẤN LUYỆN MODEL</b>",
            ""
        ]
        
        # Trained models
        trained_count = len(training_results.get('trained_models', []))
        message_parts.append(f"✅ Đã train: <b>{trained_count} models</b>")
        message_parts.append("")
        
        # Metrics for each location
        metrics = training_results.get('metrics', {})
        for location_id, metric in metrics.items():
            location_name = location_map.get(location_id, f"Location {location_id}")
            message_parts.extend([
                f"📍 <b>{location_name}</b>",
                f"   • MAE: {metric.get('mae', 0):.2f}°C",
                f"   • RMSE: {metric.get('rmse', 0):.2f}°C",
                f"   • R²: {metric.get('r2_score', 0):.3f}",
                ""
            ])
        
        # Errors
        errors = training_results.get('errors', [])
        if errors:
            message_parts.append("❌ <b>Lỗi:</b>")
            for error in errors[:3]:  # Show max 3 errors
                message_parts.append(f"   • {error}")
            message_parts.append("")
        
        message_parts.append(f"⏰ {datetime.now().strftime('%H:%M %d/%m/%Y')}")
        
        return self.send_message("\n".join(message_parts))
    
    def send_error_notification(self, error_message: str, job_name: str) -> bool:
        """
        Send error notification
        
        Args:
            error_message: Error description
            job_name: Name of the failed job
            
        Returns:
            True if successful
        """
        message = (
            f"❌ <b>LỖI HỆ THỐNG</b>\n\n"
            f"Job: <code>{job_name}</code>\n"
            f"Lỗi: {error_message}\n\n"
            f"⏰ {datetime.now().strftime('%H:%M %d/%m/%Y')}"
        )
        return self.send_message(message)
    
    def test_connection(self) -> bool:
        """
        Test Telegram bot connection
        
        Returns:
            True if bot is working
        """
        try:
            url = f"{self.api_url}/getMe"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            bot_info = response.json()
            if bot_info.get('ok'):
                logger.info(f"Bot connected: {bot_info.get('result', {}).get('username')}")
                return True
            else:
                logger.error("Bot connection failed")
                return False
                
        except Exception as e:
            logger.error(f"Bot test failed: {str(e)}")
            return False


def send_daily_forecast(predictions: Dict[int, pd.DataFrame]) -> bool:
    """
    Utility function to send daily forecast
    Used in Airflow DAG
    """
    try:
        bot = TelegramWeatherBot()
        
        # Test connection first
        if not bot.test_connection():
            logger.error("Bot connection test failed")
            return False
        
        # Send forecast
        success = bot.send_forecast(predictions)
        return success
        
    except Exception as e:
        logger.error(f"Error sending forecast: {str(e)}")
        return False