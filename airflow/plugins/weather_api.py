"""
Weather API Module - Open-Meteo Integration
Handles all API calls to Open-Meteo weather service
"""

import requests
import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import json

logger = logging.getLogger(__name__)


class WeatherAPIClient:
    """Client for Open-Meteo Weather API"""
    
    BASE_URL = "https://api.open-meteo.com/v1/forecast"
    
    # Weather parameters to fetch
    CURRENT_PARAMS = [
        "temperature_2m",
        "relative_humidity_2m",
        "weather_code",
        "wind_speed_10m",
        "wind_direction_10m",
        "precipitation",
        "pressure_msl"
    ]
    
    HOURLY_PARAMS = [
        "temperature_2m",
        "precipitation",
        "weather_code",
        "cloud_cover",
        "wind_speed_10m"
    ]
    
    DAILY_PARAMS = [
        "temperature_2m_max",
        "temperature_2m_min",
        "precipitation_sum",
        "weather_code"
    ]
    
    def __init__(self, timeout: int = 30):
        """
        Initialize Weather API Client
        
        Args:
            timeout: Request timeout in seconds
        """
        self.timeout = timeout
        self.session = requests.Session()
    
    def fetch_weather_data(
        self, 
        latitude: float, 
        longitude: float,
        location_name: str,
        forecast_days: int = 7
    ) -> Optional[Dict]:
        """
        Fetch complete weather data for a location
        
        Args:
            latitude: Location latitude
            longitude: Location longitude
            location_name: Name of the location (for logging)
            forecast_days: Number of days to forecast (max 16)
            
        Returns:
            Dictionary containing current, hourly, and daily weather data
        """
        try:
            params = {
                "latitude": latitude,
                "longitude": longitude,
                "current": ",".join(self.CURRENT_PARAMS),
                "hourly": ",".join(self.HOURLY_PARAMS),
                "daily": ",".join(self.DAILY_PARAMS),
                "timezone": "Asia/Ho_Chi_Minh",
                "forecast_days": forecast_days
            }
            
            logger.info(f"Fetching weather data for {location_name} ({latitude}, {longitude})")
            
            response = self.session.get(
                self.BASE_URL,
                params=params,
                timeout=self.timeout
            )
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"Successfully fetched weather data for {location_name}")
            
            return self._parse_response(data, location_name)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for {location_name}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching data for {location_name}: {str(e)}")
            return None
    
    def _parse_response(self, data: Dict, location_name: str) -> Dict:
        """
        Parse and structure API response
        
        Args:
            data: Raw API response
            location_name: Name of the location
            
        Returns:
            Structured weather data dictionary
        """
        return {
            "location": location_name,
            "current": self._parse_current(data.get("current", {})),
            "hourly": self._parse_hourly(data.get("hourly", {})),
            "daily": self._parse_daily(data.get("daily", {})),
            "fetched_at": datetime.now().isoformat()
        }
    
    def _parse_current(self, current_data: Dict) -> Dict:
        """Parse current weather data"""
        return {
            "timestamp": current_data.get("time"),
            "temperature": current_data.get("temperature_2m"),
            "humidity": current_data.get("relative_humidity_2m"),
            "weather_code": current_data.get("weather_code"),
            "wind_speed": current_data.get("wind_speed_10m"),
            "wind_direction": current_data.get("wind_direction_10m"),
            "precipitation": current_data.get("precipitation"),
            "pressure": current_data.get("pressure_msl")
        }
    
    def _parse_hourly(self, hourly_data: Dict) -> List[Dict]:
        """Parse hourly forecast data"""
        if not hourly_data or "time" not in hourly_data:
            return []
        
        times = hourly_data.get("time", [])
        temperatures = hourly_data.get("temperature_2m", [])
        precipitation = hourly_data.get("precipitation", [])
        weather_codes = hourly_data.get("weather_code", [])
        cloud_cover = hourly_data.get("cloud_cover", [])
        wind_speed = hourly_data.get("wind_speed_10m", [])
        
        # Only keep next 24 hours to save storage
        max_hours = min(24, len(times))
        
        hourly_records = []
        for i in range(max_hours):
            hourly_records.append({
                "timestamp": times[i],
                "temperature": temperatures[i] if i < len(temperatures) else None,
                "precipitation": precipitation[i] if i < len(precipitation) else None,
                "weather_code": weather_codes[i] if i < len(weather_codes) else None,
                "cloud_cover": cloud_cover[i] if i < len(cloud_cover) else None,
                "wind_speed": wind_speed[i] if i < len(wind_speed) else None
            })
        
        return hourly_records
    
    def _parse_daily(self, daily_data: Dict) -> List[Dict]:
        """Parse daily forecast data"""
        if not daily_data or "time" not in daily_data:
            return []
        
        times = daily_data.get("time", [])
        temp_max = daily_data.get("temperature_2m_max", [])
        temp_min = daily_data.get("temperature_2m_min", [])
        precipitation = daily_data.get("precipitation_sum", [])
        weather_codes = daily_data.get("weather_code", [])
        
        daily_records = []
        for i in range(len(times)):
            daily_records.append({
                "date": times[i],
                "temp_max": temp_max[i] if i < len(temp_max) else None,
                "temp_min": temp_min[i] if i < len(temp_min) else None,
                "precipitation": precipitation[i] if i < len(precipitation) else None,
                "weather_code": weather_codes[i] if i < len(weather_codes) else None
            })
        
        return daily_records
    
    def fetch_multiple_locations(self, locations: List[Dict]) -> Dict[str, Dict]:
        """
        Fetch weather data for multiple locations
        
        Args:
            locations: List of location dictionaries with keys:
                      {id, name, latitude, longitude}
        
        Returns:
            Dictionary mapping location_id to weather data
        """
        results = {}
        
        for location in locations:
            location_id = location.get("id")
            location_name = location.get("name")
            latitude = location.get("latitude")
            longitude = location.get("longitude")
            
            if not all([location_id, location_name, latitude, longitude]):
                logger.warning(f"Skipping invalid location: {location}")
                continue
            
            weather_data = self.fetch_weather_data(
                latitude=latitude,
                longitude=longitude,
                location_name=location_name
            )
            
            if weather_data:
                results[location_id] = weather_data
        
        logger.info(f"Fetched data for {len(results)}/{len(locations)} locations")
        return results
    
    @staticmethod
    def get_weather_description(weather_code: int) -> str:
        """
        Convert weather code to human-readable description
        Based on WMO Weather interpretation codes
        """
        weather_codes = {
            0: "Quang đãng",
            1: "Chủ yếu quang đãng",
            2: "Có mây rải rác",
            3: "U ám",
            45: "Sương mù",
            48: "Sương mù đóng băng",
            51: "Mưa phùn nhẹ",
            53: "Mưa phùn vừa",
            55: "Mưa phùn dày đặc",
            61: "Mưa nhẹ",
            63: "Mưa vừa",
            65: "Mưa to",
            71: "Tuyết nhẹ",
            73: "Tuyết vừa",
            75: "Tuyết dày",
            80: "Mưa rào nhẹ",
            81: "Mưa rào vừa",
            82: "Mưa rào to",
            95: "Dông",
            96: "Dông có mưa đá nhẹ",
            99: "Dông có mưa đá to"
        }
        return weather_codes.get(weather_code, "Không xác định")
    
    def validate_response(self, data: Dict) -> bool:
        """
        Validate API response data quality
        
        Args:
            data: Parsed weather data
            
        Returns:
            True if data is valid, False otherwise
        """
        if not data:
            return False
        
        # Check if current weather exists
        if not data.get("current"):
            logger.warning("Missing current weather data")
            return False
        
        # Check temperature is in reasonable range
        temp = data["current"].get("temperature")
        if temp is None or temp < -50 or temp > 60:
            logger.warning(f"Invalid temperature value: {temp}")
            return False
        
        # Check timestamp is recent (within last hour)
        timestamp_str = data["current"].get("timestamp")
        if timestamp_str:
            try:
                timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                if abs((datetime.now() - timestamp).total_seconds()) > 3600:
                    logger.warning(f"Timestamp too old: {timestamp_str}")
                    return False
            except Exception as e:
                logger.warning(f"Invalid timestamp format: {timestamp_str}")
                return False
        
        logger.info("Data validation passed")
        return True


# Utility function for Airflow tasks
def fetch_weather_for_all_cities() -> Dict:
    """
    Fetch weather data for all configured cities
    Returns dictionary of location_id -> weather_data
    """
    # Load locations from config
    with open('/opt/airflow/config/locations.json', 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    locations = config.get("cities", [])
    
    client = WeatherAPIClient()
    weather_data = client.fetch_multiple_locations(locations)
    
    return weather_data