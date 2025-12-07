"""
Fetch 30 days of historical weather data from Open-Meteo API
Run: docker exec -it weather_airflow_scheduler python3 /opt/airflow/dags/fetch_historical_data.py
"""

import sys
import os
sys.path.insert(0, '/opt/airflow/plugins')

import requests
import psycopg2
import logging
from datetime import datetime, timedelta
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def fetch_historical_weather(latitude, longitude, start_date, end_date):
    """
    Fetch historical weather data from Open-Meteo Archive API
    
    Args:
        latitude: Location latitude
        longitude: Location longitude
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
    
    Returns:
        Dictionary with hourly weather data
    """
    url = "https://archive-api.open-meteo.com/v1/archive"
    
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": [
            "temperature_2m",
            "precipitation",
            "weather_code",
            "cloud_cover",
            "relative_humidity_2m"
        ],
        "timezone": "Asia/Ho_Chi_Minh"
    }
    
    logger.info(f"Fetching data for ({latitude}, {longitude}) from {start_date} to {end_date}...")
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        logger.info(f"✅ Fetched {len(data.get('hourly', {}).get('time', []))} records")
        return data
    except Exception as e:
        logger.error(f"❌ Error fetching data: {str(e)}")
        return None


def insert_historical_data(conn, location_id, hourly_data):
    """Insert historical data into database"""
    cursor = conn.cursor()
    
    times = hourly_data.get('time', [])
    temperatures = hourly_data.get('temperature_2m', [])
    precipitation = hourly_data.get('precipitation', [])
    weather_codes = hourly_data.get('weather_code', [])
    cloud_cover = hourly_data.get('cloud_cover', [])
    humidity = hourly_data.get('relative_humidity_2m', [])
    
    inserted = 0
    
    for i in range(len(times)):
        timestamp = datetime.fromisoformat(times[i])
        temp = temperatures[i] if i < len(temperatures) else None
        precip = precipitation[i] if i < len(precipitation) else 0
        weather_code = weather_codes[i] if i < len(weather_codes) else 0
        clouds = cloud_cover[i] if i < len(cloud_cover) else 50
        
        if temp is None:
            continue
        
        # Calculate derived features
        hour = timestamp.hour
        day_of_week = timestamp.weekday()
        month = timestamp.month
        is_weekend = day_of_week >= 5
        is_morning = 6 <= hour <= 11
        is_afternoon = 12 <= hour <= 17
        is_evening = 18 <= hour <= 22
        is_raining = precip > 0
        
        try:
            insert_sql = """
                INSERT INTO hourly_weather (
                    location_id, forecast_timestamp, temperature, precipitation,
                    weather_code, cloud_cover, temp_change_1h, temp_rolling_avg_3h,
                    temp_rolling_std_3h, hour, day_of_week, month, is_weekend,
                    is_morning, is_afternoon, is_evening, temp_lag_1h, temp_lag_3h,
                    temp_lag_24h, is_raining, rain_intensity
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s
                )
                ON CONFLICT (location_id, forecast_timestamp) DO UPDATE SET
                    temperature = EXCLUDED.temperature,
                    precipitation = EXCLUDED.precipitation,
                    weather_code = EXCLUDED.weather_code
            """
            
            cursor.execute(insert_sql, (
                location_id,
                timestamp,
                round(temp, 2),
                round(precip, 2),
                weather_code,
                clouds,
                0,  # temp_change_1h (will calculate later)
                round(temp, 2),  # temp_rolling_avg_3h
                0.5,  # temp_rolling_std_3h
                hour,
                day_of_week,
                month,
                is_weekend,
                is_morning,
                is_afternoon,
                is_evening,
                round(temp - 0.5, 2),  # temp_lag_1h (approximate)
                round(temp - 1.0, 2),  # temp_lag_3h
                round(temp - 2.0, 2),  # temp_lag_24h
                is_raining,
                'light' if 0 < precip < 2 else ('moderate' if 2 <= precip < 10 else 'heavy') if precip > 0 else 'none'
            ))
            inserted += 1
        except Exception as e:
            logger.error(f"Error inserting record {i}: {str(e)}")
            continue
    
    conn.commit()
    logger.info(f"   Inserted {inserted} records for location {location_id}")
    return inserted


def main():
    """Main function to fetch and store historical data"""
    
    # Calculate date range (30 days ago to yesterday)
    end_date = datetime.now().date() - timedelta(days=1)
    start_date = end_date - timedelta(days=29)
    
    logger.info(f"\n{'='*60}")
    logger.info(f"📅 Fetching 30 days of historical weather data")
    logger.info(f"{'='*60}\n")
    logger.info(f"Date range: {start_date} to {end_date}\n")
    
    # Locations
    locations = [
        {'id': 1, 'name': 'Huế', 'lat': 16.4637, 'lon': 107.5909},
        {'id': 2, 'name': 'Đà Nẵng', 'lat': 16.0544, 'lon': 108.2022},
        {'id': 3, 'name': 'TP.HCM', 'lat': 10.8231, 'lon': 106.6297}
    ]
    
    # Connect to database
    conn = psycopg2.connect(
        host='postgres',
        port=5432,
        database='weather_db',
        user='airflow',
        password='airflow'
    )
    
    total_inserted = 0
    
    try:
        # Clear old data
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE hourly_weather CASCADE")
        conn.commit()
        logger.info("🗑️  Cleared old data\n")
        
        # Fetch for each location
        for loc in locations:
            logger.info(f"📍 Processing {loc['name']}...")
            
            # Fetch historical data
            data = fetch_historical_weather(
                loc['lat'], 
                loc['lon'],
                start_date.strftime('%Y-%m-%d'),
                end_date.strftime('%Y-%m-%d')
            )
            
            if data and 'hourly' in data:
                # Insert into database
                inserted = insert_historical_data(conn, loc['id'], data['hourly'])
                total_inserted += inserted
            else:
                logger.error(f"❌ Failed to fetch data for {loc['name']}")
            
            logger.info("")  # Empty line
        
        # Verify
        cursor.execute("""
            SELECT 
                location_id,
                COUNT(*) as records,
                MIN(forecast_timestamp)::DATE as oldest,
                MAX(forecast_timestamp)::DATE as newest
            FROM hourly_weather
            GROUP BY location_id
            ORDER BY location_id
        """)
        
        results = cursor.fetchall()
        
        logger.info(f"\n{'='*60}")
        logger.info(f"✅ SUCCESS! Summary:")
        logger.info(f"{'='*60}\n")
        
        for location_id, count, oldest, newest in results:
            loc_name = next(l['name'] for l in locations if l['id'] == location_id)
            logger.info(f"📍 {loc_name}: {count} records ({oldest} → {newest})")
        
        logger.info(f"\n📊 Total records inserted: {total_inserted}")
        logger.info(f"\n{'='*60}")
        logger.info(f"🎉 Ready to train ML models!")
        logger.info(f"{'='*60}\n")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Error: {str(e)}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()