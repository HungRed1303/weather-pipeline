-- Weather Forecast Database Schema
-- Author: Weather Forecast System
-- Date: 2024-12-06

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- =====================================================
-- Table 1: Locations (Thành phố theo dõi)
-- =====================================================
CREATE TABLE IF NOT EXISTS locations (
    location_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    latitude DECIMAL(10, 7) NOT NULL,
    longitude DECIMAL(10, 7) NOT NULL,
    timezone VARCHAR(50) NOT NULL DEFAULT 'Asia/Ho_Chi_Minh',
    country VARCHAR(50) DEFAULT 'Vietnam',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert default cities
INSERT INTO locations (name, latitude, longitude) VALUES
    ('Huế', 16.4637, 107.5909),
    ('Đà Nẵng', 16.0544, 108.2022),
    ('TP.HCM', 10.8231, 106.6297)
ON CONFLICT (name) DO NOTHING;

-- =====================================================
-- Table 2: Current Weather (Thời tiết realtime)
-- =====================================================
CREATE TABLE IF NOT EXISTS current_weather (
    id SERIAL PRIMARY KEY,
    location_id INTEGER NOT NULL REFERENCES locations(location_id) ON DELETE CASCADE,
    timestamp TIMESTAMP NOT NULL,
    temperature DECIMAL(5, 2),
    relative_humidity INTEGER,
    weather_code INTEGER,
    wind_speed DECIMAL(5, 2),
    wind_direction INTEGER,
    precipitation DECIMAL(6, 2),
    pressure DECIMAL(7, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(location_id, timestamp)
);

-- Index for faster queries
CREATE INDEX idx_current_weather_location_time ON current_weather(location_id, timestamp DESC);
CREATE INDEX idx_current_weather_created ON current_weather(created_at DESC);

-- =====================================================
-- Table 3: Hourly Weather (Dữ liệu cho ML training)
-- =====================================================
CREATE TABLE IF NOT EXISTS hourly_weather (
    id SERIAL PRIMARY KEY,
    location_id INTEGER NOT NULL REFERENCES locations(location_id) ON DELETE CASCADE,
    forecast_timestamp TIMESTAMP NOT NULL,
    temperature DECIMAL(5, 2),
    precipitation DECIMAL(6, 2),
    weather_code INTEGER,
    cloud_cover INTEGER,
    
    -- Engineered Features
    temp_change_1h DECIMAL(5, 2),
    temp_rolling_avg_3h DECIMAL(5, 2),
    temp_rolling_std_3h DECIMAL(5, 2),
    
    -- Time Features
    hour INTEGER,
    day_of_week INTEGER,
    month INTEGER,
    is_weekend BOOLEAN,
    is_morning BOOLEAN,
    is_afternoon BOOLEAN,
    is_evening BOOLEAN,
    
    -- Lag Features
    temp_lag_1h DECIMAL(5, 2),
    temp_lag_3h DECIMAL(5, 2),
    temp_lag_24h DECIMAL(5, 2),
    
    -- Precipitation Features
    is_raining BOOLEAN,
    rain_intensity VARCHAR(20),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(location_id, forecast_timestamp)
);

-- Indexes
CREATE INDEX idx_hourly_weather_location_time ON hourly_weather(location_id, forecast_timestamp DESC);
CREATE INDEX idx_hourly_weather_training ON hourly_weather(location_id, forecast_timestamp) 
    WHERE forecast_timestamp < CURRENT_TIMESTAMP;

-- =====================================================
-- Table 4: Daily Weather Summary (Tổng hợp theo ngày)
-- =====================================================
CREATE TABLE IF NOT EXISTS daily_weather_summary (
    id SERIAL PRIMARY KEY,
    location_id INTEGER NOT NULL REFERENCES locations(location_id) ON DELETE CASCADE,
    date DATE NOT NULL,
    temp_max DECIMAL(5, 2),
    temp_min DECIMAL(5, 2),
    temp_avg DECIMAL(5, 2),
    temp_std DECIMAL(5, 2),
    precipitation_total DECIMAL(6, 2),
    precipitation_hours INTEGER,
    avg_humidity DECIMAL(5, 2),
    avg_wind_speed DECIMAL(5, 2),
    dominant_weather_code INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(location_id, date)
);

-- Index
CREATE INDEX idx_daily_summary_location_date ON daily_weather_summary(location_id, date DESC);

-- =====================================================
-- Table 5: Weather Predictions (Kết quả dự báo từ ML)
-- =====================================================
CREATE TABLE IF NOT EXISTS weather_predictions (
    id SERIAL PRIMARY KEY,
    location_id INTEGER NOT NULL REFERENCES locations(location_id) ON DELETE CASCADE,
    prediction_timestamp TIMESTAMP NOT NULL,
    predicted_temperature DECIMAL(5, 2) NOT NULL,
    prediction_lower DECIMAL(5, 2),  -- Confidence interval lower bound
    prediction_upper DECIMAL(5, 2),  -- Confidence interval upper bound
    actual_temperature DECIMAL(5, 2),  -- Filled in later for evaluation
    error DECIMAL(5, 2),  -- |predicted - actual|
    model_version VARCHAR(50) NOT NULL,
    model_params JSONB,  -- Store model hyperparameters
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    evaluated_at TIMESTAMP,
    UNIQUE(location_id, prediction_timestamp, model_version, created_at)
);

-- Indexes
CREATE INDEX idx_predictions_location_time ON weather_predictions(location_id, prediction_timestamp DESC);
CREATE INDEX idx_predictions_evaluation ON weather_predictions(location_id, evaluated_at) 
    WHERE actual_temperature IS NOT NULL;

-- =====================================================
-- Table 6: Model Performance Metrics
-- =====================================================
CREATE TABLE IF NOT EXISTS model_metrics (
    id SERIAL PRIMARY KEY,
    location_id INTEGER NOT NULL REFERENCES locations(location_id) ON DELETE CASCADE,
    model_version VARCHAR(50) NOT NULL,
    evaluation_date DATE NOT NULL,
    mae DECIMAL(6, 3),  -- Mean Absolute Error
    rmse DECIMAL(6, 3),  -- Root Mean Square Error
    mape DECIMAL(6, 3),  -- Mean Absolute Percentage Error
    r2_score DECIMAL(6, 4),  -- R-squared
    sample_size INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(location_id, model_version, evaluation_date)
);

-- =====================================================
-- Table 7: ETL Job Logs
-- =====================================================
CREATE TABLE IF NOT EXISTS etl_job_logs (
    id SERIAL PRIMARY KEY,
    job_name VARCHAR(100) NOT NULL,
    job_type VARCHAR(50) NOT NULL,  -- 'ingestion', 'transform', 'ml_training', 'prediction'
    status VARCHAR(20) NOT NULL,  -- 'success', 'failed', 'running'
    records_processed INTEGER,
    error_message TEXT,
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    duration_seconds INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_etl_logs_job_time ON etl_job_logs(job_name, started_at DESC);

-- =====================================================
-- Views for Analytics
-- =====================================================

-- View: Latest weather for all locations
CREATE OR REPLACE VIEW v_latest_weather AS
SELECT 
    l.name as city,
    l.latitude,
    l.longitude,
    cw.timestamp,
    cw.temperature,
    cw.relative_humidity,
    cw.weather_code,
    cw.precipitation,
    cw.wind_speed
FROM current_weather cw
INNER JOIN locations l ON cw.location_id = l.location_id
WHERE cw.timestamp = (
    SELECT MAX(timestamp) 
    FROM current_weather cw2 
    WHERE cw2.location_id = cw.location_id
);

-- View: Model accuracy by location
CREATE OR REPLACE VIEW v_model_accuracy AS
SELECT 
    l.name as city,
    wp.model_version,
    COUNT(*) as prediction_count,
    AVG(ABS(wp.predicted_temperature - wp.actual_temperature)) as avg_error,
    STDDEV(ABS(wp.predicted_temperature - wp.actual_temperature)) as std_error,
    MAX(wp.evaluated_at) as last_evaluation
FROM weather_predictions wp
INNER JOIN locations l ON wp.location_id = l.location_id
WHERE wp.actual_temperature IS NOT NULL
GROUP BY l.name, wp.model_version;

-- =====================================================
-- Functions & Triggers
-- =====================================================

-- Function: Auto-update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger for locations table
CREATE TRIGGER update_locations_updated_at 
    BEFORE UPDATE ON locations
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Function: Calculate prediction error when actual data arrives
CREATE OR REPLACE FUNCTION calculate_prediction_error()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.actual_temperature IS NOT NULL AND OLD.actual_temperature IS NULL THEN
        NEW.error = ABS(NEW.predicted_temperature - NEW.actual_temperature);
        NEW.evaluated_at = CURRENT_TIMESTAMP;
    END IF;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger for weather_predictions
CREATE TRIGGER calculate_error_on_actual_update
    BEFORE UPDATE ON weather_predictions
    FOR EACH ROW
    EXECUTE FUNCTION calculate_prediction_error();

-- =====================================================
-- Data Retention Policies (Optional - Run manually)
-- =====================================================

-- Delete old hourly data (keep 30 days)
-- Run this in a scheduled job
CREATE OR REPLACE FUNCTION cleanup_old_hourly_data()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM hourly_weather
    WHERE created_at < CURRENT_TIMESTAMP - INTERVAL '30 days';
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Delete old current weather (keep 7 days)
CREATE OR REPLACE FUNCTION cleanup_old_current_weather()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM current_weather
    WHERE created_at < CURRENT_TIMESTAMP - INTERVAL '7 days';
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- Grant Permissions
-- =====================================================
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO airflow;

-- =====================================================
-- Initial Data Validation
-- =====================================================
DO $$
BEGIN
    RAISE NOTICE 'Database initialization complete!';
    RAISE NOTICE 'Locations count: %', (SELECT COUNT(*) FROM locations);
END $$;