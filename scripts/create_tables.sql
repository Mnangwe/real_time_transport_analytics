-- Staging table for raw streaming events
CREATE TABLE staging_transit_events (
    event_id SERIAL PRIMARY KEY,
    vehicle_id VARCHAR(100),
    line_id VARCHAR(50),
    destination VARCHAR(200),
    current_location VARCHAR(200),
    expected_arrival TIMESTAMP,
    time_to_station INTEGER,
    timestamp TIMESTAMP,
    stop_id VARCHAR(50),
    processing_time TIMESTAMP,
    ingestion_date DATE,
    processed BOOLEAN DEFAULT FALSE
);

CREATE INDEX idx_staging_ingestion_date ON staging_transit_events(ingestion_date);
CREATE INDEX idx_staging_processed ON staging_transit_events(processed);


-- Dimension: Transit Lines
CREATE TABLE dim_transit_lines (
    line_key SERIAL PRIMARY KEY,
    line_id VARCHAR(50) NOT NULL,
    line_name VARCHAR(100),
    line_type VARCHAR(50),
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE
);

-- Dimension: Stops
CREATE TABLE dim_stops (
    stop_key SERIAL PRIMARY KEY,
    stop_id VARCHAR(50) NOT NULL,
    stop_name VARCHAR(200),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE
);

-- Dimension: Date
CREATE TABLE dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL,
    day_of_week INTEGER,
    day_name VARCHAR(10),
    week_of_year INTEGER,
    month INTEGER,
    month_name VARCHAR(10),
    quarter INTEGER,
    year INTEGER,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN
);

-- Fact: Transit Events
CREATE TABLE fact_transit_events (
    event_key BIGSERIAL PRIMARY KEY,
    date_key INTEGER REFERENCES dim_date(date_key),
    line_key INTEGER REFERENCES dim_transit_lines(line_key),
    stop_key INTEGER REFERENCES dim_stops(stop_key),
    vehicle_id VARCHAR(100),
    scheduled_arrival TIMESTAMP,
    expected_arrival TIMESTAMP,
    actual_arrival TIMESTAMP,
    time_to_station INTEGER,
    delay_seconds INTEGER,
    event_timestamp TIMESTAMP
);

-- Fact: Hourly Metrics (Aggregated)
CREATE TABLE fact_hourly_metrics (
    metric_key BIGSERIAL PRIMARY KEY,
    date_key INTEGER REFERENCES dim_date(date_key),
    hour INTEGER,
    line_key INTEGER REFERENCES dim_transit_lines(line_key),
    stop_key INTEGER REFERENCES dim_stops(stop_key),
    total_arrivals INTEGER,
    avg_delay_seconds DECIMAL(10, 2),
    max_delay_seconds INTEGER,
    on_time_percentage DECIMAL(5, 2)
);

