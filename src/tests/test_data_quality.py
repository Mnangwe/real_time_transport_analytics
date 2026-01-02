import pytest
import psycopg2
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

load_dotenv()


@pytest.fixture(scope="module")
def db_connection():
    """Provide database connection for tests"""
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", 5432),
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    yield conn
    conn.close()


# ============= COMPLETENESS TESTS =============


def test_staging_table_not_empty(db_connection):
    """Test that staging table has data"""
    cursor = db_connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM staging_transit_events")
    count = cursor.fetchone()[0]
    cursor.close()

    assert count > 0, "Staging table is empty"


def test_no_null_critical_fields(db_connection):
    """Test that critical fields are never NULL"""
    cursor = db_connection.cursor()

    cursor.execute(
        """
        SELECT COUNT(1) FROM staging_transit_events 
        WHERE vehicle_id IS NULL 
           OR line_id IS NULL 
           OR timestamp IS NULL
    """
    )
    null_count = cursor.fetchone()[0]
    cursor.close()

    assert null_count == 0, f"Found {null_count} records with NULL critical fields"


def test_facts_have_foreign_keys(db_connection):
    """Test that all facts have valid dimension keys"""
    cursor = db_connection.cursor()

    cursor.execute(
        """
        SELECT COUNT(1) 
        FROM fact_transit_events f
        LEFT JOIN dim_transit_lines l ON f.line_key = l.line_key
        WHERE l.line_key IS NULL
    """
    )
    orphaned = cursor.fetchone()[0]
    cursor.close()

    assert orphaned == 0, f"Found {orphaned} fact records without valid line_key"


# ============= UNIQUENESS TESTS =============


def test_dimension_surrogate_keys_unique(db_connection):
    """Test that dimension surrogate keys are unique"""
    cursor = db_connection.cursor()

    cursor.execute(
        """
        SELECT line_key, COUNT(1) 
        FROM dim_transit_lines 
        GROUP BY line_key 
        HAVING COUNT(1) > 1
    """
    )
    duplicates = cursor.fetchall()
    cursor.close()

    assert len(duplicates) == 0, f"Found duplicate line_keys: {duplicates}"


def test_only_one_current_record_per_dimension(db_connection):
    """Test SCD Type 2: only one current record per natural key"""
    cursor = db_connection.cursor()

    cursor.execute(
        """
        SELECT line_id, COUNT(1) 
        FROM dim_transit_lines 
        WHERE is_current = TRUE
        GROUP BY line_id 
        HAVING COUNT(1) > 1
    """
    )
    duplicates = cursor.fetchall()
    cursor.close()

    assert (
        len(duplicates) == 0
    ), f"Found {len(duplicates)} line_ids with multiple current records: {duplicates}"


# ============= VALIDITY TESTS =============


def test_time_to_station_in_valid_range(db_connection):
    """Test that time_to_station is reasonable (0 to 2 hours)"""
    cursor = db_connection.cursor()

    cursor.execute(
        """
        SELECT COUNT(1) 
        FROM staging_transit_events 
        WHERE time_to_station < 0 OR time_to_station > 7200
    """
    )
    invalid_count = cursor.fetchone()[0]
    cursor.close()

    assert (
        invalid_count == 0
    ), f"Found {invalid_count} records with invalid time_to_station"


def test_no_future_timestamps(db_connection):
    """Test that no events have future timestamps"""
    cursor = db_connection.cursor()

    cursor.execute(
        """
        SELECT COUNT(1) 
        FROM staging_transit_events 
        WHERE timestamp > CURRENT_TIMESTAMP
    """
    )
    future_count = cursor.fetchone()[0]
    cursor.close()

    assert future_count == 0, f"Found {future_count} events with future timestamps"


def test_scd_valid_dates_consistent(db_connection):
    """Test that SCD Type 2 valid_from/valid_to dates are consistent"""
    cursor = db_connection.cursor()

    # Test 1: valid_to must be after valid_from
    cursor.execute(
        """
        SELECT COUNT(1) 
        FROM dim_transit_lines 
        WHERE valid_to IS NOT NULL 
          AND valid_to < valid_from
    """
    )
    invalid_dates = cursor.fetchone()[0]

    assert (
        invalid_dates == 0
    ), f"Found {invalid_dates} records where valid_to <= valid_from"

    # Test 2: Current records should have NULL valid_to
    cursor.execute(
        """
        SELECT COUNT(1) 
        FROM dim_transit_lines 
        WHERE is_current = TRUE 
          AND valid_to IS NOT NULL
    """
    )
    invalid_current = cursor.fetchone()[0]
    cursor.close()

    assert (
        invalid_current == 0
    ), f"Found {invalid_current} current records with non-NULL valid_to"


# ============= FRESHNESS TESTS =============


def test_data_freshness_last_24_hours(db_connection):
    """Test that data was ingested in the last 24 hours"""
    cursor = db_connection.cursor()

    cursor.execute(
        """
        SELECT MAX(processing_time) 
        FROM staging_transit_events
    """
    )
    last_insert = cursor.fetchone()[0]
    cursor.close()

    if last_insert:
        time_diff = datetime.now() - last_insert.replace(tzinfo=None)
        assert time_diff < timedelta(
            hours=24
        ), f"No data ingested in last 24 hours. Last insert: {last_insert}"


def test_daily_data_volume_reasonable(db_connection):
    """Test that daily data volume is within expected range"""
    cursor = db_connection.cursor()

    cursor.execute(
        """
        SELECT COUNT(1) 
        FROM staging_transit_events 
        WHERE ingestion_date = CURRENT_DATE
    """
    )
    today_count = cursor.fetchone()[0]
    cursor.close()

    # Expect at least 100 events per day if producer is running
    assert (
        today_count >= 100 or today_count == 0
    ), f"Unusually low data volume today: {today_count} records"


# ============= CONSISTENCY TESTS =============


def test_fact_date_keys_exist_in_dim_date(db_connection):
    """Test referential integrity: all date_keys exist in dim_date"""
    cursor = db_connection.cursor()

    cursor.execute(
        """
        SELECT DISTINCT f.date_key 
        FROM fact_transit_events f
        LEFT JOIN dim_date d ON f.date_key = d.date_key
        WHERE d.date_key IS NULL
        LIMIT 5
    """
    )
    missing_dates = cursor.fetchall()
    cursor.close()

    assert (
        len(missing_dates) == 0
    ), f"Found fact records with invalid date_keys: {missing_dates}"


def test_staging_processed_flag_consistency(db_connection):
    """Test that processed records don't get reprocessed"""
    cursor = db_connection.cursor()

    cursor.execute(
        """
        SELECT event_id 
        FROM staging_transit_events 
        WHERE processed = TRUE 
        LIMIT 10
    """
    )
    processed_ids = [row[0] for row in cursor.fetchall()]

    if processed_ids:
        cursor.execute(
            f"""
            SELECT COUNT(1) 
            FROM staging_transit_events 
            WHERE event_id = ANY(%s) 
              AND processed = FALSE
        """,
            (processed_ids,),
        )

        flipped_back = cursor.fetchone()[0]
        cursor.close()

        assert (
            flipped_back == 0
        ), f"Found {flipped_back} records that were marked processed but are now unprocessed"
    else:
        cursor.close()


# ============= STATISTICAL TESTS =============


def test_reasonable_average_delay(db_connection):
    """Test that average delay is within reasonable bounds"""
    cursor = db_connection.cursor()

    cursor.execute(
        """
        SELECT AVG(time_to_station)::INTEGER as avg_time
        FROM staging_transit_events 
        WHERE time_to_station IS NOT NULL
    """
    )
    avg_time = cursor.fetchone()[0]
    cursor.close()

    if avg_time:
        # Average should be between 30 seconds and 20 minutes
        assert (
            30 <= avg_time <= 1200
        ), f"Average time_to_station is unrealistic: {avg_time} seconds"


def test_line_distribution_not_skewed(db_connection):
    """Test that we're getting data from multiple lines"""
    cursor = db_connection.cursor()

    cursor.execute(
        """
        SELECT COUNT(DISTINCT line_id) 
        FROM staging_transit_events
    """
    )
    unique_lines = cursor.fetchone()[0]
    cursor.close()

    assert (
        unique_lines >= 3
    ), f"Only {unique_lines} unique lines found. Expected at least 3"


# ============= BUSINESS LOGIC TESTS =============


def test_warehouse_matches_staging_volume(db_connection):
    """Test that warehouse has expected proportion of staging data"""
    cursor = db_connection.cursor()

    cursor.execute(
        """
        SELECT COUNT(*) FROM staging_transit_events WHERE processed = TRUE
    """
    )
    processed_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(1) FROM fact_transit_events")
    fact_count = cursor.fetchone()[0]
    cursor.close()

    if processed_count > 0:
        ratio = fact_count / processed_count
        assert (
            ratio >= 0.9
        ), f"Only {ratio*100:.1f}% of processed records made it to warehouse"


def test_hourly_metrics_aggregated_correctly(db_connection):
    """Test that hourly metrics match raw data"""
    cursor = db_connection.cursor()

    cursor.execute(
        """
        SELECT date_key, hour 
        FROM fact_hourly_metrics 
        LIMIT 1
    """
    )
    result = cursor.fetchone()

    if result:
        date_key, hour = result

        cursor.execute(
            """
            SELECT total_arrivals 
            FROM fact_hourly_metrics 
            WHERE date_key = %s AND hour = %s
        """,
            (date_key, hour),
        )
        agg_total = cursor.fetchone()[0]

        cursor.execute(
            """
            SELECT COUNT(1) 
            FROM fact_transit_events 
            WHERE date_key = %s 
              AND EXTRACT(HOUR FROM event_timestamp) = %s
        """,
            (date_key, hour),
        )
        actual_count = cursor.fetchone()[0]
        cursor.close()

        assert (
            agg_total == actual_count
        ), f"Hourly metrics mismatch: aggregated={agg_total}, actual={actual_count}"
    else:
        cursor.close()
