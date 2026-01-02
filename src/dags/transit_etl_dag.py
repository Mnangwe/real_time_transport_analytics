from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import logging

default_args = {
    'owner': 'azabenathi',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def extract_staging_events(**context):

    execution_date = context["logical_date"].date() 

    pg_hook = PostgresHook(postgres_conn_id="transit_postgres")
    conn = pg_hook.get_conn()

    query = f"""
        SELECT 
            vehicle_id,
            line_id,
            destination,
            stop_id,
            expected_arrival,
            time_to_station,
            timestamp as event_timestamp,
            processing_time
        FROM staging_transit_events
        WHERE ingestion_date = %s
        AND processed = FALSE
        ORDER BY processing_time
    """

    df = pd.read_sql(query, conn, params=(execution_date,))

    if len(df) == 0:
        logging.warning(f"No events found for {execution_date}")
        return None

    file_path = f"/tmp/transit_events_{execution_date}.parquet"
    df.to_parquet(file_path, index=False)
    logging.info(f"Extracted {len(df)} events to {file_path}")

    conn.close()

    return file_path


def transform_and_aggregate(**context):
    """Transform raw events and create aggregations"""
    execution_date = context["logical_date"].date()
    
    file_path = context["task_instance"].xcom_pull(task_ids="extract_staging_events")

    if file_path is None:
        logging.warning("No data to transform")
        return None

    df = pd.read_parquet(file_path)

    # Clean data
    df["expected_arrival"] = pd.to_datetime(df["expected_arrival"])
    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"])
    df["event_hour"] = df["event_timestamp"].dt.hour
    df["event_date"] = df["event_timestamp"].dt.date

    # Calculate delays
    df["delay_seconds"] = df["time_to_station"] - 60  # Assume baseline 60s
    df["is_delayed"] = df["delay_seconds"] > 300  # > 5 minutes

    # Hourly aggregations by line
    hourly_agg = (
        df.groupby(["line_id", "event_date", "event_hour"])
        .agg(
            {
                "vehicle_id": "count",  # total arrivals
                "time_to_station": ["mean", "max", "min"],
                "delay_seconds": ["mean", "std"],
                "is_delayed": "sum",  # count of delayed
            }
        )
        .reset_index()
    )

    hourly_agg.columns = [
        "line_id",
        "event_date",
        "hour",
        "total_arrivals",
        "avg_time_to_station",
        "max_time_to_station",
        "min_time_to_station",
        "avg_delay",
        "std_delay",
        "delayed_count",
    ]

    # Calculate on-time percentage
    hourly_agg["on_time_percentage"] = (
        (hourly_agg["total_arrivals"] - hourly_agg["delayed_count"])
        / hourly_agg["total_arrivals"]
        * 100
    )

    # Save transformed data
    transform_path = f"/tmp/transit_transformed_{execution_date}.parquet"
    df.to_parquet(transform_path, index=False)

    agg_path = f"/tmp/transit_hourly_agg_{execution_date}.parquet"
    hourly_agg.to_parquet(agg_path, index=False)

    transformed_data = {"transformed": transform_path, "aggregated": agg_path}

    return transformed_data


def upsert_dimensions(**context):
    """Update dimension tables with SCD Type 2 logic"""
    execution_date = context["logical_date"]    

    paths = context["task_instance"].xcom_pull(task_ids="transform_and_aggregate")

    if paths is None:
        return

    df = pd.read_parquet(paths["transformed"])

    pg_hook = PostgresHook(postgres_conn_id="transit_postgres")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Get unique lines from batch
    unique_lines = df[["line_id", "destination"]].drop_duplicates()

    for _, row in unique_lines.iterrows():
        line_id = row["line_id"]
        destination = row["destination"]

        # Check if line exists with current destination
        cursor.execute(
            """
            SELECT line_key, line_name 
            FROM dim_transit_lines 
            WHERE line_id = %s AND is_current = TRUE
        """,
            (line_id,),
        )

        result = cursor.fetchone()

        if result:
            line_key, current_dest = result

            # If destination changed, implement SCD Type 2
            if current_dest != destination:
                # Close current record
                cursor.execute(
                    """
                    UPDATE dim_transit_lines
                    SET is_current = FALSE,
                        valid_to = %s
                    WHERE line_key = %s
                """,
                    (execution_date, line_key),
                )

                # Insert new record
                cursor.execute(
                    """
                    INSERT INTO dim_transit_lines 
                    (line_id, line_name, line_type, valid_from, is_current)
                    VALUES (%s, %s, 'bus', %s, TRUE)
                """,
                    (line_id, destination, execution_date),
                )

                logging.info(f"SCD Type 2: Updated line {line_id}")
        else:
            # New line - insert
            cursor.execute(
                """
                INSERT INTO dim_transit_lines 
                (line_id, line_name, line_type, valid_from, is_current)
                VALUES (%s, %s, 'bus', %s, TRUE)
            """,
                (line_id, destination, execution_date),
            )

            logging.info(f"New line inserted: {line_id}")

    # Similar logic for dim_stops
    unique_stops = df["stop_id"].unique()
    for stop_id in unique_stops:
        # Check if stop exists and is current
        cursor.execute(
            """
            SELECT stop_key FROM dim_stops 
            WHERE stop_id = %s AND is_current = TRUE
        """,
            (stop_id,),
        )

        result = cursor.fetchone()

        if not result:
            # New stop - insert only if doesn't exist as current
            cursor.execute(
                """
                INSERT INTO dim_stops (stop_id, stop_name, valid_from, is_current)
                VALUES (%s, %s, %s, TRUE)
            """,
                (stop_id, f"Stop {stop_id}", execution_date),
            )

            logging.info(f"New stop inserted: {stop_id}")
        else:
            logging.debug(f"Stop {stop_id} already exists")

    conn.commit()
    cursor.close()
    conn.close()


def load_facts(**context):
    """Load fact tables"""
    execution_date = context["logical_date"]    

    paths = context["task_instance"].xcom_pull(task_ids="transform_and_aggregate")

    if paths is None:
        return

    # Load detailed events
    df_events = pd.read_parquet(paths["transformed"])

    pg_hook = PostgresHook(postgres_conn_id="transit_postgres")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Get date_key
    cursor.execute(
        """
        SELECT date_key FROM dim_date 
        WHERE full_date = %s
    """,
        (execution_date,),
    )

    date_key = cursor.fetchone()
    if not date_key:
        # Insert date if doesn't exist
        cursor.execute(
            """
            INSERT INTO dim_date 
            (date_key, full_date, day_of_week, day_name, month, year, is_weekend)
            VALUES 
            (%s, %s, %s, %s, %s, %s, %s)
            RETURNING date_key
        """,
            (
                int(execution_date.strftime("%Y%m%d")),
                execution_date,
                execution_date.weekday(),
                execution_date.strftime("%A"),
                execution_date.month,
                execution_date.year,
                execution_date.weekday() >= 5,
            ),
        )
        date_key = cursor.fetchone()[0]
    else:
        date_key = date_key[0]

    # Insert fact records
    for _, row in df_events.iterrows():
        cursor.execute(
            """
            INSERT INTO fact_transit_events 
            (date_key, line_key, stop_key, vehicle_id, expected_arrival, 
             time_to_station, delay_seconds, event_timestamp)
            SELECT 
                %s,
                l.line_key,
                s.stop_key,
                %s, %s, %s, %s, %s
            FROM dim_transit_lines l
            CROSS JOIN dim_stops s
            WHERE l.line_id = %s AND l.is_current = TRUE
            AND s.stop_id = %s AND s.is_current = TRUE
        """,
            (
                date_key,
                row["vehicle_id"],
                row["expected_arrival"],
                row["time_to_station"],
                row["delay_seconds"],
                row["event_timestamp"],
                row["line_id"],
                row["stop_id"],
            ),
        )

    # Load hourly aggregations
    df_agg = pd.read_parquet(paths["aggregated"])

    for _, row in df_agg.iterrows():
        cursor.execute(
            """
            INSERT INTO fact_hourly_metrics 
            (date_key, hour, line_key, total_arrivals, avg_delay_seconds, 
             max_delay_seconds, on_time_percentage)
            SELECT 
                %s, %s, l.line_key, %s, %s, %s, %s
            FROM dim_transit_lines l
            WHERE l.line_id = %s AND l.is_current = TRUE
        """,
            (
                date_key,
                row["hour"],
                row["total_arrivals"],
                row["avg_delay"],
                row["max_time_to_station"],
                row["on_time_percentage"],
                row["line_id"],
            ),
        )

    conn.commit()
    cursor.close()
    conn.close()


def mark_processed(**context):
    """Mark staging records as processed"""
    execution_date = context["logical_date"]    

    pg_hook = PostgresHook(postgres_conn_id="transit_postgres")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute(
        """
        UPDATE staging_transit_events
        SET processed = TRUE
        WHERE ingestion_date = %s
    """,
        (execution_date,),
    )

    rows_updated = cursor.rowcount
    conn.commit()
    cursor.close()
    conn.close()

    logging.info(f"Marked {rows_updated} records as processed")


with DAG(
    'transit_etl_daily',
    default_args=default_args,
    description='Daily ETL for transit data warehouse',
    schedule_interval='0 2 * * *',
    start_date=datetime(2025,11,15),
    catchup=False,
    tags=['transit', 'etl', 'warehouse']
) as dag:

    extract = PythonOperator(
        task_id='extract_staging_events',
        python_callable=extract_staging_events,
    )

    transform = PythonOperator(
        task_id="transform_and_aggregate",
        python_callable=transform_and_aggregate,
    )

    upsert_dims = PythonOperator(
        task_id="upsert_dimensions",
        python_callable=upsert_dimensions,
    )

    load = PythonOperator(
        task_id="load_facts",
        python_callable=load_facts,
    )

    mark_done = PythonOperator(
        task_id="mark_processed",
        python_callable=mark_processed,
    )

    data_quality = PostgresOperator(
        task_id='data_quality_check',
        postgres_conn_id='transit_postgres',
        sql="""
            SELECT
                COUNT(*) as record_count,
                COUNT(DISTINCT vehicle_id) as unique_lines
            FROM fact_transit_events
            WHERE DATE(event_timestamp) = '{{ ds }}'
            HAVING COUNT(*) < 100; 
        """
    )

    extract >> transform >> upsert_dims >> load >> mark_done >> data_quality 
