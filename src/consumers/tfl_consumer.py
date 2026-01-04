import json
import logging
from kafka import KafkaConsumer
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

class TransitEventConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            'transit_events',
            bootstrap_servers='localhost:9092',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id='transit-consumer-group',
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            max_poll_records=100
        )

        self.db_conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST'),
            port=os.getenv('POSTGRES_PORT'),
            database=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD')
        )

        self.batch = []
        self.batch_size = 100
        self.stats = {
            'total_processed': 0,
            'total_inserted': 0,
            'errors': 0
        }

        logger.info("Transit Event Consumer initialized")

    def process_event(self, event):
        try:
            processed_event = {
                'vehicle_id': event.get('vehicle_id'),
                'line_id': event.get('line_id'),
                'destination': event.get('destination'),
                'current_location': event.get('current_location'),
                'expected_arrival': event.get('expected_arrival'),
                'time_to_station': event.get('time_to_station'),
                'timestamp': event.get('timestamp'),
                'stop_id': event.get('stop_id'),
                'processing_time': datetime.utcnow().isoformat(),
                'ingestion_date': datetime.utcnow().date().isoformat()
            }

            if not processed_event['vehicle_id'] or not processed_event['line_id']:
                logger.warning(f"Skipping event with missing required fields: {event}")
                return

            return processed_event

        except Exception as e:
            logger.error(f"Error processing event: {e}")
            self.stats['errors'] += 1
            return None

    def insert_batch(self):

        if not self.batch:
            return

        try:
            cursor = self.db_conn.cursor()

            insert_query = """
                INSERT INTO staging_transit_events (
                    vehicle_id, line_id, destination, current_location,
                    expected_arrival, time_to_station, timestamp, stop_id,
                    processing_time, ingestion_date, processed
                )
                VALUES (
                    %(vehicle_id)s, %(line_id)s, %(destination)s, %(current_location)s,
                    %(expected_arrival)s, %(time_to_station)s, %(timestamp)s, %(stop_id)s,
                    %(processing_time)s, %(ingestion_date)s, FALSE
                )
            """

            execute_batch(cursor, insert_query, self.batch)
            self.db_conn.commit()

            batch_size = len(self.batch)
            self.stats['total_inserted'] += batch_size

            logger.info(f"Inserted batch of {batch_size} events. Total: {self.stats['total_inserted']}")

            self.batch = []
            cursor.close()

        except Exception as e:
            logger.error(f"Error inserting batch: {e}")
            self.db_conn.rollback()
            self.stats['errors'] += 1

    def consume(self):
        logger.info("Starting consumption from Kafka...")
        logger.info("Press Ctrl+C to stop")

        try:
            for message in self.consumer:
                event = message.value
                self.stats['total_processed'] += 1

                # Process event
                processed = self.process_event(event)

                if processed:
                    self.batch.append(processed)

                    # Insert batch when size reached
                    if len(self.batch) >= self.batch_size:
                        self.insert_batch()

                # Log stats every 100 messages
                if self.stats['total_processed'] % 100 == 0:
                    logger.info(
                        f"Stats - Processed: {self.stats['total_processed']}, "
                        f"Inserted: {self.stats['total_inserted']}, "
                        f"Errors: {self.stats['errors']}"
                    )
        except KeyboardInterrupt:
            logger.info("Shutting down consumer...")
            # Insert remaining batch
            if self.batch:
                self.insert_batch()

        finally:
            self.cleanup()

    def cleanup(self):
        """Clean up resources"""
        logger.info(
            f"Final Stats - Total Processed: {self.stats['total_processed']}, "
            f"Total Inserted: {self.stats['total_inserted']}, "
            f"Errors: {self.stats['errors']}"
        )

        self.consumer.close()
        self.db_conn.close()
        logger.info("Consumer closed")

if __name__ == "__main__":
    consumer = TransitEventConsumer()
    consumer.consume()
