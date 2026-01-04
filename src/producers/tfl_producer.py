import requests
import json
from kafka import KafkaProducer
import time
from datetime import datetime
import os
from dotenv import load_dotenv
import logging

load_dotenv()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

class TfLProducer:

    def __init__(self, kafka_broker="localhost:9092"):
        self.producer = KafkaProducer(
            bootstrap_servers=[kafka_broker],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        self.api_base = os.getenv("BASE_URL")

    def fetch_bus_arrivals(self, stop_id="940GZZLUASL"):
        """Fetch real-time bus arrivals for a stop"""
        url = f"{self.api_base}/StopPoint/{stop_id}/Arrivals"
        response = requests.get(url)

        if response.status_code == 200:
            arrivals = response.json()
            for arrival in arrivals:
                enriched = {
                    "vehicle_id": arrival.get("vehicleId"),
                    "line_id": arrival.get("lineName"),
                    "destination": arrival.get("destinationName"),
                    "current_location": arrival.get("currentLocation"),
                    "expected_arrival": arrival.get("expectedArrival"),
                    "time_to_station": arrival.get("timeToStation"),
                    "timestamp": datetime.utcnow().isoformat(),
                    "stop_id": stop_id,
                }

                # Send to Kafka
                self.producer.send("transit_events", value=enriched)
                print(f"Sent: {enriched['line_id']} → {enriched['destination']}")

        self.producer.flush()

    def stream_continuous(self, interval=45):
        """Stream data every 30 seconds"""
        logger.info("Starting TfL producer...")
        logger.info(f"Fetching data every {interval} seconds")
        logger.info("Press Ctrl+C to stop")

        try:
            while True:
                self.fetch_bus_arrivals()
                self.producer.flush()
                time.sleep(interval)

        except KeyboardInterrupt:
            logger.info("Shutting down producer...")
        finally:
            self.producer.flush(timeout=10)
            logger.info("Producer closed")

if __name__ == "__main__":
    producer = TfLProducer()
    producer.stream_continuous()
