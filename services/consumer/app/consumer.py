import json
import os
import signal
import sys
import logging
from datetime import datetime
from kafka import KafkaConsumer

from schema import validate_event
from db import upsert_daily_summary, get_connection

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | CONSUMER | %(message)s"
)
logger = logging.getLogger(__name__)

# -----------------------------
# Environment Variables
# -----------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
TOPIC_NAME = os.getenv("KAFKA_TOPIC_USER_ACTIVITIES")
CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP")

if not all([KAFKA_BOOTSTRAP_SERVERS, TOPIC_NAME, CONSUMER_GROUP]):
    logger.error("Kafka environment variables missing")
    sys.exit(1)

# -----------------------------
# Kafka Consumer
# -----------------------------
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id=CONSUMER_GROUP,
    enable_auto_commit=False,
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

running = True

# -----------------------------
# Graceful Shutdown
# -----------------------------
def shutdown_handler(signum, frame):
    global running
    logger.info("Shutdown signal received. Stopping consumer...")
    running = False

signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

# -----------------------------
# Processing Loop
# -----------------------------
logger.info("Kafka Consumer started")

try:
    conn = get_connection()
    logger.info("Database connection established")
    
    while running:
        records = consumer.poll(timeout_ms=1000)

        for _, messages in records.items():
            for message in messages:
                try:
                    event = message.value
                    validate_event(event)

                    upsert_daily_summary(conn, event)

                    consumer.commit()

                    logger.info(
                        f"Processed event: user={event['user_id']} "
                        f"item={event['item_id']} type={event['activity_type']}"
                    )

                except Exception as e:
                    logger.error(f"Failed to process message: {e}")

except Exception as e:
    logger.exception("Consumer crashed")
finally:
    if 'conn' in locals():
        conn.close()
    consumer.close()
    logger.info("Consumer shutdown complete")
