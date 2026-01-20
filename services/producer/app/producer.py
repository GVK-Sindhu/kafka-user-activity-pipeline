import json
import os
import signal
import sys
import time
import uuid
import random
import logging
from datetime import datetime, timezone
from kafka import KafkaProducer

# -----------------------------
# Logging Configuration
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | PRODUCER | %(message)s"
)
logger = logging.getLogger(__name__)

# -----------------------------
# Environment Variables
# -----------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
TOPIC_NAME = os.getenv("KAFKA_TOPIC_USER_ACTIVITIES")
EVENT_RATE = int(os.getenv("PRODUCER_EVENT_RATE", "2"))

if not KAFKA_BOOTSTRAP_SERVERS or not TOPIC_NAME:
    logger.error("Kafka configuration missing in environment variables")
    sys.exit(1)

# -----------------------------
# Kafka Producer
# -----------------------------
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5
)

running = True

# -----------------------------
# Graceful Shutdown
# -----------------------------
def shutdown_handler(signum, frame):
    global running
    logger.info("Shutdown signal received. Stopping producer...")
    running = False

signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

# -----------------------------
# Event Generator
# -----------------------------
def generate_event():
    return {
        "user_id": str(uuid.uuid4()),
        "activity_type": random.choice(["view", "click", "purchase"]),
        "item_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

# -----------------------------
# Main Loop
# -----------------------------
logger.info("Kafka Producer started")
logger.info(f"Producing events to topic '{TOPIC_NAME}' at ~{EVENT_RATE} events/sec")

try:
    while running:
        event = generate_event()
        producer.send(TOPIC_NAME, value=event)
        logger.info(f"Event sent: {event}")
        time.sleep(1 / EVENT_RATE)
except Exception as e:
    logger.exception("Producer error occurred")
finally:
    try:
        producer.flush()
        producer.close()
    except:
        pass
    logger.info("Producer shutdown complete")
    sys.exit(0)
