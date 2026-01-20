import pytest
import requests
import time
import os
import json
from uuid import uuid4
from kafka import KafkaProducer
from datetime import datetime, timezone

# Configuration
API_URL = "http://localhost:8000"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093")
TOPIC_NAME = os.getenv("KAFKA_TOPIC_USER_ACTIVITIES", "user_activities")

@pytest.fixture(scope="module")
def kafka_producer():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    yield producer
    producer.close()

def test_health_check():
    response = requests.get(f"{API_URL}/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}

def test_full_pipeline_flow(kafka_producer):
    # 1. Generate unique IDs
    user_id = str(uuid4())
    item_id = str(uuid4())
    activity_type = "purchase"
    timestamp = datetime.now(timezone.utc).isoformat()
    
    event = {
        "user_id": user_id,
        "item_id": item_id,
        "activity_type": activity_type,
        "timestamp": timestamp
    }
    
    # 2. Produce Event
    future = kafka_producer.send(TOPIC_NAME, value=event)
    future.get(timeout=10) # Ensure sent
    
    # 3. Wait for Consumer to Process (Polling loop is 1s, give it a few seconds)
    time.sleep(5)
    
    # 4. Check API
    response = requests.get(f"{API_URL}/api/user-activity-summary/{user_id}")
    
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) >= 1
    
    record = data[0]
    assert record["item_id"] == item_id
    assert record["purchase_count"] == 1
    # Check that other counts are 0 (assuming new unique item/day)
    # If other tests run, this might be tricky, but with unique user_id/item_id pair it should be fine.
    
def test_api_404_response():
    non_existent_user = str(uuid4())
    response = requests.get(f"{API_URL}/api/user-activity-summary/{non_existent_user}")
    assert response.status_code == 404
    assert "No activity found" in response.json()["detail"]

def test_api_invalid_uuid():
    response = requests.get(f"{API_URL}/api/user-activity-summary/not-a-uuid")
    assert response.status_code == 422 # FastAPI validation error
