# Kafka User Activity Pipeline

An event-driven data pipeline built with Apache Kafka, Python, and PostgreSQL.

## Overview

This project simulates a real-time user activity tracking system.
- **Producer**: Generates valid `user_activity` events (views, clicks, purchases) and streams them to Kafka.
- **Consumer**: Validates events against a JSON schema and aggregates daily stats (view_count, click_count, purchase_count) per user/item into Postgres.
- **API**: Serves the aggregated daily summary for a given user via a REST endpoint.

## Architecture

- **Services**:
    - `soookeeper`: Orchestration for Kafka.
    - `kafka`: Message broker.
    - `postgres`: Persistent storage for aggregated data.
    - `producer_service`: Python script generating events.
    - `consumer_service`: Python script processing and aggregating events.
    - `api_service`: FastAPI application exposing data.

## Setup & Running

1. **Prerequisites**:
    - Docker & Docker Compose installed.

2. **Start the Stack**:
    ```bash
    docker-compose up --build
    ```
    Wait for all services to be healthy. The producer will start sending events automatically (default 2 events/sec).

3. **Check API**:
    You can query the API for a specific user ID.
    ```bash
    curl http://localhost:8000/api/user-activity-summary/<UUID>
    ```

    Note: Check `docker-compose` logs to see generic generated UUIDs, or use the seeded user:
    ```bash
    curl http://localhost:8000/api/user-activity-summary/11111111-1111-1111-1111-111111111111
    ```

## Development & Testing

### Running Tests
Tests are located in `tests/`.

**Unit Tests**:
```bash
# Install dependencies
pip install -r services/consumer/requirements.txt
pip install -r services/producer/requirements.txt # if needed
pip install pytest requests

pytest tests/unit
```

**Integration Tests**:
Requires the full docker stack to be running.
```bash
pytest tests/integration
```

## Configuration

Environment variables are managed via `.env` files. See `.env.example` for defaults.
- `PRODUCER_EVENT_RATE`: Events per second.
- `KAFKA_TOPIC_USER_ACTIVITIES`: KAFKA_TOPIC_USER_ACTIVITIES: Name of the Kafka topic where user_activity events are produced and consumed.
.
