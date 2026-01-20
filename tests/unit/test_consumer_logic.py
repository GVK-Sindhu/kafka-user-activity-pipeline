import pytest
from uuid import uuid4
from datetime import datetime, timezone
from unittest.mock import MagicMock

from app.schema import validate_event
from app.db import upsert_daily_summary

# -------------------------------
# Schema validation tests
# -------------------------------

def test_validate_event_success():
    event = {
        "user_id": str(uuid4()),
        "activity_type": "click",
        "item_id": str(uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    assert validate_event(event) is True


def test_validate_event_missing_field():
    event = {
        "user_id": str(uuid4()),
        "activity_type": "view",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    assert validate_event(event) is False


def test_validate_event_invalid_uuid():
    event = {
        "user_id": "not-a-uuid",
        "activity_type": "view",
        "item_id": str(uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    assert validate_event(event) is False


def test_validate_event_invalid_activity():
    event = {
        "user_id": str(uuid4()),
        "activity_type": "scroll",
        "item_id": str(uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    assert validate_event(event) is False


# -------------------------------
# Aggregation / UPSERT tests
# -------------------------------

def test_aggregation_upsert_called_correctly():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()

    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    event = {
        "user_id": "11111111-1111-1111-1111-111111111111",
        "item_id": "22222222-2222-2222-2222-222222222222",
        "activity_type": "purchase",
        "timestamp": "2026-01-18T10:30:00+00:00"
    }

    upsert_daily_summary(mock_conn, event)

    assert mock_cursor.execute.called
    assert mock_conn.commit.called
