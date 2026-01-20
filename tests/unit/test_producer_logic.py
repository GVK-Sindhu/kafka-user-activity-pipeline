import pytest
from app.producer import generate_event
from uuid import UUID
from datetime import datetime

def test_generate_event_structure():
    event = generate_event()
    assert isinstance(event, dict)
    assert "user_id" in event
    assert "activity_type" in event
    assert "item_id" in event
    assert "timestamp" in event

def test_generate_event_values():
    event = generate_event()
    
    # Check UUIDs
    assert UUID(event["user_id"])
    assert UUID(event["item_id"])
    
    # Check Activity Type
    assert event["activity_type"] in ["view", "click", "purchase"]
    
    # Check Timestamp
    try:
        # Should be ISO 8601
        datetime.fromisoformat(event["timestamp"])
    except ValueError:
        pytest.fail("Timestamp is not valid ISO 8601")
