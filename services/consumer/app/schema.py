from jsonschema import Draft7Validator

USER_ACTIVITY_SCHEMA = {
    "type": "object",
    "properties": {
        "user_id": {"type": "string", "format": "uuid"},
        "activity_type": {
            "type": "string",
            "enum": ["view", "click", "purchase"]
        },
        "item_id": {"type": "string", "format": "uuid"},
        "timestamp": {"type": "string", "format": "date-time"}
    },
    "required": ["user_id", "activity_type", "item_id", "timestamp"]
}

validator = Draft7Validator(USER_ACTIVITY_SCHEMA)

def validate_event(event: dict) -> bool:
    """
    Returns True if event is valid, False otherwise
    """
    errors = sorted(validator.iter_errors(event), key=lambda e: e.path)
    return len(errors) == 0
