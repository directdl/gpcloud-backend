"""Utility helpers for safely serializing complex data structures."""

from datetime import datetime, timedelta
from typing import Any

try:  # bson is optional; fall back gracefully if not installed
    from bson import ObjectId  # type: ignore
except (ImportError, ModuleNotFoundError):  # pragma: no cover - env specific
    ObjectId = None  # type: ignore


def make_serializable(data: Any) -> Any:
    """Recursively convert values into JSON-serializable representations."""

    if isinstance(data, dict):
        return {key: make_serializable(value) for key, value in data.items()}

    if isinstance(data, list):
        return [make_serializable(item) for item in data]

    if isinstance(data, (datetime, timedelta)):
        return data.isoformat()

    if ObjectId is not None and isinstance(data, ObjectId):  # pragma: no branch
        return str(data)

    if hasattr(data, "__class__") and data.__class__.__name__ == "ObjectId":
        return str(data)

    if hasattr(data, "__class__") and "ObjectId" in str(data.__class__):
        return str(data)

    return data

