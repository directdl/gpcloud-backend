"""Database factory for Flask application factory pattern."""

from typing import Optional
from plugins.database import Database

_database_instance: Optional[Database] = None


def init_database() -> Database:
    """Initialize database instance (call from Flask app factory)."""
    global _database_instance
    if _database_instance is None:
        _database_instance = Database()
    return _database_instance


def get_database() -> Database:
    """Get the current database instance."""
    global _database_instance
    if _database_instance is None:
        raise RuntimeError("Database not initialized. Call init_database() first.")
    return _database_instance


def reset_database() -> None:
    """Reset database instance (for testing)."""
    global _database_instance
    _database_instance = None
