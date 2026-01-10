"""
BuzzHeavier Service - Handles BuzzHeavier-specific database operations and auto-upload logic.
"""

from datetime import datetime, timedelta
from pymongo import ASCENDING
from config import (
    BUZZHEAVIER_AUTO_UPLOAD_ENABLED,
    BUZZHEAVIER_AUTO_UPLOAD_TIME,
    BUZZHEAVIER_AUTO_UPLOAD_UNIT,
)


class BuzzHeavierService:
    """Service class for BuzzHeavier-specific operations."""

    def __init__(self, db_instance):
        """Initialize with database instance."""
        self.db = db_instance

    def get_timedelta(self):
        """Get timedelta for BuzzHeavier auto-upload."""
        auto_upload_time = BUZZHEAVIER_AUTO_UPLOAD_TIME
        auto_upload_unit = BUZZHEAVIER_AUTO_UPLOAD_UNIT.lower()

        if auto_upload_unit == 'minutes':
            return timedelta(minutes=auto_upload_time)
        elif auto_upload_unit == 'hours':
            return timedelta(hours=auto_upload_time)
        else:  # default to days
            return timedelta(days=auto_upload_time)

    def is_auto_upload_enabled(self):
        """Check if BuzzHeavier auto-upload is enabled."""
        return BUZZHEAVIER_AUTO_UPLOAD_ENABLED

    def get_expired_links(self):
        """Get BuzzHeavier links that need to be re-uploaded."""
        if not self.is_auto_upload_enabled():
            return []

        now = datetime.utcnow()

        query = {
            '$or': [
                {
                    'status': 'completed',
                    'error': None,
                    'buzzheavier_auto_upload_time': {'$lte': now},
                    'buzzheavier_id': {'$ne': None}
                }
                # No retry_pending for BuzzHeavier - it never gets banned
            ]
        }

        # Stream results in small batches to reduce RAM pressure
        return self.db.links.find(query).sort('created_time', ASCENDING).batch_size(200)

    def update_upload_time(self, token, now=None):
        """Update BuzzHeavier upload time after successful upload."""
        if now is None:
            now = datetime.utcnow()

        update_data = {
            'buzzheavier_uploaded_time': now,
            'buzzheavier_auto_upload_time': now + self.get_timedelta()
        }

        return self.db.links.update_one(
            {'token': token},
            {'$set': update_data}
        )

    def get_auto_upload_batch_size(self):
        """Get batch size for auto-upload operations."""
        return 200  # Can be made configurable later if needed
