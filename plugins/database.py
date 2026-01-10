from pymongo import MongoClient, ASCENDING
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, Iterator, Optional

from utils.validators import ensure_filesize_within_limit
from utils.statuses import (
    STATUS_COMPLETED,
    STATUS_IDLE,
    STATUS_PENDING,
    STATUS_RETRY_PENDING,
)
from config import (
    PIXELDRAIN_AUTO_UPLOAD_TIME,
    PIXELDRAIN_AUTO_UPLOAD_UNIT,
)

load_dotenv()


class FilesizeExceededError(Exception):
    """Raised when the provided file size exceeds configured limits."""

class Database:
    def __init__(self):
        # Initialize MongoDB connection
        mongodb_uri = os.getenv('MONGODB_URI')
        if not mongodb_uri:
            raise ValueError("MONGODB_URI not found in environment variables")
        
        # Initialize optimized MongoDB connection
        self.client = MongoClient(
            mongodb_uri,
            maxPoolSize=50,
            connectTimeoutMS=2500,
            retryWrites=True,
            tlsAllowInvalidCertificates=True  # Fix for RDP SSL issues
        )
        self.db = self.client[os.getenv('DB_NAME', 'link_converter')]
        self.links = self.db.links
        self.bot_status = self.db.bot_status
        self.new_photo_links = self.db.new_photo_links
        self.pd_lost = self.db.pd_lost
        self.jobs = self.db.jobs
        
        # Create optimized indexes for fast queries
        self._create_indexes()
    
    def _create_indexes(self):
        """Create database indexes for optimal performance"""
        # Create indexes for links collection
        self.links.create_index([('token', 1)], unique=True)
        self.links.create_index([('drive_id', 1)])  # Primary lookup index
        self.links.create_index([('created_time', ASCENDING)])
        self.links.create_index([('status', ASCENDING)])
        self.links.create_index([('pixeldrain_auto_upload_time', ASCENDING)])
        self.links.create_index([('gp_id', 1)])  # Index for encrypted Google Photos ID lookup
        
        # Create indexes for new photo links collection
        self.new_photo_links.create_index([('token', 1)], unique=True)
        self.new_photo_links.create_index([('drive_id', 1)])
        self.new_photo_links.create_index([('created_time', ASCENDING)])
        self.new_photo_links.create_index([('status', ASCENDING)])
        
        # Create bot_status indexes
        self.bot_status.create_index([('service', 1)], unique=True)
        
        # Create pd_lost indexes
        self.pd_lost.create_index([('token', 1)], unique=True)
        self.pd_lost.create_index([('drive_id', 1)])
        self.pd_lost.create_index([('created_time', ASCENDING)])

        # Create jobs indexes
        self.jobs.create_index([('status', ASCENDING)])
        self.jobs.create_index([('type', ASCENDING)])
        self.jobs.create_index([('created_at', ASCENDING)])
        self.jobs.create_index([('locked_at', ASCENDING)])
        # TTL index for auto-cleanup of completed/failed jobs after 24 hours
        self.jobs.create_index([('ttl_at', ASCENDING)], expireAfterSeconds=0)
        # Unique constraint to prevent duplicate jobs for same token+type
        self.jobs.create_index([('token', 1), ('type', 1)], unique=True, sparse=True)

    def ping(self) -> bool:
        """Test database connection"""
        try:
            # Use admin command to ping the database
            self.client.admin.command('ping')
            return True
        except Exception as e:
            print(f"Database ping failed: {str(e)}")
            return False

    def save_link(
        self,
        token: str,
        drive_id: str,
        status: str = STATUS_PENDING,
        filename: Optional[str] = None,
        filesize: Optional[str] = None,
        filetype: Optional[str] = None,
    ) -> str:
        """Save a new link"""
        max_size = float(os.getenv('MAX_FILE_SIZE_GB', '6.5'))
        try:
            ensure_filesize_within_limit(filesize, max_size_gb=max_size)
        except ValueError as exc:
            raise FilesizeExceededError(f"File size {filesize} exceeds maximum limit of {max_size}GB") from exc
        
        now = datetime.utcnow()
        
        # Calculate Pixeldrain auto upload time
        pixeldrain_delta = self._get_timedelta('PIXELDRAIN')
        
        # Check if Pixeldrain auto upload is enabled
        if os.getenv('PIXELDRAIN_AUTO_UPLOAD_ENABLED', 'true').lower() == 'true':
            pixeldrain_auto_upload_time = now + pixeldrain_delta
        else:
            pixeldrain_auto_upload_time = None
        
        doc = {
            'token': token,
            'drive_id': drive_id,
            'status': status,
            'created_time': now,
            'pixeldrain_auto_upload_time': pixeldrain_auto_upload_time,
            'pixeldrain_uploaded_time': None,
            'pixeldrain_id': None,
            'viking_id': None,
            'gphotos_id': None,  # Only store the Google Photos ID
            'gp_id': None,  # Encrypted Google Photos ID with email identity
            'filename': filename,
            'filesize': filesize,
            'filetype': filetype,
            'error': None
        }
        
        return str(self.links.insert_one(doc).inserted_id)
        
    def get_link(self, token: str) -> Optional[Dict[str, Any]]:
        """Get a link by token"""
        return self.links.find_one({'token': token})
        
    def find_link_by_drive_id(self, drive_id: str) -> Optional[Dict[str, Any]]:
        """Find a link by drive_id with optimized projection"""
        try:
            # Use projection to return only necessary fields for faster queries
            doc = self.links.find_one(
                {'drive_id': drive_id},
                {
                    'token': 1,
                    'filename': 1, 
                    'filesize': 1,
                    'status': 1,
                    '_id': 0
                }
            )
            return doc
        except Exception as e:
            print(f"Error finding link by drive_id: {str(e)}")
            return None

    def get_pending_links(self) -> list[Dict[str, Any]]:
        """Get all pending links for task recovery"""
        try:
            cursor = self.links.find(
                {'status': STATUS_PENDING},
                {
                    'token': 1,
                    'drive_id': 1,
                    'filename': 1,
                    'filesize': 1,
                    'created_time': 1,
                    '_id': 0
                }
            ).sort('created_time', ASCENDING)
            return list(cursor)
        except Exception as e:
            print(f"Error getting pending links: {str(e)}")
            return []

    def iter_pending_links(self, batch_size: int = 100) -> Iterator[Dict[str, Any]]:
        """Yield pending links cursor with small batches to reduce memory."""
        try:
            return self.links.find(
                {'status': STATUS_PENDING},
                {
                    'token': 1,
                    'drive_id': 1,
                    'created_time': 1,
                    '_id': 0
                }
            ).sort('created_time', ASCENDING).batch_size(batch_size)
        except Exception as e:
            print(f"Error creating pending links cursor: {str(e)}")
            return iter(())
        
    def update_link(self, token: str, **kwargs: Any):
        """Update a link"""
        update = {'$set': kwargs}
        now = datetime.utcnow()
        
        # Handle Pixeldrain upload time update
        if 'pixeldrain_id' in kwargs:
            pixeldrain_updates = {
                'pixeldrain_uploaded_time': now
            }
            
            # Only set next auto upload time if enabled
            if os.getenv('PIXELDRAIN_AUTO_UPLOAD_ENABLED', 'true').lower() == 'true':
                pixeldrain_updates['pixeldrain_auto_upload_time'] = now + self._get_timedelta('PIXELDRAIN')
            else:
                pixeldrain_updates['pixeldrain_auto_upload_time'] = None
                
            update['$set'].update(pixeldrain_updates)
            
        return self.links.update_one(
            {'token': token},
            update
        )
        
    def _get_timedelta(self, service: str) -> timedelta:
        """Get timedelta from configuration for a specific service"""
        normalized_service = (service or '').upper()
        if normalized_service == 'PIXELDRAIN':
            auto_upload_time = PIXELDRAIN_AUTO_UPLOAD_TIME
            auto_upload_unit = PIXELDRAIN_AUTO_UPLOAD_UNIT.lower()
        else:
            raise ValueError(f"Unsupported service '{service}' for auto-upload timing")

        if auto_upload_unit == 'minutes':
            return timedelta(minutes=auto_upload_time)
        elif auto_upload_unit == 'hours':
            return timedelta(hours=auto_upload_time)
        else:  # default to days
            return timedelta(days=auto_upload_time)
        
    def _build_expired_condition(self, service: str, now: datetime, include_retry: bool = True) -> Dict[str, Any]:
        """Build query condition for expired links of a specific service"""
        service_lower = service.lower()
        conditions = [
            {
                'status': STATUS_COMPLETED,
                'error': None,
                f'{service_lower}_auto_upload_time': {'$lte': now},
                f'{service_lower}_id': {'$ne': None}
            }
        ]
        
        # Add retry condition for services that support it
        if include_retry:
            conditions.append({
                'status': STATUS_RETRY_PENDING,
                f'{service_lower}_id': None
            })
        
        return {'$or': conditions}

    def get_expired_links(self, service: Optional[str] = None) -> Iterable[Dict[str, Any]]:
        """Get links that need to be re-uploaded for a specific service"""
        # Check if auto upload is enabled for the service
        normalized_service = (service or '').upper()
        if normalized_service != 'PIXELDRAIN':
            raise ValueError(f"Unsupported service '{service}' for auto-upload retrieval")
            
        now = datetime.utcnow()
        
        # Build query based on service
        query = self._build_expired_condition('pixeldrain', now, include_retry=True)
            
        # Stream results in small batches to reduce RAM pressure
        return self.links.find(query).sort('created_time', ASCENDING).batch_size(
            int(os.getenv('AUTOUPLOAD_BATCH_SIZE', '200'))
        )  # Cursor; caller should iterate

    def get_bot_status(self, service: str) -> Dict[str, Any]:
        """Get bot status for a service"""
        status = self.bot_status.find_one({'service': service})
        if not status:
            # Initialize status if not exists
            status = {
                'service': service,
                'status': STATUS_IDLE,
                'last_process_time': None,
                'current_token': None,
                'last_service': None  # Track which service was last processed
            }
            self.bot_status.insert_one(status)
        return status

    def update_bot_status(self, bot_id: str, **kwargs: Any):
        """Update bot status"""
        return self.bot_status.update_one(
            {'service': bot_id},
            {'$set': kwargs},
            upsert=True
        )
    
    def save_new_photo_link(
        self,
        token: str,
        drive_id: str,
        status: str = STATUS_PENDING,
        filename: Optional[str] = None,
        filesize: Optional[str] = None,
        filetype: Optional[str] = None,
    ) -> str:
        """Save a new photo link in separate collection"""
        # Check file size limit
        max_size = float(os.getenv('MAX_FILE_SIZE_GB', '6.5'))
        try:
            ensure_filesize_within_limit(filesize, max_size_gb=max_size)
        except ValueError as exc:
            raise FilesizeExceededError(f"File size {filesize} exceeds maximum limit of {max_size}GB") from exc
        
        now = datetime.utcnow()
            
        doc = {
            'token': token,
            'drive_id': drive_id,
            'status': status,
            'created_time': now,
            'new_gphotos_id': None,  # Only store the new Google Photos ID
            'filename': filename,
            'filesize': filesize,
            'filetype': filetype,
            'error': None
        }
        
        return str(self.new_photo_links.insert_one(doc).inserted_id)
        
    def get_new_photo_link(self, token: str) -> Optional[Dict[str, Any]]:
        """Get a new photo link by token"""
        return self.new_photo_links.find_one({'token': token})
        
    def find_new_photo_link_by_drive_id(self, drive_id: str) -> Optional[Dict[str, Any]]:
        """Find an existing new photo link by drive_id"""
        return self.new_photo_links.find_one({
            'drive_id': drive_id,
            'status': {'$in': [STATUS_COMPLETED, STATUS_PENDING]}  # Only return active links
        })
        
    def update_new_photo_link(self, token: str, **kwargs: Any):
        """Update a new photo link"""
        update = {'$set': kwargs}
        
        return self.new_photo_links.update_one(
            {'token': token},
            update
        )
    
    def get_pending_new_photo_links(self) -> list[Dict[str, Any]]:
        """Get all pending new photo links for task recovery"""
        try:
            cursor = self.new_photo_links.find(
                {'status': STATUS_PENDING},
                {
                    'token': 1,
                    'drive_id': 1,
                    'filename': 1,
                    'filesize': 1,
                    'filetype': 1,
                    'created_time': 1,
                    '_id': 0
                }
            ).sort('created_time', ASCENDING)
            return list(cursor)
        except Exception as e:
            print(f"Error getting pending new photo links: {str(e)}")
            return []

    def iter_pending_new_photo_links(self, batch_size: int = 100) -> Iterator[Dict[str, Any]]:
        """Yield pending new photo links cursor with small batches to reduce memory."""
        try:
            return self.new_photo_links.find(
                {'status': STATUS_PENDING},
                {
                    'token': 1,
                    'drive_id': 1,
                    'created_time': 1,
                    '_id': 0
                }
            ).sort('created_time', ASCENDING).batch_size(batch_size)
        except Exception as e:
            print(f"Error creating pending new photo links cursor: {str(e)}")
            return iter(())

    def reupload_link(self, token: str, **kwargs: Any):
        """Update link for reupload - preserves token and drive_id, updates everything else"""
        # Remove token and drive_id from kwargs to preserve them
        kwargs.pop('token', None)
        kwargs.pop('drive_id', None)
        
        update = {'$set': kwargs}
        now = datetime.utcnow()
        
        # Handle Pixeldrain upload time update
        if 'pixeldrain_id' in kwargs:
            pixeldrain_updates = {
                'pixeldrain_uploaded_time': now
            }
            
            # Only set next auto upload time if enabled
            if os.getenv('PIXELDRAIN_AUTO_UPLOAD_ENABLED', 'true').lower() == 'true':
                pixeldrain_updates['pixeldrain_auto_upload_time'] = now + self._get_timedelta('PIXELDRAIN')
            else:
                pixeldrain_updates['pixeldrain_auto_upload_time'] = None
                
            update['$set'].update(pixeldrain_updates)
            
        return self.links.update_one(
            {'token': token},
            update
        )
    
    def save_pd_lost(
        self,
        token: str,
        drive_id: str,
        filename: Optional[str] = None,
        filesize: Optional[str] = None,
        filetype: Optional[str] = None,
        error: Optional[str] = None,
    ):
        """Save a task to pd_lost collection for later PixelDrain retry"""
        now = datetime.utcnow()
        
        doc = {
            'token': token,
            'drive_id': drive_id,
            'filename': filename,
            'filesize': filesize,
            'filetype': filetype,
            'error': error,
            'created_time': now,
            'retry_count': 0
        }
        
        # Use upsert to avoid duplicates
        return self.pd_lost.update_one(
            {'token': token},
            {'$set': doc},
            upsert=True
        )
    
    def get_pd_lost_tasks(self) -> list[Dict[str, Any]]:
        """Get all pd_lost tasks for retry"""
        try:
            cursor = self.pd_lost.find(
                {},
                {
                    'token': 1,
                    'drive_id': 1,
                    'filename': 1,
                    'filesize': 1,
                    'filetype': 1,
                    'retry_count': 1,
                    'created_time': 1,
                    '_id': 0
                }
            ).sort('created_time', ASCENDING)  # Process oldest first
            return list(cursor)
        except Exception as e:
            print(f"Error getting pd_lost tasks: {str(e)}")
            return []
    
    def remove_pd_lost(self, token: str):
        """Remove a task from pd_lost collection after successful retry"""
        return self.pd_lost.delete_one({'token': token})
    
    def increment_pd_lost_retry(self, token: str):
        """Increment retry count for a pd_lost task"""
        return self.pd_lost.update_one(
            {'token': token},
            {'$inc': {'retry_count': 1}}
        )