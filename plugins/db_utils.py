"""
Database utility functions for file information retrieval
Lightweight helper for getting file info from MongoDB database
"""

import os
from pymongo import MongoClient
from typing import Optional, Dict, Any


class DBUtils:
    """Lightweight database utility for file information"""
    
    def __init__(self):
        """Initialize MongoDB connection from environment variables"""
        mongodb_uri = os.getenv('MONGODB_URI')
        if not mongodb_uri:
            raise ValueError("MONGODB_URI not found in environment variables")
        
        # Initialize lightweight MongoDB connection
        self.client = MongoClient(
            mongodb_uri,
            maxPoolSize=10,  # Small pool for lightweight operations
            connectTimeoutMS=2500,
            retryWrites=True,
            tlsAllowInvalidCertificates=True
        )
        self.db = self.client[os.getenv('DB_NAME', 'link_converter')]
        self.links = self.db.links
        self.new_photo_links = self.db.new_photo_links
    
    def get_filename_by_token(self, token: str) -> Optional[str]:
        """
        Get filename from MongoDB using token
        
        Args:
            token: Download token
            
        Returns:
            Filename if found, None otherwise
        """
        try:
            # Try main links collection first
            result = self.links.find_one(
                {'token': token},
                {'filename': 1, '_id': 0}
            )
            
            if result and result.get('filename'):
                return result['filename']
            
            # Try new_photo_links collection
            result = self.new_photo_links.find_one(
                {'token': token},
                {'filename': 1, '_id': 0}
            )
            
            if result and result.get('filename'):
                return result['filename']
            
            return None
            
        except Exception as e:
            print(f"[DB_UTILS] Error getting filename: {str(e)}")
            return None
    
    def get_file_info_by_token(self, token: str) -> Optional[Dict[str, Any]]:
        """
        Get complete file information from MongoDB using token
        
        Args:
            token: Download token
            
        Returns:
            Dictionary with file info if found, None otherwise
        """
        try:
            # Try main links collection first
            result = self.links.find_one(
                {'token': token},
                {'filename': 1, 'filesize': 1, 'raw_size': 1, 'filetype': 1, 'drive_id': 1, '_id': 0}
            )
            
            if not result:
                # Try new_photo_links collection
                result = self.new_photo_links.find_one(
                    {'token': token},
                    {'filename': 1, 'filesize': 1, 'raw_size': 1, 'filetype': 1, 'drive_id': 1, '_id': 0}
                )
            
            if result:
                return {
                    'filename': result.get('filename'),
                    'filesize': result.get('filesize', 'Unknown'),
                    'raw_size': result.get('raw_size', 0),
                    'filetype': result.get('filetype', 'application/octet-stream'),
                    'file_id': result.get('drive_id')  # MongoDB uses 'drive_id' field
                }
            
            return None
            
        except Exception as e:
            print(f"[DB_UTILS] Error getting file info: {str(e)}")
            return None
    
    def check_token_exists(self, token: str) -> bool:
        """
        Check if token exists in MongoDB
        
        Args:
            token: Download token
            
        Returns:
            True if token exists, False otherwise
        """
        try:
            # Check main links collection
            if self.links.count_documents({'token': token}, limit=1) > 0:
                return True
            
            # Check new_photo_links collection
            if self.new_photo_links.count_documents({'token': token}, limit=1) > 0:
                return True
            
            return False
            
        except Exception as e:
            print(f"[DB_UTILS] Error checking token: {str(e)}")
            return False
    
    def get_link_data(self, token: str) -> Optional[Dict[str, Any]]:
        """
        Get complete link data from MongoDB (for backward compatibility)
        
        Args:
            token: Download token
            
        Returns:
            Dictionary with all link data if found, None otherwise
        """
        try:
            # Try main links collection first
            result = self.links.find_one({'token': token})
            
            if not result:
                # Try new_photo_links collection
                result = self.new_photo_links.find_one({'token': token})
            
            return result
            
        except Exception as e:
            print(f"[DB_UTILS] Error getting link data: {str(e)}")
            return None

