from flask import Blueprint, jsonify, request, Response
import secrets
import threading
import os
import shutil
import json
import signal
import atexit
import hashlib
import base64
from plugins.database_factory import get_database
import requests
import urllib3
from api.mysqldbreq import mysql_status_client
from api.common import validate_api_key, simple_encrypt
from utils.validators import ensure_filesize_within_limit
from utils.statuses import (
    STATUS_COMPLETED,
    STATUS_ERROR,
    STATUS_FAILED,
    STATUS_PENDING,
    STATUS_SUCCESS,
)
from plugins.registry import get_enabled_uploaders
from queueing.redis_queue import RedisJobQueue
from typing import Any, Dict, Optional, Tuple, TYPE_CHECKING, Union

if TYPE_CHECKING:  # pragma: no cover
    from typing import TypedDict
    from queue import Queue

ResponseData = Dict[str, Any]

def _convert_filesize_to_raw_size(filesize_str: Optional[str]) -> Optional[int]:
    """Convert human-readable filesize string to raw bytes"""
    if not filesize_str:
        return None
    
    size_str = str(filesize_str).upper()
    try:
        if 'GB' in size_str:
            size_gb = float(size_str.replace('GB', '').strip())
            return int(size_gb * 1024 * 1024 * 1024)
        elif 'MB' in size_str:
            size_mb = float(size_str.replace('MB', '').strip())
            return int(size_mb * 1024 * 1024)
        elif 'KB' in size_str:
            size_kb = float(size_str.replace('KB', '').strip())
            return int(size_kb * 1024)
        elif 'TB' in size_str:
            size_tb = float(size_str.replace('TB', '').strip())
            return int(size_tb * 1024 * 1024 * 1024 * 1024)
        elif 'B' in size_str:
            return int(size_str.replace('B', '').strip())
        else:
            # Try to parse as pure number (assume bytes)
            return int(float(size_str))
    except (ValueError, TypeError):
        return None

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Create blueprint
api = Blueprint('api', __name__)

# Services will be initialized after database factory is ready
db = None
redis_queue = RedisJobQueue()

def init_routes_services():
    """Initialize route services after database factory is ready"""
    global db
    db = get_database()

# Task recovery system
shutdown_event = threading.Event()

def signal_handler(signum: int, frame: Any) -> None:
    """Handle shutdown signals gracefully"""
    print(f"Received signal {signum}, initiating graceful shutdown...")
    shutdown_event.set()
    
    print("Shutting down.")
    os._exit(0)


def register_signal_handlers() -> None:
    """Register signal handlers (call from app factory in local dev only)."""
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

atexit.register(lambda: None)

def _recover_interrupted_task(token: str, drive_id: str) -> None:
    """Helper function to recover an interrupted task"""
    from plugins.drive import GoogleDrive
    
    if db is None:
        raise RuntimeError("Database not initialized. Call init_routes_services() first.")

    print(f"Requeuing interrupted task {token}")
    try:
        # Check if job already exists in Redis to avoid duplicate queuing
        if redis_queue.redis.exists(redis_queue._get_job_key(token)):
            print(f"[RECOVERY] Job for token {token} already exists in Redis, skipping requeue")
            return

        # Try to get file info from database first
        link_data = db.get_link(token)
        if link_data and link_data.get('filename'):
            file_info = {
                'filename': link_data.get('filename'),
                'filesize': link_data.get('filesize'),
                'file_id': drive_id,
                'filetype': link_data.get('filetype', '')
            }

            # Convert filesize to raw_size if available
            if file_info.get('filesize'):
                raw_size = _convert_filesize_to_raw_size(file_info['filesize'])
                if raw_size:
                    file_info['raw_size'] = raw_size
                    print(f"[RECOVERY] Using cached file info for {token}, avoiding API call")
                    redis_queue.enqueue_process(token, drive_id, file_info, False)
                    return

            # Fallback to API call if conversion fails
            drive = GoogleDrive()
            file_info = drive.get_file_info(drive_id)
            redis_queue.enqueue_process(token, drive_id, file_info, False)
            return

        # No cached info, need API call
        drive = GoogleDrive()
        file_info = drive.get_file_info(drive_id)
        redis_queue.enqueue_process(token, drive_id, file_info, False)

    except Exception as e:
        print(f"Failed to requeue task {token}: {str(e)}")
        err_text = str(e)
        if 'not found' in err_text.lower() or 'no access' in err_text.lower():
            friendly_error = 'File not found in Google Drive or access has been revoked'
        else:
            friendly_error = f'Recovery failed: {err_text}'
        db.update_link(token, status=STATUS_FAILED, error=friendly_error)

def _check_interrupted_download(token: str) -> bool:
    """Check if a download was interrupted by looking for download directory"""
    download_dir = os.path.join(os.getcwd(), 'downloads', token)
    return os.path.exists(download_dir)

def recover_pending_tasks() -> None:
    """Recover pending tasks on startup"""
    if db is None:
        raise RuntimeError("Database not initialized. Call init_routes_services() first.")
    try:
        # Stream pending tasks from database in small batches to reduce RAM usage
        pending_iter = db.iter_pending_links(batch_size=int(os.getenv('RECOVERY_BATCH_SIZE', '200')))
        found_any = False
        
        for link in pending_iter:
            if not found_any:
                print("Found pending tasks, attempting recovery...")
                found_any = True

            token = link['token']
            drive_id = link['drive_id']
            
            # Check if task was interrupted during download
            if _check_interrupted_download(token):
                # Task was interrupted, mark as failed
                print(f"Task {token} was interrupted, marking as failed")
                db.update_link(token, status=STATUS_FAILED, error='Task interrupted by server restart')
                continue
            
            # Task was queued but never started, requeue it
            _recover_interrupted_task(token, drive_id)
            
        if not found_any:
            from utils.logging_config import get_modern_logger
            recovery_logger = get_modern_logger("RECOVERY")
            recovery_logger.info("No pending tasks found for recovery")
            
    except ConnectionError as err:
        print(f"Database connection error during recovery: {err}")
    except Exception as err:
        print(f"Unexpected error during task recovery: {err}")

def init_recovery_on_startup() -> None:
    """Initialize recovery routines (to be called from app factory)."""
    recover_pending_tasks()
    # Redis worker will handle all queued tasks automatically

# Simple encryption function that is compatible with PHP and other languages
def simple_encrypt(text: str) -> str:
    """Simple encryption compatible with PHP
    
    PHP Decryption example:
    ----------------------
    function simple_decrypt($encrypted) {
        $secret = 'my_simple_encryption_key'; // same as in Python
        $data = base64_decode(strtr($encrypted, '-_', '+/'));
        $hash = substr($data, 0, 8);
        $text = substr($data, 8);
        $new_hash = substr(md5($text . $secret), 0, 8);
        if ($hash != $new_hash) {
            return false;
        }
        return $text;
    }
    """
    if not text:
        return None
        
    # Get secret key from env or use default
    secret = os.getenv('ENCRYPTION_SECRET', 'mksbhai')
    
    # Create a simple hash to verify integrity
    hash_value = hashlib.md5((text + secret).encode()).hexdigest()[:8]
    
    # Prepend hash to the text
    result = hash_value + text
    
    # Base64 encode for URL safety and replace chars for URL compatibility
    encoded = base64.b64encode(result.encode()).decode()
    encoded = encoded.replace('+', '-').replace('/', '_').replace('=', '')
    
    return encoded

def load_api_keys() -> Dict[str, Dict[str, Any]]:
    """Load API keys from JSON file"""
    try:
        keys_file = os.path.join(os.path.dirname(__file__), 'keys.json')
        with open(keys_file, 'r') as f:
            data = json.load(f)
        return {user['api_key']: user for user in data['users']}
    except Exception:
        return {}

def validate_api_key(api_key: str) -> bool:
    """Validate API key"""
    api_keys = load_api_keys()
    user = api_keys.get(api_key)
    
    if not user:
        return False
        
    return user['is_active']

## All processing is now handled by Redis worker service

@api.route('/<api_key>/reupload/<token>', methods=['GET'])
def reupload_file(api_key: str, token: str) -> Union[Response, tuple[Response, int]]:
    """Reupload file using existing token"""
    # Check if processing is enabled
    if os.getenv('ENABLE_PROCESS', 'true').lower() != 'true':
        return jsonify({
            'success': False,
            'error': 'System processing is currently disabled'
        }), 503

    if db is None:
        return jsonify({'success': False, 'error': 'Database not initialized'}), 500
    try:
        # Validate API key
        if not validate_api_key(api_key):
            return jsonify({
                'success': False,
                'error': 'Invalid or inactive API key'
            }), 401
        
        # Get existing link data
        link_data = db.get_link(token)
        if not link_data:
            return jsonify({
                'success': False,
                'error': 'Link not found'
            }), 404
        
        file_id = link_data['drive_id']
        print(f"Reupload: Using file_id from database: {file_id}")
        
        # Construct file_info from database data for reupload
        file_info = {
            'filename': link_data.get('filename'),
            'filesize': link_data.get('filesize'),
            'filetype': link_data.get('filetype'),
            'file_id': link_data.get('drive_id')
        }
        print(f"Reupload: Constructed file_info from database: {file_info}")
        
        # Enqueue to Redis queue
        try:
            job_id = redis_queue.enqueue_process(token, file_id, file_info, True)
            print(f"✅ Enqueued Redis reupload job {job_id} for token {token}")
        except Exception as e:
            print(f"❌ Redis queue failed: {e}")
            return jsonify({
                'success': False,
                'error': 'Redis queue unavailable. Please retry later.'
            }), 503
        
        return jsonify({
            'success': True,
            'message': 'Reupload started',
            'data': {
                'token': token,
                'filename': link_data.get('filename'),
                'filesize': link_data.get('filesize'),
                'status': STATUS_PENDING,
                'check_url': f"/api/{api_key}/status/{token}"
            }
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@api.route('/<api_key>/<path:drive_path>', methods=['GET'])
def generate_link(api_key: str, drive_path: str) -> Union[Response, tuple[Response, int]]:
    """Generate download link from Google Drive link or ID"""
    # Check if processing is enabled
    if os.getenv('ENABLE_PROCESS', 'true').lower() != 'true':
        return jsonify({
            'success': False,
            'error': 'System processing is currently disabled'
        }), 503

    from utils.logging_config import LogContext, set_correlation_id
    from plugins.drive import GoogleDrive
    import logging
    
    # Set correlation ID for tracking
    correlation_id = set_correlation_id()
    logger = logging.getLogger(__name__)
    
    with LogContext(correlation_id, endpoint="generate_link", api_key=api_key[:8] + "..."):
        if db is None:
            return jsonify({'success': False, 'error': 'Database not initialized'}), 500
        try:
            logger.info("Link generation request", extra={"drive_path": drive_path})
            
            # Validate API key
            if not validate_api_key(api_key):
                logger.warning("Invalid API key used")
                return jsonify({
                    'success': False,
                    'error': 'Invalid or inactive API key'
                }), 401
            
            # Initialize Google Drive
            drive = GoogleDrive()
            
            # Check if input is a full URL or just an ID
            if 'drive.google.com' in drive_path:
                file_id = drive.extract_file_id(drive_path)
                if not file_id:
                    return jsonify({
                        'success': False,
                        'error': 'Invalid Google Drive link'
                    }), 400
            else:
                file_id = drive_path
                
            # FIRST: Check if this file_id already exists in database (FASTEST)
            existing_link = db.find_link_by_drive_id(file_id)
            if existing_link:
                # Return existing link data with 303 See Other status
                response: "Response" = jsonify({
                    'success': True,
                    'message': 'Existing link found',
                    'data': {
                        'token': existing_link['token'],
                        'filename': existing_link.get('filename'),
                        'filesize': existing_link.get('filesize'),
                        'status': existing_link['status'],
                        'check_url': f"/api/{api_key}/status/{existing_link['token']}"
                    }
                })
                response.status_code = 303
                response.headers['Location'] = f"/api/{api_key}/status/{existing_link['token']}"
                return response

            # SECOND: Get file info ONLY if not in database (for new files)
            try:
                file_info = drive.get_file_info(file_id)
                max_size = float(os.getenv('MAX_FILE_SIZE_GB', '6.5'))
                ensure_filesize_within_limit(file_info.get('filesize'), max_size_gb=max_size)
            except ValueError:
                    return jsonify({
                        'success': False,
                        'error': f'File size ({file_info["filesize"]}) exceeds maximum limit of {max_size}GB',
                        'max_size': f'{max_size}GB',
                        'file_size': file_info["filesize"]
                    }), 400
            except Exception as e:
                return jsonify({
                    'success': False,
                    'error': f'Error checking file size: {str(e)}'
                }), 500  # Changed from 400 to 500 for general errors

            # Generate new token and save link
            token = secrets.token_urlsafe(8)
            db.save_link(
                token=token,
                drive_id=file_id,
                status=STATUS_PENDING,
                filename=file_info['filename'],
                filesize=file_info['filesize'],
                filetype=file_info.get('filetype', '')
            )
            
            # Enqueue to Redis queue
            job_id = redis_queue.enqueue_process(token, file_id, file_info, False)
            print(f"✅ Enqueued Redis job {job_id} for token {token}")
            
            # Return response with file info
            return jsonify({
                'success': True,
                'message': 'Link generation started',
                'data': {
                    'token': token,
                    'filename': file_info['filename'],
                    'filesize': file_info['filesize'],
                    'status': STATUS_PENDING,
                    'check_url': f"/api/{api_key}/status/{token}"
                }
            })
            
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500

@api.route('/<api_key>/status/<token>', methods=['GET'])
def check_status(api_key: str, token: str) -> Union[Response, tuple[Response, int]]:
    """Check status of a download"""
    if db is None:
        return jsonify({'success': False, 'error': 'Database not initialized'}), 500
    try:
        # Validate API key
        if not validate_api_key(api_key):
            return jsonify({
                'success': False,
                'error': 'Invalid or inactive API key'
            }), 401
        
        # Get link data
        link_data = db.get_link(token)
        if not link_data:
            return jsonify({
                'success': False,
                'error': 'Link not found'
            }), 404
        
        response = {
            'success': True,
            'data': {
                'token': token,
                'status': link_data['status'],
                'filename': link_data.get('filename'),
                'filesize': link_data.get('filesize'),
                'filetype': link_data.get('filetype', '')
            }
        }
        
        # Add download URLs if IDs exist, regardless of status
        if link_data.get('pixeldrain_id'):
            response['data']['pixeldrain_url'] = f"https://pixeldrain.com/u/{link_data['pixeldrain_id']}"
        if link_data.get('buzzheavier_id'):
            response['data']['buzzheavier_url'] = f"https://buzzheavier.com/{link_data['buzzheavier_id']}"
        if link_data.get('viking_id'):
            response['data']['viking_url'] = f"https://vikingfile.com/f/{link_data['viking_id']}"
        if link_data.get('gphotos_id'):
            response['data']['gphotos_id'] = link_data['gphotos_id']
        if link_data.get('gp_id'):
            response['data']['gp_id'] = link_data['gp_id']
        # Add error if failed
        elif link_data['status'] == STATUS_FAILED:
            response['data']['error'] = link_data.get('error')
            
        return jsonify(response)
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500 
