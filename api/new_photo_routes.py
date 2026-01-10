from flask import Blueprint, jsonify, request
import secrets
import threading
import os
import shutil
import json
from plugins.database import Database
from plugins.drive import GoogleDrive
from plugins.gphotos import GPhotos, NewGPhotos
import queue
from api.common import validate_api_key
from config import MAX_NEW_UPLOAD
from queueing.redis_queue import RedisJobQueue
from utils.logging_config import get_modern_logger

logger = get_modern_logger("NEW_PHOTO")

def _convert_filesize_to_raw_size(filesize_str):
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

# Create blueprint for new photo upload endpoint
new_photo_api = Blueprint('new_photo_api', __name__)

# Initialize database
db = Database()

# Initialize Redis queue
redis_queue = RedisJobQueue()

# Create separate queue system for new photo uploads
new_photo_queue = queue.Queue()
new_photo_active_tasks = 0
new_photo_queue_lock = threading.Lock()

## Encryption moved to api.common if needed later

def load_api_keys():
    """Deprecated: Use api.common.validate_api_key"""
    return {}

class NewGPhotos(GPhotos):
    """Extended GPhotos class that uses GP_AUTH_DATA_NEW"""
    
    def __init__(self):
        # Load new authentication data from environment
        auth_data = os.getenv('GP_AUTH_DATA_NEW')
        if not auth_data:
            raise ValueError("GP_AUTH_DATA_NEW environment variable is not set")
            
        # Initialize Google Photos client with new auth data
        try:
            from plugins.gphotos import CustomClient
            self.client = CustomClient(auth_data=auth_data)
            
            # Initialize mimetypes
            import mimetypes
            mimetypes.init()
            
        except Exception as e:
            print(f"[ERROR] Failed to initialize NewGPhotos: {str(e)}")
            raise e

def process_new_photo_upload(token, file_id, file_info):
    """Background process to download and upload file to new Google Photos account only"""
    global new_photo_active_tasks
    
    try:
        print(f"[INFO] Processing new photo upload: {token}")
        
        drive = GoogleDrive()
        new_gphotos = NewGPhotos()
        
        # Download file from Google Drive 
        print(f"[INFO] Starting download from Google Drive for: {token}")
        
        # For recovery tasks, check if we can create raw_size from existing filesize
        if file_info and 'file_id' in file_info and not file_info.get('raw_size'):
            # Try to convert filesize to raw_size to avoid API call
            existing_filesize = file_info.get('filesize')
            if existing_filesize:
                raw_size = _convert_filesize_to_raw_size(existing_filesize)
                if raw_size:
                    file_info['raw_size'] = raw_size
                    print(f"[INFO] Converted filesize '{existing_filesize}' to raw_size: {raw_size} bytes")
        
        # For recovery tasks, pass file_info to avoid API calls (only if raw_size is available)
        if file_info and 'file_id' in file_info and file_info.get('raw_size'):
            # This is a recovery task with valid raw_size, use stored file_info to avoid API call
            print(f"[INFO] Using stored file info for recovery task")
            local_path, filename, filesize = drive.download_file(file_id, token, file_info=file_info, job_type='new_photo')
        else:
            # This is a new task or recovery task without raw_size, let it fetch file_info
            if file_info and 'file_id' in file_info:
                print(f"[INFO] Recovery task but missing raw_size, fetching file info from API")
            local_path, filename, filesize = drive.download_file(file_id, token, job_type='new_photo')
        print(f"[INFO] Download completed: {filename} ({filesize})")
        
        # Check environment variable for Google Photos upload (new photo routes)
        enable_gphotos = os.getenv('NEW_ENABLE_GPHOTOS', 'true').lower() == 'true'
        
        # Upload to new Google Photos account only if enabled
        if enable_gphotos:
            try:
                print(f"[INFO] Starting Google Photos upload for: {token}")
                gphotos_id = new_gphotos.upload(local_path, token)
                
                if gphotos_id:
                    # Store the encrypted version in the database
                    db.update_new_photo_link(token, new_gphotos_id=gphotos_id)
                    # Mark as completed
                    db.update_new_photo_link(token, status='completed', error=None)
                    print(f"[SUCCESS] Google Photos upload completed: {token}")
                    
                    # Cleanup immediately after successful upload
                    try:
                        download_dir = os.path.join(os.getcwd(), 'downloads', token)
                        if os.path.exists(download_dir):
                            shutil.rmtree(download_dir)
                            print(f"[INFO] Cleaned up downloads directory: {token}")
                    except Exception as cleanup_error:
                        print(f"[WARN] Failed to cleanup directory: {cleanup_error}")
                        
                else:
                    raise Exception("Google Photos upload failed - no media key returned")
                    
            except Exception as e:
                error_msg = str(e)
                if "UploadRejected" in error_msg or "File upload rejected" in error_msg:
                    print(f"[ERROR] Google Photos rejected file: {token} - {filename}")
                    print(f"[ERROR] This might be due to file format, size, or account limits")
                else:
                    print(f"[ERROR] Google Photos upload error: {token} - {error_msg}")
                raise e
        else:
            print(f"[INFO] Google Photos upload disabled, marking as completed: {token}")
            # Mark as completed even if upload is disabled
            db.update_new_photo_link(token, status='completed', error=None)
            
            # Cleanup when disabled too
            try:
                download_dir = os.path.join(os.getcwd(), 'downloads', token)
                if os.path.exists(download_dir):
                    shutil.rmtree(download_dir)
                    print(f"[INFO] Cleaned up downloads directory (upload disabled): {token}")
            except Exception as cleanup_error:
                print(f"[WARN] Failed to cleanup directory: {cleanup_error}")
        
    except Exception as e:
        print(f"[ERROR] New photo upload failed for token {token}: {str(e)}")
        db.update_new_photo_link(token, 
                      status='failed',
                      error=str(e))

    finally:
        # Decrement active tasks counter and process next item in queue if available
        with new_photo_queue_lock:
            new_photo_active_tasks -= 1
            
            # Check if there are tasks in queue and we can process more
            if not new_photo_queue.empty() and new_photo_active_tasks < MAX_NEW_UPLOAD:
                # Get next task from queue
                next_task = new_photo_queue.get()
                # Increment active tasks counter
                new_photo_active_tasks += 1
                # Start processing in a new thread
                thread = threading.Thread(
                    target=process_new_photo_upload,
                    args=next_task
                )
                thread.daemon = True
                thread.start()
                print(f"[INFO] Started next queued task. Active: {new_photo_active_tasks}, Queue: {new_photo_queue.qsize()}")
                
        # Fallback cleanup in case of unexpected error (only if not already cleaned)
        try:
            download_dir = os.path.join(os.getcwd(), 'downloads', token)
            if os.path.exists(download_dir):
                shutil.rmtree(download_dir)
                print(f"[INFO] Fallback cleanup for: {token}")
        except Exception:
            pass

def recover_pending_new_photo_tasks():
    """Recover pending new photo tasks on startup"""
    try:
        # Stream pending links from database in small batches to reduce RAM usage
        pending_iter = db.iter_pending_new_photo_links(batch_size=int(os.getenv('RECOVERY_BATCH_SIZE', '200')))

        recovered_count = 0
        global new_photo_active_tasks
        for i, link_data in enumerate(pending_iter):
            token = link_data['token']
            drive_id = link_data['drive_id']
            
            # Use stored file_info from database - no need to refetch
            # Calculate raw_size from filesize string using helper function
            filesize_str = link_data.get('filesize', 'unknown')
            raw_size = _convert_filesize_to_raw_size(filesize_str) if filesize_str and filesize_str != 'unknown' else None
            
            file_info = {
                'filename': link_data.get('filename', 'unknown'),
                'filesize': filesize_str,
                'filetype': link_data.get('filetype', ''),
                'file_id': drive_id,
                'raw_size': raw_size
            }
            
            # Only start MAX_NEW_UPLOAD tasks immediately, rest goes to queue
            if i < MAX_NEW_UPLOAD:
                # Start processing immediately
                with new_photo_queue_lock:
                    new_photo_active_tasks += 1
                thread = threading.Thread(
                    target=process_new_photo_upload,
                    args=(token, drive_id, file_info)
                )
                thread.daemon = True
                thread.start()
            else:
                # Add to queue
                new_photo_queue.put((token, drive_id, file_info))
        
            recovered_count += 1

        if recovered_count == 0:
            logger.info("No pending new photo tasks found")
        else:
            logger.success(f"Recovery queued {recovered_count} tasks. Active: {new_photo_active_tasks}, Queue size: {new_photo_queue.qsize()}")
        
    except Exception as e:
        print(f"[RECOVERY ERROR] Failed to recover pending tasks: {str(e)}")
        import traceback
        print(f"[RECOVERY ERROR] Full traceback: {traceback.format_exc()}")

@new_photo_api.route('/new-photos/<api_key>/<path:drive_path>', methods=['GET'])
def generate_new_photo_link(api_key, drive_path):
    """Generate new photo upload link from Google Drive link or ID - uploads only to new Google Photos account"""
    try:
        # Validate API key
        if not validate_api_key(api_key):
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
            
        # Get file info first to check size and type
        try:
            file_info = drive.get_file_info(file_id)
            
            # Check if file is a video (required for Google Photos)
            import mimetypes
            mime_type, _ = mimetypes.guess_type(file_info['filename'])
            if not (mime_type and mime_type.startswith('video/')):
                return jsonify({
                    'success': False,
                    'error': 'Only video files are supported for Google Photos upload',
                    'file_type': mime_type or 'unknown'
                }), 400
            
            # Check file size limit
            size_str = str(file_info['filesize']).upper()
            if 'GB' in size_str:
                size_gb = float(size_str.replace('GB', ''))
            elif 'MB' in size_str:
                size_gb = float(size_str.replace('MB', '')) / 1024
            elif 'TB' in size_str:
                size_gb = float(size_str.replace('TB', '')) * 1024
            else:
                size_gb = 0
                
            max_size = float(os.getenv('MAX_FILE_SIZE_GB', '6.5'))
            if size_gb > max_size:
                return jsonify({
                    'success': False,
                    'error': f'File size ({file_info["filesize"]}) exceeds maximum limit of {max_size}GB',
                    'max_size': f'{max_size}GB',
                    'file_size': file_info["filesize"]
                }), 400
                
        except Exception as e:
            return jsonify({
                'success': False,
                'error': f'Error checking file info: {str(e)}'
            }), 400

        # Check if this file_id already exists in new photos database
        existing_link = db.find_new_photo_link_by_drive_id(file_id)
        if existing_link:
            # Return existing link data with 303 See Other status
            response = jsonify({
                'success': True,
                'message': 'Existing new photo link found',
                'data': {
                    'token': existing_link['token'],
                    'filename': existing_link.get('filename'),
                    'filesize': existing_link.get('filesize'),
                    'status': existing_link['status'],
                    'check_url': f"/api/new-photos/{api_key}/status/{existing_link['token']}"
                }
            })
            response.status_code = 303
            response.headers['Location'] = f"/api/new-photos/{api_key}/status/{existing_link['token']}"
            return response

        # Generate new token and save link in new photos collection
        token = secrets.token_urlsafe(16)
        db.save_new_photo_link(token=token,
                    drive_id=file_id,
                    status='pending',
                    filename=file_info['filename'],
                    filesize=file_info['filesize'],
                    filetype=file_info.get('filetype', ''))
        
                # Enqueue to Redis queue
        try:
            job_id = redis_queue.enqueue_newphoto(token, file_id, file_info)
            print(f"[INFO] ✅ Enqueued Redis new photo job {job_id} for token {token}")
        except Exception as e:
            print(f"[ERROR] ❌ Redis queue failed, falling back to local queue: {e}")
            # Fallback to local queue if Redis fails
            global new_photo_active_tasks
            with new_photo_queue_lock:
                if new_photo_active_tasks < MAX_NEW_UPLOAD:
                    new_photo_active_tasks += 1
                    thread = threading.Thread(
                        target=process_new_photo_upload,
                        args=(token, file_id, file_info)
                    )
                    thread.daemon = True
                    thread.start()
                    print(f"[INFO] Started new task immediately. Active: {new_photo_active_tasks}")
                else:
                    new_photo_queue.put((token, file_id, file_info))
                    print(f"[INFO] Added new photo task to queue. Queue size: {new_photo_queue.qsize()}")
        
        # Return response with file info
        return jsonify({
            'success': True,
            'message': 'New photo upload started',
            'data': {
                'token': token,
                'filename': file_info['filename'],
                'filesize': file_info['filesize'],
                'status': 'pending',
                'upload_type': 'new_google_photos_only',
                'check_url': f"/api/new-photos/{api_key}/status/{token}"
            }
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@new_photo_api.route('/new-photos/<api_key>/status/<token>', methods=['GET'])
def check_new_photo_status(api_key, token):
    """Check status of a new photo upload"""
    try:
        # Validate API key
        if not validate_api_key(api_key):
            return jsonify({
                'success': False,
                'error': 'Invalid or inactive API key'
            }), 401

        # Get link data from new photos collection
        link_data = db.get_new_photo_link(token)
        if not link_data:
            return jsonify({
                'success': False,
                'error': 'New photo link not found'
            }), 404
        
        response = {
            'success': True,
            'data': {
                'token': token,
                'status': link_data['status'],
                'filename': link_data.get('filename'),
                'filesize': link_data.get('filesize'),
                'filetype': link_data.get('filetype', ''),
                'upload_type': 'new_google_photos_only'
            }
        }
        
        # Add Google Photos ID if exists
        if link_data.get('new_gphotos_id'):
            response['data']['new_gphotos_id'] = link_data['new_gphotos_id']
        # Add error if failed
        elif link_data['status'] == 'failed':
            response['data']['error'] = link_data.get('error')
            
        return jsonify(response)
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@new_photo_api.route('/new-photos/<api_key>/queue-status', methods=['GET'])
def get_new_photo_queue_status(api_key):
    """Get current queue status for new photo uploads"""
    try:
        # Validate API key
        if not validate_api_key(api_key):
            return jsonify({
                'success': False,
                'error': 'Invalid or inactive API key'
            }), 401

        # Get pending count from database
        pending_count = len(db.get_pending_new_photo_links())

        with new_photo_queue_lock:
            return jsonify({
                'success': True,
                'data': {
                    'active_uploads': new_photo_active_tasks,
                    'max_concurrent': MAX_NEW_UPLOAD,
                    'queue_size': new_photo_queue.qsize(),
                    'available_slots': MAX_NEW_UPLOAD - new_photo_active_tasks,
                    'database_pending': pending_count
                }
            })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@new_photo_api.route('/new-photos/<api_key>/recover-pending', methods=['GET'])
def manual_recover_pending_tasks(api_key):
    """Manually recover pending new photo tasks - admin endpoint"""
    try:
        # Validate API key
        if not validate_api_key(api_key):
            return jsonify({
                'success': False,
                'error': 'Invalid or inactive API key'
            }), 401

        # Start recovery in background thread
        recovery_thread = threading.Thread(target=recover_pending_new_photo_tasks)
        recovery_thread.daemon = True
        recovery_thread.start()

        return jsonify({
            'success': True,
            'message': 'Recovery process started in background',
            'data': {
                'recovery_initiated': True
            }
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500