from flask import Blueprint, jsonify
import threading
import os
import shutil
import queue
import time

from plugins.database import Database
from plugins.drive import GoogleDrive
from plugins.pixeldrain import PixelDrain
from plugins.gphotos import NewGPhotos
from api.common import validate_api_key, simple_decrypt
from config import MAX_PD_LOST
from queueing.redis_queue import RedisJobQueue
from utils.logging_config import get_modern_logger

logger = get_modern_logger("PD_LOST")


pd_lost_api = Blueprint('pd_lost_api', __name__)

db = Database()
redis_queue = RedisJobQueue()
pd_lost_retry_lock = threading.Lock()
pd_lost_retry_active = False

# Concurrency control for PixelDrain retry
pd_lost_queue: "queue.Queue[tuple[str, str]]" = queue.Queue()
pd_lost_queue_lock = threading.Lock()
pd_lost_active_tasks = 0


# Local key loader removed; using api.common.validate_api_key


def _retry_pixeldrain_single(token: str, drive_id: str, file_info: dict | None) -> None:
    """Retry PixelDrain upload for a single token from pd_lost.

    Steps:
    - Ensure local file exists for the token; re-download from Drive if needed
    - Upload to PixelDrain
    - Update main links collection with pixeldrain_id
    - Remove entry from pd_lost on success
    - On failure, increment retry_count and keep for later
    """
    try:
        logger.info(f"PixelDrain retry started for token: {token}")

        # Try to reuse already-downloaded file if present
        downloads_dir = os.path.join(os.getcwd(), 'downloads', token)
        local_path = None
        if os.path.exists(downloads_dir):
            for name in os.listdir(downloads_dir):
                candidate = os.path.join(downloads_dir, name)
                if os.path.isfile(candidate):
                    local_path = candidate
                    break

        # If not found locally, re-download with Google Photos priority
        if local_path is None:
            # Get link data to check for Google Photos ID
            link_data = db.get_link(token) or {}
            photos_id = link_data.get('gphotos_id')
            filename = link_data.get('filename', 'unknown')
            
            # Try Google Photos first if available
            if photos_id:
                print(f"[INFO] pd_lost: Attempting Google Photos download for {token}")
                
                # Decrypt gphotos_id if encrypted
                if len(photos_id) > 20 and not photos_id.startswith('AF1Q'):
                    try:
                        decrypted_id = simple_decrypt(photos_id)
                        if decrypted_id:
                            photos_id = decrypted_id
                            print("[DEBUG] Successfully decrypted gphotos_id for pd_lost")
                        else:
                            print("[WARN] Failed to decrypt gphotos_id for pd_lost")
                            photos_id = None
                    except Exception as e:
                        print(f"[ERROR] Decryption error in pd_lost: {e}")
                        photos_id = None
                
                if photos_id:
                    try:
                        gphotos = NewGPhotos()
                        download_path = os.path.join(downloads_dir, f"gphotos_{token}")
                        os.makedirs(os.path.dirname(download_path), exist_ok=True)
                        
                        local_path = gphotos.download_by_media_key(photos_id, download_path)
                        
                        if local_path and os.path.exists(local_path):
                            print(f"[SUCCESS] pd_lost: Downloaded from Google Photos: {os.path.basename(local_path)}")
                        else:
                            print("[WARN] pd_lost: Google Photos download failed, falling back to Drive")
                            local_path = None
                    except Exception as e:
                        print(f"[ERROR] pd_lost: Google Photos download error: {e}, falling back to Drive")
                        local_path = None
            
            # Fallback to Google Drive if Google Photos failed or no photos_id
            if local_path is None:
                print(f"[INFO] pd_lost: Falling back to Google Drive download for {token}")
                
                # Build file_info from links collection
                filesize_str = link_data.get('filesize', 'unknown')
                raw_size = None
                try:
                    if filesize_str and str(filesize_str).lower() != 'unknown':
                        size_str = str(filesize_str).upper().replace(' ', '')
                        if 'GB' in size_str:
                            raw_size = int(float(size_str.replace('GB', '')) * 1024 * 1024 * 1024)
                        elif 'MB' in size_str:
                            raw_size = int(float(size_str.replace('MB', '')) * 1024 * 1024)
                        elif 'KB' in size_str:
                            raw_size = int(float(size_str.replace('KB', '')) * 1024)
                        elif 'TB' in size_str:
                            raw_size = int(float(size_str.replace('TB', '')) * 1024 * 1024 * 1024 * 1024)
                except Exception:
                    raw_size = None

                file_info = {
                    'filename': filename,
                    'filesize': filesize_str,
                    'filetype': link_data.get('filetype', ''),
                    'file_id': drive_id,
                    'raw_size': raw_size,
                }

                drive = GoogleDrive()
                if raw_size:
                    print("[INFO] Using stored file info for pd_lost Drive fallback")
                    local_path, filename, _ = drive.download_file(drive_id, token, file_info=file_info, job_type='pd_lost')
                else:
                    print("[INFO] pd_lost Drive fallback without raw_size, fetching file info from API")
                    local_path, filename, _ = drive.download_file(drive_id, token, job_type='pd_lost')
                print(f"[INFO] Drive fallback successful: {filename}")

        # Upload to PixelDrain
        pixeldrain = PixelDrain()
        pd_id = pixeldrain.upload(local_path)

        # Update main link with PixelDrain id
        db.update_link(token, pixeldrain_id=pd_id)
        logger.success(f"PixelDrain retry successful. Token: {token}, ID: {pd_id}")

        # Remove from pd_lost on success
        db.db.pd_lost.delete_one({'token': token})
        logger.info(f"Removed token from pd_lost: {token}")

    except Exception as e:
        logger.error(f"PixelDrain retry failed for {token}: {e}")
        err_text = str(e)
        lower = err_text.lower()
        if 'not found' in lower or 'no access' in lower:
            # Permanently skip this token for future retries
            db.db.pd_lost.update_one(
                {'token': token},
                {'$set': {'error': 'File not found or no access', 'skip': True}}
            )
            logger.warning(f"Marked token as skip (file missing or no access): {token}")
        else:
            # Increment retry_count and store latest error with timestamp
            import datetime
            db.db.pd_lost.update_one(
                {'token': token},
                {
                    '$inc': {'retry_count': 1}, 
                    '$set': {
                        'error': err_text,
                        'last_retry': datetime.datetime.utcnow()
                    }
                }
            )
            # Optional: Drop after too many retries
            doc = db.db.pd_lost.find_one({'token': token}, {'retry_count': 1})
            if doc and int(doc.get('retry_count', 0)) >= 3:
                db.db.pd_lost.delete_one({'token': token})
                logger.warning(f"Removed token from pd_lost after 3 retries: {token}")
    finally:
        # Cleanup token-specific downloads directory
        try:
            if os.path.exists(downloads_dir):
                shutil.rmtree(downloads_dir)
        except Exception:
            pass


def _start_next_pd_lost_task_if_possible() -> None:
    """Start next queued pd_lost task if concurrency allows."""
    global pd_lost_active_tasks
    with pd_lost_queue_lock:
        while not pd_lost_queue.empty() and pd_lost_active_tasks < MAX_PD_LOST:
            token, drive_id = pd_lost_queue.get()
            pd_lost_active_tasks += 1
            thread = threading.Thread(target=_pd_lost_worker, args=(token, drive_id))
            thread.daemon = True
            thread.start()
            print(f"[INFO] Started pd_lost task. Active: {pd_lost_active_tasks}, Queue: {pd_lost_queue.qsize()}, MAX_PD_LOST: {MAX_PD_LOST}")


def _pd_lost_worker(token: str, drive_id: str) -> None:
    """Worker wrapper around single retry with concurrency bookkeeping."""
    global pd_lost_active_tasks
    try:
        _retry_pixeldrain_single(token, drive_id, None)
    finally:
        with pd_lost_queue_lock:
            pd_lost_active_tasks -= 1
            print(f"[INFO] Finished pd_lost task. Active: {pd_lost_active_tasks}, Queue: {pd_lost_queue.qsize()}")
        _start_next_pd_lost_task_if_possible()


def _enqueue_pd_lost_tasks(tasks: list[dict]) -> int:
    """Put tasks into the pd_lost retry queue."""
    count = 0
    with pd_lost_queue_lock:
        for task in tasks:
            token = task.get('token')
            drive_id = task.get('drive_id')
            if not token or not drive_id:
                continue
            # Skip permanently skipped tasks
            if task.get('skip') is True:
                continue
            pd_lost_queue.put((token, drive_id))
            count += 1
    return count


def _begin_pd_lost_processing(tasks: list[dict]) -> None:
    """Enqueue tasks and kick off workers respecting MAX_PD_LOST."""
    global pd_lost_retry_active
    # Enqueue to Redis queue
    try:
        redis_queue = RedisJobQueue()
        count = 0
        for t in tasks:
            token = t.get('token')
            drive_id = t.get('drive_id')
            if token and drive_id:
                redis_queue.enqueue_pdlost(token, drive_id)
                count += 1
        logger.success(f"pd_lost: Enqueued {count} Redis tasks")
        with pd_lost_retry_lock:
            pd_lost_retry_active = False
        logger.info("PixelDrain retry dispatch complete")
    except Exception as e:
        print(f"[ERROR] Redis queue failed, falling back to local queue: {e}")
        # Fallback to local queue if Redis fails
        enq = _enqueue_pd_lost_tasks(tasks)
        print(f"[INFO] pd_lost: Enqueued {enq} tasks. Active: {pd_lost_active_tasks}, Queue: {pd_lost_queue.qsize()}, MAX_PD_LOST: {MAX_PD_LOST}")
        _start_next_pd_lost_task_if_possible()
        with pd_lost_retry_lock:
            pd_lost_retry_active = False
        print("[INFO] PixelDrain retry dispatch complete")


def _load_pd_lost_tasks(seed_if_empty: bool = True) -> list[dict]:
    """Load pd_lost tasks; optionally seed from links when empty."""
    projection = {
        'token': 1,
        'drive_id': 1,
        'filename': 1,
        'filesize': 1,
        'filetype': 1,
        'retry_count': 1,
        'skip': 1,
        '_id': 0,
    }
    tasks = list(db.db.pd_lost.find({
        '$and': [
            {'$or': [{'skip': {'$exists': False}}, {'skip': False}]},
            {'$or': [{'retry_count': {'$exists': False}}, {'retry_count': {'$lt': 3}}]}
        ]
    }, projection).sort('created_time', 1))
    if tasks or not seed_if_empty:
        return tasks

    # Seed from main links where PixelDrain missing but others exist
    # Limit seeding to prevent overwhelming the system
    logger.debug("PD_LOST seeding: Checking for eligible links from last 7 days...")
    
    # Calculate 7 days ago timestamp
    import datetime
    seven_days_ago = datetime.datetime.utcnow() - datetime.timedelta(days=7)
    
    seed_cursor = db.db.links.find(
        {
            'status': 'completed',
            'pixeldrain_id': None,
            'created_time': {'$gte': seven_days_ago},  # Only last 7 days
            '$or': [
                {'buzzheavier_id': {'$ne': None}},
                {'gphotos_id': {'$ne': None}},
            ],
        },
        {
            'token': 1,
            'drive_id': 1,
            'filename': 1,
            'filesize': 1,
            'filetype': 1,
            'created_time': 1,
            '_id': 0,
        }
    ).sort('created_time', -1).limit(50)  # Sort by newest first, limit to 50
    seeded_docs = []
    now = __import__('datetime').datetime.utcnow()
    seed_count = 0
    
    for doc in seed_cursor:
        token_val = doc.get('token')
        created_time = doc.get('created_time')
        filename = doc.get('filename', 'unknown')
        
        if not token_val:
            print(f"[WARN] PD_LOST seeding: Skipping doc without token: {doc}")
            continue
        
        try:
            # Check if document already exists
            existing_doc = db.db.pd_lost.find_one({'token': token_val}, {'_id': 1})

            if existing_doc:
                # Document exists, just update the fields (exclude created_time)
                set_fields = {
                    k: v for k, v in doc.items() if k not in ['token', 'created_time']
                }
                set_fields.update({
                    'retry_count': 0,
                    'error': None,
                    'skip': False,
                })
                result = db.db.pd_lost.update_one(
                    {'token': token_val},
                    {'$set': set_fields}
                )
            else:
                # Document doesn't exist, create new one with created_time
                insert_fields = {
                    'token': token_val,
                    'created_time': now,
                    'retry_count': 0,
                    'error': None,
                    'skip': False,
                }
                # Add all fields from the source document
                for k, v in doc.items():
                    if k != 'token':  # token already set above
                        insert_fields[k] = v
                result = db.db.pd_lost.insert_one(insert_fields)
            
            if (hasattr(result, 'upserted_id') and result.upserted_id) or \
               (hasattr(result, 'modified_count') and result.modified_count > 0) or \
               (hasattr(result, 'inserted_id') and result.inserted_id):
                seeded_docs.append({'token': token_val})
                seed_count += 1
                print(f"[DEBUG] PD_LOST seeding: Added/Updated token: {token_val} | File: {filename} | Created: {created_time}")
            else:
                print(f"[DEBUG] PD_LOST seeding: No change for token: {token_val}")
                
        except Exception as e:
            print(f"[ERROR] PD_LOST seeding: Failed to process token {token_val}: {e}")
            continue
    
    logger.info(f"PD_LOST seeding: Processed {seed_count} documents, successfully seeded {len(seeded_docs)} tasks")

    if seeded_docs:
        logger.info(f"Seeded {len(seeded_docs)} tasks into pd_lost from links (last 7 days)")
        # Return all tasks including newly seeded ones
        all_tasks = list(db.db.pd_lost.find({}, projection).sort('created_time', 1))
        logger.debug(f"PD_LOST seeding: Total tasks after seeding: {len(all_tasks)}")
        return all_tasks
    
    logger.debug("PD_LOST seeding: No tasks were seeded from last 7 days")
    return []


@pd_lost_api.route('/<api_key>/retry-pixeldrain', methods=['GET'])
def retry_pixeldrain_manual(api_key: str):
    """Manually trigger PixelDrain retry for all entries in pd_lost.

    Returns JSON with how many tasks were detected and whether a worker was started.
    """
    try:
        global pd_lost_retry_active
        if not validate_api_key(api_key):
            return jsonify({'success': False, 'error': 'Invalid or inactive API key'}), 401

        # Check if PD_LOST processing is enabled
        if os.getenv('PD_LOST', 'true').lower() != 'true':
            return jsonify({'success': False, 'error': 'PD_LOST processing is disabled (PD_LOST=false)'}), 503

        # Prevent concurrent workers
        with pd_lost_retry_lock:
            if pd_lost_retry_active:
                return jsonify({'success': True, 'message': 'Retry already in progress'}), 200
            pd_lost_retry_active = True

        tasks = _load_pd_lost_tasks(seed_if_empty=True)

        if not tasks:
            with pd_lost_retry_lock:
                pd_lost_retry_active = False
            return jsonify({'success': True, 'message': 'No tasks found for PixelDrain retry', 'tasks_found': 0}), 200

        _begin_pd_lost_processing(tasks)

        # Get seeding potential count for last 7 days
        import datetime
        seven_days_ago = datetime.datetime.utcnow() - datetime.timedelta(days=7)
        seedable_links = db.db.links.count_documents({
            'status': 'completed',
            'pixeldrain_id': None,
            'created_time': {'$gte': seven_days_ago},
            '$or': [
                {'buzzheavier_id': {'$ne': None}},
                {'gphotos_id': {'$ne': None}},
            ],
        })
        
        return jsonify({
            'success': True, 
            'message': f'Started PixelDrain retry for {len(tasks)} tasks', 
            'tasks_found': len(tasks), 
            'max_concurrency': MAX_PD_LOST,
            'seedable_links_last_7_days': seedable_links,
            'seeding_criteria': 'Last 7 days, completed status, missing pixeldrain_id, has buzzheavier_id or gphotos_id'
        }), 200

    except Exception as e:
        # Reset active flag on failure to start
        with pd_lost_retry_lock:
            pd_lost_retry_active = False
        return jsonify({'success': False, 'error': str(e)}), 500


def recover_pd_lost_on_startup() -> None:
    """Automatically load and process pd_lost tasks on startup with logs."""
    try:
        # Check if PD_LOST processing is enabled
        if os.getenv('PD_LOST', 'true').lower() != 'true':
            logger.info("PD_LOST recovery: Skipped (PD_LOST=false)")
            return
        
        # Small delay to ensure DB is ready
        time.sleep(2)
        logger.info(f"PD_LOST recovery: MAX_PD_LOST={MAX_PD_LOST}")
        
        # First try to seed from existing links if pd_lost is empty, then process tasks
        tasks = _load_pd_lost_tasks(seed_if_empty=True)
        if not tasks:
            logger.info("PD_LOST recovery: No existing tasks found and no seeding possible")
            return
            
        print(f"[INFO] PD_LOST recovery: Found {len(tasks)} tasks (including seeded). Starting all...")
        _begin_pd_lost_processing(tasks)
        
    except Exception as e:
        print(f"[ERROR] PD_LOST recovery failed: {e}")


def periodic_pd_lost_enqueue() -> None:
    """Periodically check and enqueue remaining pd_lost tasks."""
    try:
        # Check if PD_LOST processing is enabled
        if os.getenv('PD_LOST', 'true').lower() != 'true':
            return
        
        # Get eligible tasks that aren't in queue yet
        tasks = _load_pd_lost_tasks(seed_if_empty=False)
        if not tasks:
            return
            
        # Enqueue tasks to Redis (upsert will handle duplicates automatically)
        redis_queue = RedisJobQueue()
        enqueued_count = 0
        for task in tasks:
            token = task.get('token')
            drive_id = task.get('drive_id')
            
            if token and drive_id:
                redis_queue.enqueue_pdlost(token, drive_id)
                enqueued_count += 1
                
        if enqueued_count > 0:
            print(f"[INFO] Periodic pd_lost: ✅ Enqueued {enqueued_count} additional Redis tasks")
            
    except Exception as e:
        print(f"[ERROR] Periodic pd_lost enqueue failed: {e}")


@pd_lost_api.route('/<api_key>/pd-lost/status', methods=['GET'])
def pd_lost_status(api_key: str):
    """Return pd_lost processing status: counts and concurrency."""
    try:
        if not validate_api_key(api_key):
            return jsonify({'success': False, 'error': 'Invalid or inactive API key'}), 401

        total = db.db.pd_lost.count_documents({})
        pending = db.db.pd_lost.count_documents({'$or': [{'skip': {'$exists': False}}, {'skip': False}]})
        skipped = db.db.pd_lost.count_documents({'skip': True})

        # Check seeding potential for last 7 days
        import datetime
        seven_days_ago = datetime.datetime.utcnow() - datetime.timedelta(days=7)
        seedable_links = db.db.links.count_documents({
            'status': 'completed',
            'pixeldrain_id': None,
            'created_time': {'$gte': seven_days_ago},
            '$or': [
                {'buzzheavier_id': {'$ne': None}},
                {'gphotos_id': {'$ne': None}},
            ],
        })

        return jsonify({
            'success': True,
            'data': {
                'total_pd_lost': total,
                'pending_pd_lost': pending,
                'skipped_pd_lost': skipped,
                'seedable_links_last_7_days': seedable_links,
                'queue_size': pd_lost_queue.qsize(),
                'active_tasks': pd_lost_active_tasks,
                'max_concurrency': MAX_PD_LOST,
            }
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

