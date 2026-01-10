import os
import time
import threading
import logging
import uuid
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from config import (
    MAX_NEW_UPLOAD,
    MAX_PD_LOST,
    REDIS_NO_JOBS_SLEEP_SEC,
    REDIS_WORKER_INTERVAL_SEC,
)
from queueing.redis_queue import RedisJobQueue
from utils.logging_config import get_modern_logger, print_status_table

logger = get_modern_logger("RedisWorker")

class RedisWorker:
    """Redis-based job worker for high-performance job processing"""
    
    def __init__(self):
        self.worker_id = str(uuid.uuid4())
        self.queue = RedisJobQueue()
        self.shutdown_event = threading.Event()
        self.worker_threads = []
        
        # GLOBAL limit for ALL job types combined
        self.max_concurrent_total = int(os.getenv('MAX_CONCURRENT_TASKS', '3'))
        
        # Priority order for job types (higher priority = processed first)
        self.job_type_priority = {
            'process_file': 1,  # Highest priority
            'auto_upload': 2,
            'new_photo': 3,
            'pd_lost': 4
        }
        
        # Worker control - tracks active workers per type
        self.active_workers = {
            'process_file': 0,
            'new_photo': 0,
            'pd_lost': 0,
            'auto_upload': 0
        }
        
        # Single global lock for all job types
        self.global_worker_lock = threading.Lock()
    
    def start(self):
        """Start the Redis worker"""
        logger.process_start(f"Redis Worker ({self.worker_id})")
        
        # Start heartbeat thread FIRST
        heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        heartbeat_thread.start()
        self.worker_threads.append(heartbeat_thread)
        
        # Reset processing jobs from DEAD workers only
        processing_reset_count = self.queue.reset_all_processing_jobs()
        if processing_reset_count > 0:
            logger.warning(f"Server restart recovery: Reset {processing_reset_count} processing jobs from dead workers")
        
        # Force reset stuck jobs on startup (auto-recovery)
        stuck_count = self.queue.reset_stuck_jobs()
        if stuck_count > 0:
            logger.warning(f"Startup recovery: Reset {stuck_count} stuck jobs")
        
        # Force reset emergency stuck jobs on startup (more aggressive recovery)
        emergency_stuck_count = self.queue.force_reset_stuck_jobs()
        if emergency_stuck_count > 0:
            logger.error(f"Emergency recovery: Force reset {emergency_stuck_count} emergency stuck jobs")
        
        # Start worker thread
        worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        worker_thread.start()
        self.worker_threads.append(worker_thread)
        
        # Start health check thread
        health_thread = threading.Thread(target=self._health_check_loop, daemon=True)
        health_thread.start()
        self.worker_threads.append(health_thread)
        
        logger.success("Redis worker started successfully")
    
    def stop(self):
        """Stop the Redis worker gracefully"""
        logger.info("🛑 Stopping Redis worker...")
        self.shutdown_event.set()
        
        # Wait for workers to complete
        for thread in self.worker_threads:
            thread.join(timeout=30)
            
        # Remove heartbeat key
        try:
            self.queue.redis.delete(f"worker_heartbeat:{self.worker_id}")
        except:
            pass
        
        logger.info("✅ Redis worker stopped")
    
    def _heartbeat_loop(self):
        """Periodic heartbeat to keep worker alive in Redis"""
        logger.info(f"💓 Starting heartbeat loop for worker {self.worker_id}")
        while not self.shutdown_event.is_set():
            try:
                # Set heartbeat key with 30s TTL
                self.queue.redis.setex(f"worker_heartbeat:{self.worker_id}", 30, "alive")
                time.sleep(10)
            except Exception as e:
                logger.error(f"❌ Heartbeat error: {e}")
                time.sleep(5)

    def _worker_loop(self):
        """Main worker loop"""
        check_interval = REDIS_WORKER_INTERVAL_SEC
        no_jobs_sleep = REDIS_NO_JOBS_SLEEP_SEC  # Sleep longer when no jobs
        
        logger.info(f"🔄 Entering worker loop with {check_interval}s interval")
        
        consecutive_no_jobs = 0
        
        while not self.shutdown_event.is_set():
            try:
                # Determine allowed job types based on env vars
                enable_process = os.getenv('ENABLE_PROCESS', 'true').lower() == 'true'
                enable_pd_lost = os.getenv('PD_LOST', 'true').lower() == 'true'
                
                # Build job types list based on configuration
                job_types = []
                
                # process_file and new_photo controlled by ENABLE_PROCESS
                if enable_process:
                    job_types.append(('process_file', self._handle_process_file))
                    job_types.append(('new_photo', self._handle_new_photo))
                
                # pd_lost controlled by PD_LOST
                if enable_pd_lost:
                    job_types.append(('pd_lost', self._handle_pd_lost))
                
                # auto_upload is always included (controlled separately by PIXELDRAIN_AUTO_UPLOAD_PROCESS in autouploader.py)
                job_types.append(('auto_upload', self._handle_auto_upload))
                
                jobs_found = False
                for job_type, handler in job_types:
                    if not self.shutdown_event.is_set():
                        # Check GLOBAL limit before attempting
                        total_active = sum(self.active_workers.values())
                        if total_active >= self.max_concurrent_total:
                            break  # Global limit reached, stop checking all job types
                        
                        if self._process_job_type(job_type, handler):
                            jobs_found = True
                            # Only start ONE job per loop iteration to prevent race condition
                            break
                
                # Adjust sleep based on whether jobs were found
                if jobs_found:
                    consecutive_no_jobs = 0
                    sleep_time = check_interval
                else:
                    consecutive_no_jobs += 1
                    # Sleep longer when no jobs found (exponential backoff, max 30s)
                    sleep_time = min(no_jobs_sleep * (2 ** min(consecutive_no_jobs - 1, 3)), 30)
                    
                    # Log only occasionally when no jobs
                    if consecutive_no_jobs % 5 == 0:  # Log every 5th consecutive no-jobs
                        logger.info(f"💤 No jobs found for {consecutive_no_jobs} consecutive checks, sleeping for {sleep_time}s")
                
                # Sleep before next check
                time.sleep(sleep_time)
                
            except Exception as e:
                logger.error(f"❌ Error in worker loop: {e}")
                time.sleep(check_interval * 2)  # Back off on error
    
    def _process_job_type(self, job_type: str, handler):
        """Process available jobs of a specific type"""
        try:
            with self.global_worker_lock:
                # Check GLOBAL limit (all job types combined)
                total_active = sum(self.active_workers.values())
                if total_active >= self.max_concurrent_total:
                    logger.debug(f"⏸️ Global limit reached: {total_active}/{self.max_concurrent_total} active workers")
                    return False  # Global max workers reached
                
                # Try to fetch and lock a job
                job = self.queue.fetch_and_lock(job_type, worker_id=self.worker_id)
                if job:
                    # Increment active worker count for this type
                    self.active_workers[job_type] += 1
                    
                    # Start worker thread
                    worker_thread = threading.Thread(
                        target=self._job_worker,
                        args=(job_type, job, handler),
                        daemon=True
                    )
                    worker_thread.start()
                    
                    total_after = sum(self.active_workers.values())
                    logger.info(f"🚀 Started {job_type} worker for token {job.get('token')} (Total: {total_after}/{self.max_concurrent_total})")
                    return True # Indicate job found
                else:
                    # No jobs available
                    return False # Indicate no job found
                    
        except Exception as e:
            logger.error(f"❌ Error processing {job_type}: {e}")
            return False # Indicate no job found
    
    def _job_worker(self, job_type: str, job: dict, handler):
        """Individual job worker"""
        token = job.get('token', 'Unknown')
        
        try:
            # Validate job has required fields before processing (job type specific)
            if not job.get('token') or not job.get('type'):
                raise ValueError(f"Corrupted job: Missing token or type. Job data: {job}")
            
            # Job type specific validation
            if job_type == 'auto_upload':
                if not job.get('service_type') or not job.get('link_data'):
                    raise ValueError(f"Corrupted auto_upload job: Missing service_type or link_data. Job data: {job}")
            else:
                if not job.get('file_id'):
                    raise ValueError(f"Corrupted {job_type} job: Missing file_id. Job data: {job}")
            
            logger.info(f"⚡ Processing {job_type} job: {token}")
            
            # Call the appropriate handler
            handler(job)
            
            # Mark as done
            self.queue.mark_done(job.get('_id', ''), token)
            logger.info(f"✅ Completed {job_type} job: {token}")
            
        except Exception as e:
            # Enhanced error logging with full traceback
            import traceback
            error_msg = f"Job {token} failed: {str(e)}"
            logger.error(f"{error_msg}\nTraceback: {traceback.format_exc()}")
            
            try:
                # Only mark as failed if token exists (not corrupted)
                if token and token != 'Unknown':
                    self.queue.mark_failed(token, str(e))
                else:
                    logger.error(f"Cannot mark corrupted job as failed: {job}")
            except Exception as db_error:
                logger.error(f"Failed to mark job {token} as failed: {db_error}")
        
        finally:
            # Decrement active worker count
            with self.global_worker_lock:
                self.active_workers[job_type] -= 1
                total_active = sum(self.active_workers.values())
                logger.debug(f"📊 {job_type} worker finished. Total active: {total_active}/{self.max_concurrent_total}")
    
    def _handle_process_file(self, job: dict):
        """Handle process_file jobs"""
        from plugins.database_factory import get_database
        from plugins.service.processing_service import ProcessingService
        
        # Parse file_info from JSON string
        file_info_str = job.get('file_info', '{}')
        try:
            import json
            file_info = json.loads(file_info_str) if file_info_str else None
        except json.JSONDecodeError as e:
            print(f"Redis worker JSON decode error: {e}")
            file_info = None
        
        # If file_info is empty, fetch from database (recovery from server restart)
        if not file_info or not file_info.get('filename'):
            try:
                db = get_database()
                link_data = db.get_link(job['token'])
                if link_data and link_data.get('filename'):
                    file_info = {
                        'filename': link_data.get('filename'),
                        'filesize': link_data.get('filesize'),
                        'filetype': link_data.get('filetype'),
                        'file_id': link_data.get('drive_id')
                    }
                    print(f"✅ Recovery: Fetched file_info from database for token {job['token']}: {file_info.get('filename')}")
                else:
                    print(f"⚠️ Recovery: No filename found in database for token {job['token']}")
            except Exception as e:
                print(f"❌ Recovery: Failed to fetch file_info from database for token {job['token']}: {e}")
        
        is_reupload = job.get('is_reupload', 'false').lower() == 'true'
        
        # Log reupload status for debugging
        if is_reupload:
            logger.info(f"🔄 Processing REUPLOAD job: {job['token']}")
        else:
            logger.info(f"⚡ Processing process_file job: {job['token']}")
        
        # Create processing service instance
        db = get_database()
        queue_instance = RedisJobQueue()
        processing_service = ProcessingService(db, queue_instance)
        processing_service.process_file(
            job['token'],
            job['file_id'],
            file_info,
            is_reupload
        )
    
    def _handle_new_photo(self, job: dict):
        """Handle new_photo jobs"""
        from api.new_photo_routes import process_new_photo_upload
        
        # Parse file_info from JSON string
        file_info_str = job.get('file_info', '{}')
        try:
            import json
            file_info = json.loads(file_info_str) if file_info_str else None
        except json.JSONDecodeError:
            file_info = None
        
        process_new_photo_upload(
            job['token'],
            job['file_id'],
            file_info
        )
    
    def _handle_pd_lost(self, job: dict):
        """Handle pd_lost jobs"""
        from api.pd_lost import _retry_pixeldrain_single
        
        # Parse file_info from JSON string
        file_info_str = job.get('file_info', '{}')
        try:
            import json
            file_info = json.loads(file_info_str) if file_info_str else None
        except json.JSONDecodeError:
            file_info = None
        
        _retry_pixeldrain_single(
            job['token'],
            job['file_id'],
            file_info
        )
    
    def _handle_auto_upload(self, job: dict):
        """Handle auto_upload jobs"""
        service_type = job.get('service_type', 'pixeldrain')
        link_data_str = job.get('link_data', '{}')
        
        try:
            import json
            link_data = json.loads(link_data_str) if link_data_str else {}
        except json.JSONDecodeError:
            link_data = {}
        
        if service_type == 'pixeldrain':
            from plugins.autouploader import AutoUploader
            uploader = AutoUploader()
            uploader.process_file(link_data)
        elif service_type == 'buzzheavier':
            from plugins.buzzauto import BuzzAuto
            uploader = BuzzAuto()
            uploader.process_file(link_data)
        else:
            raise ValueError(f"Unknown service type: {service_type}")
    
    def _health_check_loop(self):
        """Periodic health check and maintenance loop"""
        health_check_interval = 15  # 15 seconds (more frequent for faster stuck job detection)
        
        logger.info(f"🏥 Starting health check loop with {health_check_interval}s interval")
        
        while not self.shutdown_event.is_set():
            try:
                time.sleep(health_check_interval)
                
                if self.shutdown_event.is_set():
                    break
                
                logger.info("🏥 Performing health check...")
                self._perform_health_check()
                
                # Process retry jobs
                self.queue.process_retry_jobs()
                
            except Exception as e:
                logger.error(f"❌ Error in health check loop: {e}")
    
    def _perform_health_check(self):
        """Perform periodic health check and maintenance"""
        try:
            # Clean up orphaned jobs first
            orphaned_count = self.queue.cleanup_orphaned_jobs()
            if orphaned_count > 0:
                logger.info(f"🧹 Health check cleaned up {orphaned_count} orphaned jobs")
            
            # Check for stuck jobs with more aggressive timeout
            stuck_jobs = self.queue.get_stuck_jobs()
            if stuck_jobs:
                logger.warning(f"⚠️ Health check found {len(stuck_jobs)} stuck jobs - auto-resetting")
                for job in stuck_jobs:
                    logger.warning(f"   - Stuck job: {job['token']} ({job['type']}) since {job['locked_at']}")
                reset_count = self.queue.reset_stuck_jobs()
                logger.info(f"🔄 Health check reset {reset_count} stuck jobs")
            
            # Force cleanup of jobs stuck in processing for too long (emergency recovery)
            emergency_stuck = self.queue.get_emergency_stuck_jobs()
            if emergency_stuck:
                logger.error(f"🚨 EMERGENCY: Found {len(emergency_stuck)} jobs stuck for over 1 hour - force resetting")
                for job in emergency_stuck:
                    logger.error(f"   - Emergency stuck: {job['token']} ({job['type']}) since {job['locked_at']}")
                emergency_reset_count = self.queue.force_reset_stuck_jobs()
                logger.info(f"🚨 Emergency reset {emergency_reset_count} stuck jobs")
            
            # Clean up expired jobs
            cleanup_count = self.queue.cleanup_expired_jobs()
            if cleanup_count > 0:
                logger.info(f"🧹 Health check cleaned up {cleanup_count} expired jobs")
            
            # Log queue health status
            stats = self.queue.get_queue_stats()
            total_processing = sum(data.get('processing', 0) for data in stats.values())
            total_queued = sum(data.get('queued', 0) for data in stats.values())
            
            # Get retry system stats
            retry_stats = self.queue.get_retry_stats()
            waiting_retry = retry_stats.get('waiting_retry', 0)
            ready_for_retry = retry_stats.get('ready_for_retry_now', 0)
            
            logger.info(f"📊 Queue Status: {total_processing} processing, {total_queued} queued | Retry: {waiting_retry} waiting, {ready_for_retry} ready")
            
            # Log worker status
            total_active = sum(self.active_workers.values())
            logger.info(f"👷 Total active workers: {total_active}/{self.max_concurrent_total} on server {self.worker_id[:8]}")
            for job_type, count in self.active_workers.items():
                if count > 0:  # Only log active job types
                    logger.info(f"   - {job_type}: {count} active")
                
        except Exception as e:
            logger.error(f"❌ Health check error: {e}")
    
    def get_status(self) -> Dict[str, Any]:
        """Get worker status for monitoring"""
        try:
            stats = self.queue.get_queue_stats()
            
            return {
                'status': 'running' if not self.shutdown_event.is_set() else 'stopping',
                'active_workers': self.active_workers.copy(),
                'max_workers': self.max_workers.copy(),
                'queue_stats': stats,
                'uptime': getattr(self, '_start_time', None),
                'last_health_check': getattr(self, '_last_health_check', None)
            }
        except Exception as e:
            logger.error(f"❌ Error getting worker status: {e}")
            return {'status': 'error', 'error': str(e)}

# Global worker instance
_worker = None

def start_redis_worker():
    """Start the Redis worker globally"""
    global _worker
    if _worker is None:
        _worker = RedisWorker()
        _worker.start()
    return _worker

def stop_redis_worker():
    """Stop the Redis worker globally"""
    global _worker
    if _worker:
        _worker.stop()
        _worker = None

def get_redis_worker() -> Optional[RedisWorker]:
    """Get the global Redis worker instance"""
    return _worker
