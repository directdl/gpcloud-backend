import os
import time
import json
import threading
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

from utils.redis_client import get_redis_client
from utils.serialization import make_serializable
from config import (
    JOB_LOCK_TIMEOUT_SEC,
    JOB_TTL_SEC,
    MAX_NEW_UPLOAD,
    MAX_PD_LOST,
)

DEFAULT_JOB_TTL_SEC = JOB_TTL_SEC
DEFAULT_LOCK_TIMEOUT_SEC = JOB_LOCK_TIMEOUT_SEC

class RedisJobQueue:
    """Redis-based job queue for high-performance job processing"""

    def __init__(self) -> None:
        self.redis = get_redis_client().redis
        self.job_prefix = "job:"
        self.queue_prefix = "queue:"
        self.processing_prefix = "processing:"
        self.failed_prefix = "failed:"
        
    def _get_job_key(self, token: str) -> str:
        """Get Redis key for job hash"""
        return f"{self.job_prefix}{token}"
    
    def _get_queue_key(self, job_type: str) -> str:
        """Get Redis key for priority queue"""
        return f"{self.queue_prefix}{job_type}"
    
    def _get_processing_key(self, job_type: str) -> str:
        """Get Redis key for processing set"""
        return f"{self.processing_prefix}{job_type}"
    
    def _get_failed_key(self) -> str:
        """Get Redis key for failed jobs"""
        return f"{self.failed_prefix}jobs"
    
    def _get_retry_key(self) -> str:
        """Get Redis key for retry jobs sorted set"""
        return f"retry:jobs"

    def _get_retry_payload_key(self) -> str:
        """Get Redis key for retry job payload hash"""
        return "retry:payloads"

    def enqueue_process(self, token: str, file_id: str, file_info: Optional[dict], is_reupload: bool) -> str:
        """Enqueue a process_file job"""
        # Reupload gets highest priority (0), regular process_file gets priority 1
        priority = 0 if is_reupload else 1
        
        # Check if job already exists (only active jobs should exist due to cleanup)
        existing_job = self.redis.exists(self._get_job_key(token))
        if existing_job:
            existing_status = self.redis.hget(self._get_job_key(token), 'status')
            
            if is_reupload:
                print(f"🔄 Reupload: Force re-queuing existing {existing_status} job for token {token}")
                # Remove from all possible locations
                self.redis.zrem(self._get_queue_key('process_file'), token)
                self.redis.srem(self._get_processing_key('process_file'), token)
                self.redis.delete(self._get_job_key(token))  # Clean existing job
            else:
                # For regular jobs, only prevent active duplicates
                if existing_status in ['queued', 'processing']:
                    print(f"Job already exists for token {token} (status: {existing_status})")
                    return f"process:{token}"
                else:
                    print(f"Overwriting stale job for token {token} (status: {existing_status})")
                    # Clean up stale job
                    self.redis.zrem(self._get_queue_key('process_file'), token)
                    self.redis.srem(self._get_processing_key('process_file'), token)
                    self.redis.delete(self._get_job_key(token))
        
        # Convert ObjectIds to strings for JSON serialization
        serializable_file_info = make_serializable(file_info or {})
        
        # Create job data
        job_data = {
            'type': 'process_file',
            'token': token,
            'file_id': file_id,
            'file_info': json.dumps(serializable_file_info),
            'is_reupload': str(is_reupload),
            'priority': str(priority),
            'status': 'queued',
            'created_at': datetime.utcnow().isoformat(),
            'locked_at': '',
            'error': '',
            'ttl': str(DEFAULT_JOB_TTL_SEC)
        }
        
        # Store job hash
        self.redis.hset(self._get_job_key(token), mapping=job_data)
        
        # Add to priority queue (timestamp + priority for ordering)
        # Higher priority = lower score (processed first)
        if is_reupload:
            score = time.time() - 1000000  # Very high priority (immediate processing)
            print(f"✅ Enqueued REUPLOAD job: {token} (HIGHEST PRIORITY)")
        else:
            score = time.time() + (10 - priority) * 1000000
            print(f"✅ Enqueued process_file job: {token} (priority: {priority})")
            
        self.redis.zadd(self._get_queue_key('process_file'), {token: score})
        
        # Set TTL
        self.redis.expire(self._get_job_key(token), DEFAULT_JOB_TTL_SEC)
        
        # If this is a reupload, remove from retry queue if it exists
        if is_reupload:
            self._remove_from_retry_queue(token)
        
        return f"process:{token}"
    
    def _remove_from_retry_queue(self, token: str) -> bool:
        """Remove a job from retry queue if it exists"""
        try:
            removed = self.redis.zrem(self._get_retry_key(), token)
            if removed:
                self.redis.hdel(self._get_retry_payload_key(), token)
                print(f"🎉 Removed token {token} from retry queue (reupload success)")
                return True

            # Backward compatibility: legacy entries stored full JSON payload as member
            retry_jobs = self.redis.zrange(self._get_retry_key(), 0, -1)
            for job_member in retry_jobs:
                try:
                    job_data = json.loads(job_member)
                except json.JSONDecodeError:
                    continue

                if job_data.get('token') == token:
                    if self.redis.zrem(self._get_retry_key(), job_member):
                        self.redis.hdel(self._get_retry_payload_key(), token)
                        print(f"🎉 Removed legacy retry entry for token {token}")
                        return True
            return False
        except Exception as e:
            print(f"❌ Error removing from retry queue: {e}")
            return False
    
    def enqueue_newphoto(self, token: str, file_id: str, file_info: Optional[dict]) -> str:
        """Enqueue a new_photo job"""
        # Check if job already exists
        existing_job = self.redis.exists(self._get_job_key(token))
        if existing_job:
            print(f"Job already exists for token {token}")
            return f"newphoto:{token}"
        
        # Convert ObjectIds to strings for JSON serialization
        serializable_file_info = make_serializable(file_info or {})
        
        # Create job data
        job_data = {
            'type': 'new_photo',
            'token': token,
            'file_id': file_id,
            'file_info': json.dumps(serializable_file_info),
            'is_reupload': 'false',
            'priority': '1',
            'status': 'queued',
            'created_at': datetime.utcnow().isoformat(),
            'locked_at': '',
            'error': '',
            'ttl': str(DEFAULT_JOB_TTL_SEC)
        }
        
        # Store job hash
        self.redis.hset(self._get_job_key(token), mapping=job_data)
        
        # Add to priority queue
        score = time.time() + 9000000  # Priority 1
        self.redis.zadd(self._get_queue_key('new_photo'), {token: score})
        
        # Set TTL
        self.redis.expire(self._get_job_key(token), DEFAULT_JOB_TTL_SEC)
        
        print(f"✅ Enqueued new_photo job: {token}")
        return f"newphoto:{token}"

    def enqueue_pdlost(self, token: str, drive_id: str) -> str:
        """Enqueue a pd_lost job"""
        # Check if job already exists
        existing_job = self.redis.exists(self._get_job_key(token))
        if existing_job:
            print(f"Job already exists for token {token}")
            return f"pdlost:{token}"
        
        # Create job data
        job_data = {
            'type': 'pd_lost',
            'token': token,
            'file_id': drive_id,
            'file_info': '{}',
            'is_reupload': 'false',
            'priority': '2',
            'status': 'queued',
            'created_at': datetime.utcnow().isoformat(),
            'locked_at': '',
            'error': '',
            'ttl': str(DEFAULT_JOB_TTL_SEC)
        }
        
        # Store job hash
        self.redis.hset(self._get_job_key(token), mapping=job_data)
        
        # Add to priority queue
        score = time.time() + 8000000  # Priority 2
        self.redis.zadd(self._get_queue_key('pd_lost'), {token: score})
        
        # Set TTL
        self.redis.expire(self._get_job_key(token), DEFAULT_JOB_TTL_SEC)
        
        print(f"✅ Enqueued pd_lost job: {token}")
        return f"pdlost:{token}"

    def enqueue_auto_upload(self, token: str, service_type: str, link_data: dict) -> str:
        """Enqueue an auto_upload job"""
        # Check if job already exists
        existing_job = self.redis.exists(self._get_job_key(token))
        if existing_job:
            print(f"Job already exists for token {token}")
            return f"auto_upload:{service_type}:{token}"
        
        # Convert ObjectIds to strings for JSON serialization
        serializable_link_data = make_serializable(link_data)
        
        # Create job data
        job_data = {
            'type': 'auto_upload',
            'token': token,
            'service_type': service_type,
            'link_data': json.dumps(serializable_link_data),
            'is_reupload': 'false',
            'priority': '3',
            'status': 'queued',
            'created_at': datetime.utcnow().isoformat(),
            'locked_at': '',
            'error': '',
            'ttl': str(DEFAULT_JOB_TTL_SEC)
        }
        
        # Store job hash
        self.redis.hset(self._get_job_key(token), mapping=job_data)
        
        # Add to priority queue
        score = time.time() + 7000000  # Priority 3
        self.redis.zadd(self._get_queue_key('auto_upload'), {token: score})
        
        # Set TTL
        self.redis.expire(self._get_job_key(token), DEFAULT_JOB_TTL_SEC)
        
        print(f"✅ Enqueued auto_upload job: {token} ({service_type})")
        return f"auto_upload:{service_type}:{token}"
    
    def fetch_and_lock(self, job_type: str, worker_id: str = None) -> Optional[dict]:
        """Fetch and lock a job from the queue"""
        now = datetime.utcnow()
        lock_timeout = now - timedelta(seconds=DEFAULT_LOCK_TIMEOUT_SEC)
        
        # REMOVED: Global concurrency check using Redis SCARD.
        # Each worker manages its own concurrency locally via RedisWorker.active_workers.
        # Checking global processing count prevents scaling across multiple servers.
        
        # Check if there are queued jobs
        queued_count = self.redis.zcard(self._get_queue_key(job_type))
        if queued_count == 0:
            return None
        
        # Get highest priority job (lowest score)
        jobs = self.redis.zrange(self._get_queue_key(job_type), 0, 0, withscores=True)
        if not jobs:
            return None
        
        token, score = jobs[0]
        
        # Use Redis transaction to atomically fetch and lock
        pipe = self.redis.pipeline()
        
        try:
            # Remove from queue
            pipe.zrem(self._get_queue_key(job_type), token)
            
            # Add to processing
            pipe.sadd(self._get_processing_key(job_type), token)
            
            # Update job status
            pipe.hset(self._get_job_key(token), 'status', 'processing')
            pipe.hset(self._get_job_key(token), 'locked_at', now.isoformat())
            if worker_id:
                pipe.hset(self._get_job_key(token), 'worker_id', worker_id)
            
            # Execute transaction
            results = pipe.execute()
            
            if results[0] == 1:  # Successfully removed from queue
                # Get job data
                job_data = self.redis.hgetall(self._get_job_key(token))
                if job_data:
                    # Validate critical fields exist (job type specific)
                    if not job_data.get('token') or not job_data.get('type'):
                        print(f"🛑 Corrupted job detected during fetch: Missing token or type for {token} ({job_type}) - deleting")
                        self.redis.delete(self._get_job_key(token))
                        self.redis.srem(self._get_processing_key(job_type), token)
                        return None
                    
                    # Job type specific validation
                    if job_type == 'auto_upload':
                        # auto_upload needs service_type and link_data
                        if not job_data.get('service_type') or not job_data.get('link_data'):
                            print(f"🛑 Corrupted auto_upload job: Missing service_type or link_data for {token} - deleting")
                            self.redis.delete(self._get_job_key(token))
                            self.redis.srem(self._get_processing_key(job_type), token)
                            return None
                    else:
                        # Other job types need file_id
                        if not job_data.get('file_id'):
                            print(f"🛑 Corrupted {job_type} job: Missing file_id for {token} - deleting")
                            self.redis.delete(self._get_job_key(token))
                            self.redis.srem(self._get_processing_key(job_type), token)
                            return None
                    
                    return job_data
                else:
                    return None
            else:
                return None
                
        except Exception as e:
            print(f"❌ Error during fetch_and_lock: {e}")
            return None

    def mark_done(self, job_id: str, token: str) -> None:
        """Mark a job as completed and clean up Redis data"""
        try:
            # Get job type before deletion
            job_type = self.redis.hget(self._get_job_key(token), 'type')
            
            # IMPORTANT: Remove from processing set FIRST (before deleting hash)
            # This prevents race condition where cleanup_orphaned_jobs sees token
            # in processing set but hash is partially deleted
            if job_type:
                self.redis.srem(self._get_processing_key(job_type), token)
            
            # IMPORTANT: Remove from retry queue if it was a retry job that succeeded
            self._remove_from_retry_queue(token)
            
            # Delete the job hash completely (saves Redis memory)
            # Do this AFTER removing from processing set
            self.redis.delete(self._get_job_key(token))
            
            # Optional: Add to completed tracking with timestamp (for stats)
            self.redis.zadd('completed_jobs', {token: time.time()})
            
            # Keep only last 1000 completed jobs for stats
            completed_count = self.redis.zcard('completed_jobs')
            if completed_count > 1000:
                # Remove oldest completed jobs
                self.redis.zremrangebyrank('completed_jobs', 0, completed_count - 1001)
            
            print(f"✅ Job {token} completed and cleaned up")
        except Exception as e:
            print(f"❌ Error marking job {token} as done: {e}")

    def mark_failed(self, token: str, error: str) -> None:
        """Mark a job as failed and clean up Redis data"""
        try:
            # Get job type before deletion
            job_type = self.redis.hget(self._get_job_key(token), 'type')
            
            # IMPORTANT: Remove from processing set FIRST (before any cleanup/deletion)
            # This prevents race condition with cleanup_orphaned_jobs
            if job_type:
                self.redis.srem(self._get_processing_key(job_type), token)
            
            # Add to failed jobs tracking with error info and timestamp
            failed_data = {
                'token': token,
                'error': error,
                'timestamp': time.time(),
                'job_type': job_type or 'unknown'
            }
            self.redis.zadd(self._get_failed_key(), {f"{token}:{error[:50]}": time.time()})
            
            # Keep only last 500 failed jobs for debugging
            failed_count = self.redis.zcard(self._get_failed_key())
            if failed_count > 500:
                # Remove oldest failed jobs
                self.redis.zremrangebyrank(self._get_failed_key(), 0, failed_count - 501)
            
            # Handle retry for process_file jobs only
            if job_type == 'process_file':
                self._handle_process_file_retry(token, error)
            else:
                # Delete the job hash completely for non-retryable jobs
                self.redis.delete(self._get_job_key(token))
            
            print(f"❌ Job {token} failed and cleaned up: {error}")
        except Exception as e:
            print(f"❌ Error marking job {token} as failed: {e}")

    def _handle_process_file_retry(self, token: str, error: str) -> None:
        """Handle retry logic for failed process_file jobs"""
        try:
            # Get job data before deletion
            job_data = self.redis.hgetall(self._get_job_key(token))
            if not job_data:
                return
            
            # Check if this is already a retry job
            is_reupload = job_data.get('is_reupload', 'false').lower() == 'true'
            if is_reupload:
                # This is already a retry job, don't retry again
                print(f"🛑 Job {token} already a retry job, marking as permanently failed")
                self.redis.delete(self._get_job_key(token))
                return
            
            # Get retry configuration
            retry_interval = int(os.getenv('RETRY_INTERVAL_SEC', '43200'))  # 12 hours default
            max_retry_count = int(os.getenv('MAX_RETRY_COUNT', '2'))
            
            # Check existing retry count
            retry_jobs = self.redis.zrange(self._get_retry_key(), 0, -1)
            current_retry_count = 0
            
            for job_json in retry_jobs:
                try:
                    existing_job = json.loads(job_json)
                    if existing_job.get('token') == token:
                        current_retry_count = existing_job.get('retry_count', 0)
                        # Remove existing retry entry
                        self.redis.zrem(self._get_retry_key(), job_json)
                        break
                except json.JSONDecodeError:
                    continue
            
            # Check if we can retry
            if current_retry_count >= max_retry_count:
                print(f"🛑 Job {token} reached max retry count ({max_retry_count}), marking as permanently failed")
                self.redis.delete(self._get_job_key(token))
                return
            
            # Create retry job data
            retry_job_data = {
                'token': token,
                'job_type': 'process_file',
                'file_id': job_data.get('file_id', ''),
                'file_info': job_data.get('file_info', '{}'),
                'retry_count': current_retry_count + 1,
                'error': error,
                'failed_at': datetime.utcnow().isoformat(),
                'next_retry_at': (datetime.utcnow() + timedelta(seconds=retry_interval)).isoformat()
            }
            
            # Add to retry queue with timestamp
            retry_score = time.time() + retry_interval
            retry_payload = json.dumps(retry_job_data)
            self.redis.zadd(self._get_retry_key(), {token: retry_score})
            self.redis.hset(self._get_retry_payload_key(), token, retry_payload)
            
            # Delete the original job hash
            self.redis.delete(self._get_job_key(token))
            
            print(f"🔄 Queued job {token} for retry (attempt {current_retry_count + 1}/{max_retry_count}) in {retry_interval/3600:.1f} hours")
            
        except Exception as e:
            print(f"❌ Error handling retry for job {token}: {e}")
            # Fallback: delete the job hash
            self.redis.delete(self._get_job_key(token))

    def get_queue_stats(self) -> dict:
        """Get queue statistics for monitoring"""
        try:
            stats = {}
            
            job_types = ['process_file', 'new_photo', 'pd_lost', 'auto_upload']
            
            for job_type in job_types:
                stats[job_type] = {
                    'queued': self.redis.zcard(self._get_queue_key(job_type)),
                    'processing': self.redis.scard(self._get_processing_key(job_type))
                }
            
            # Add overall stats
            stats['overall'] = {
                'completed_jobs': self.redis.zcard('completed_jobs'),
                'failed_jobs': self.redis.zcard(self._get_failed_key()),
                'total_active_jobs': sum(stats[jt]['queued'] + stats[jt]['processing'] for jt in job_types)
            }
            
            return stats
        except Exception as e:
            print(f"❌ Error getting queue stats: {e}")
            return {}

    def reset_all_processing_jobs(self) -> int:
        """
        Reset processing jobs back to queue ONLY IF their worker is dead.
        Checks for worker heartbeat key: `worker_heartbeat:<worker_id>`
        """
        try:
            reset_count = 0
            
            job_types = ['process_file', 'new_photo', 'pd_lost', 'auto_upload']
            
            for job_type in job_types:
                processing_tokens = self.redis.smembers(self._get_processing_key(job_type))
                
                for token in processing_tokens:
                    try:
                        # Get job data
                        job_key = self._get_job_key(token)
                        job_data = self.redis.hgetall(job_key)

                        if not job_data:
                            print(f"🧹 Server restart: Removing orphaned processing token {token} ({job_type})")
                            self.redis.srem(self._get_processing_key(job_type), token)
                            continue

                        stored_token = job_data.get('token')
                        if not stored_token:
                            print(f"🛑 Server restart: Corrupted job hash for token {token} ({job_type}) - deleting")
                            self.redis.delete(job_key)
                            self.redis.srem(self._get_processing_key(job_type), token)
                            continue

                        # Check if worker is alive
                        worker_id = job_data.get('worker_id')
                        if worker_id:
                            # Check heartbeat
                            if self.redis.exists(f"worker_heartbeat:{worker_id}"):
                                # Worker is alive, skip reset
                                continue
                            else:
                                print(f"💀 Worker {worker_id} appears dead. Resetting job {token}")
                        else:
                            # Legacy job or no worker ID - check timeout as fallback
                            locked_at_str = job_data.get('locked_at')
                            if locked_at_str:
                                try:
                                    locked_at = datetime.fromisoformat(locked_at_str)
                                    # If locked less than 60 mins ago, assume alive (give benefit of doubt for long downloads)
                                    if (datetime.utcnow() - locked_at).total_seconds() < 1800:
                                        continue
                                except ValueError:
                                    pass
                            
                        # Reset job status to queued
                        self.redis.hset(job_key, 'status', 'queued')
                        self.redis.hset(job_key, 'locked_at', '')
                        self.redis.hset(job_key, 'error', 'Worker died - reset to queued')
                        
                        # Remove from processing set
                        self.redis.srem(self._get_processing_key(job_type), token)
                        
                        # Add back to queue with high priority (server restart recovery)
                        score = time.time() + 1000000  # High priority for recovered jobs
                        self.redis.zadd(self._get_queue_key(job_type), {token: score})
                        
                        reset_count += 1
                        print(f"🔄 Dead worker recovery: Reset processing job {token} ({job_type}) back to queue")
                            
                    except Exception as e:
                        print(f"❌ Error resetting processing job {token}: {e}")
                        # Clean up orphaned token
                        self.redis.srem(self._get_processing_key(job_type), token)
            
            if reset_count > 0:
                print(f"🔄 Server restart recovery: Reset {reset_count} processing jobs from dead workers")
            
            return reset_count
            
        except Exception as e:
            print(f"❌ Error resetting all processing jobs: {e}")
            return 0

    def cleanup_orphaned_jobs(self) -> int:
        """Clean up orphaned processing jobs that have no job hash (due to cleanup)"""
        try:
            cleanup_count = 0
            
            job_types = ['process_file', 'new_photo', 'pd_lost', 'auto_upload']
            
            for job_type in job_types:
                processing_tokens = self.redis.smembers(self._get_processing_key(job_type))
                
                for token in processing_tokens:
                    # Check if job hash exists
                    job_exists = self.redis.exists(self._get_job_key(token))
                    
                    if not job_exists:
                        # Job hash was deleted but token still in processing set
                        # This is now rare due to mark_done/mark_failed order fix
                        print(f"🧹 Cleaning up orphaned processing token: {token} ({job_type})")
                        self.redis.srem(self._get_processing_key(job_type), token)
                        cleanup_count += 1
                    else:
                        # Job exists, validate it has required fields
                        job_data = self.redis.hgetall(self._get_job_key(token))
                        
                        # Skip if job data is empty (might be mid-deletion)
                        if not job_data:
                            print(f"🧹 Skipping empty job hash (likely mid-deletion): {token} ({job_type})")
                            continue
                        
                        # Check basic critical fields (token, type)
                        if not job_data.get('token') or not job_data.get('type'):
                            print(f"🛑 Corrupted job hash detected: Missing token or type for {token} ({job_type}) - deleting")
                            self.redis.delete(self._get_job_key(token))
                            self.redis.srem(self._get_processing_key(job_type), token)
                            cleanup_count += 1
                            continue
                        
                        # Job type specific validation
                        is_corrupted = False
                        if job_type == 'auto_upload':
                            if not job_data.get('service_type') or not job_data.get('link_data'):
                                print(f"🛑 Corrupted auto_upload job: Missing service_type or link_data for {token} - deleting")
                                is_corrupted = True
                        else:
                            if not job_data.get('file_id'):
                                print(f"🛑 Corrupted {job_type} job: Missing file_id for {token} - deleting")
                                is_corrupted = True
                        
                        if is_corrupted:
                            self.redis.delete(self._get_job_key(token))
                            self.redis.srem(self._get_processing_key(job_type), token)
                            cleanup_count += 1
                            continue
                        
                        # Check if it's stuck
                        locked_at = job_data.get('locked_at')
                        if not locked_at:
                            print(f"🧹 Found processing job without timestamp: {token} ({job_type})")
                            
                            # Reset to queued status
                            self.redis.hset(self._get_job_key(token), 'status', 'queued')
                            self.redis.hset(self._get_job_key(token), 'locked_at', '')
                            self.redis.hset(self._get_job_key(token), 'error', 'Orphaned job - reset to queued')
                            
                            # Remove from processing
                            self.redis.srem(self._get_processing_key(job_type), token)
                            
                            # Add back to queue
                            score = time.time() + 5000000  # Default priority
                            self.redis.zadd(self._get_queue_key(job_type), {token: score})
                            
                            cleanup_count += 1
            
            if cleanup_count > 0:
                print(f"🧹 Cleaned up {cleanup_count} orphaned jobs")
            return cleanup_count
            
        except Exception as e:
            print(f"❌ Error cleaning up orphaned jobs: {e}")
            return 0

    def get_emergency_stuck_jobs(self) -> list:
        """Get jobs that are stuck in processing status for over 1 hour (emergency recovery)"""
        try:
            now = datetime.utcnow()
            emergency_timeout = now - timedelta(hours=1)  # 1 hour timeout
            
            emergency_stuck = []
            
            job_types = ['process_file', 'new_photo', 'pd_lost', 'auto_upload']
            
            for job_type in job_types:
                processing_tokens = self.redis.smembers(self._get_processing_key(job_type))
                
                for token in processing_tokens:
                    job_key = self._get_job_key(token)
                    if not self.redis.exists(job_key):
                        print(f"🧹 Emergency cleanup: Removing orphaned processing token {token} ({job_type})")
                        self.redis.srem(self._get_processing_key(job_type), token)
                        continue
                    
                    locked_at_str = self.redis.hget(self._get_job_key(token), 'locked_at')
                    if locked_at_str:
                        try:
                            locked_at = datetime.fromisoformat(locked_at_str)
                            duration = now - locked_at
                            
                            if locked_at <= emergency_timeout:
                                emergency_stuck.append({
                                    'token': token,
                                    'type': job_type,
                                    'locked_at': locked_at_str,
                                    'stuck_duration': str(duration)
                                })
                                
                        except ValueError as e:
                            # Invalid date format, consider it emergency stuck
                            emergency_stuck.append({
                                'token': token,
                                'type': job_type,
                                'locked_at': locked_at_str,
                                'stuck_duration': 'Invalid date format'
                            })
                    else:
                        # Job has no locked_at timestamp - consider it emergency stuck
                        emergency_stuck.append({
                            'token': token,
                            'type': job_type,
                            'locked_at': 'Not set',
                            'stuck_duration': 'No timestamp'
                        })
            
            return emergency_stuck
        except Exception as e:
            print(f"❌ Error getting emergency stuck jobs: {e}")
            return 0

    def force_reset_stuck_jobs(self) -> int:
        """Force reset emergency stuck jobs back to queued status"""
        try:
            emergency_stuck = self.get_emergency_stuck_jobs()
            reset_count = 0
            
            for job in emergency_stuck:
                token = job['token']
                job_type = job['type']
                
                try:
                    job_key = self._get_job_key(token)
                    job_data = self.redis.hgetall(job_key)
                    if not job_data:
                        print(f"🧹 Emergency reset skipped: No job hash for token {token} ({job_type})")
                        self.redis.srem(self._get_processing_key(job_type), token)
                        continue
                    
                    if not job_data.get('token'):
                        print(f"🛑 Emergency reset: Corrupted job hash for token {token} - deleting")
                        self.redis.delete(job_key)
                        self.redis.srem(self._get_processing_key(job_type), token)
                        continue

                    # Force reset job status
                    self.redis.hset(job_key, 'status', 'queued')
                    self.redis.hset(job_key, 'locked_at', '')
                    self.redis.hset(job_key, 'error', f'EMERGENCY: Job stuck for {job.get("stuck_duration", "unknown")} - force reset')
                    
                    # Remove from processing
                    self.redis.srem(self._get_processing_key(job_type), token)
                    
                    # Add back to queue with high priority
                    score = time.time() + 1000000  # High priority for recovered jobs
                    self.redis.zadd(self._get_queue_key(job_type), {token: score})
                    
                    reset_count += 1
                    print(f"🚨 Emergency reset stuck job: {token} ({job_type}) - was stuck for {job.get('stuck_duration', 'unknown')}")
                    
                except Exception as e:
                    print(f"❌ Error emergency resetting stuck job {token}: {e}")
            
            return reset_count
        except Exception as e:
            print(f"❌ Error emergency resetting stuck jobs: {e}")
            return 0

    def get_stuck_jobs(self) -> list:
        """Get jobs that are stuck in processing status beyond timeout"""
        try:
            now = datetime.utcnow()
            # More aggressive timeout: 15 minutes
            lock_timeout = now - timedelta(minutes=15)
            
            stuck_jobs = []
            
            job_types = ['process_file', 'new_photo', 'pd_lost', 'auto_upload']
            
            for job_type in job_types:
                processing_tokens = self.redis.smembers(self._get_processing_key(job_type))
                
                for token in processing_tokens:
                    job_key = self._get_job_key(token)
                    if not self.redis.exists(job_key):
                        print(f"🧹 Cleanup: Removing orphaned processing token {token} ({job_type})")
                        self.redis.srem(self._get_processing_key(job_type), token)
                        continue
                    
                    locked_at_str = self.redis.hget(self._get_job_key(token), 'locked_at')
                    if locked_at_str:
                        try:
                            locked_at = datetime.fromisoformat(locked_at_str)
                            duration = now - locked_at
                            
                            if locked_at <= lock_timeout:
                                stuck_jobs.append({
                                    'token': token,
                                    'type': job_type,
                                    'locked_at': locked_at_str,
                                    'stuck_duration': str(duration)
                                })
                                
                        except ValueError as e:
                            # Invalid date format, consider it stuck
                            stuck_jobs.append({
                                'token': token,
                                'type': job_type,
                                'locked_at': locked_at_str,
                                'stuck_duration': 'Invalid date format'
                            })
                    else:
                        # Job has no locked_at timestamp - consider it stuck
                        stuck_jobs.append({
                            'token': token,
                            'type': job_type,
                            'locked_at': 'Not set',
                            'stuck_duration': 'No timestamp'
                        })
            
            return stuck_jobs
        except Exception as e:
            print(f"❌ Error getting stuck jobs: {e}")
            return []

    def reset_stuck_jobs(self) -> int:
        """Reset stuck jobs back to queued status"""
        try:
            stuck_jobs = self.get_stuck_jobs()
            reset_count = 0
            
            for job in stuck_jobs:
                token = job['token']
                job_type = job['type']
                stuck_duration = job.get('stuck_duration', 'Unknown')
                
                try:
                    job_key = self._get_job_key(token)
                    job_data = self.redis.hgetall(job_key)
                    if not job_data:
                        print(f"🧹 Reset skipped: No job hash for token {token} ({job_type})")
                        self.redis.srem(self._get_processing_key(job_type), token)
                        continue

                    if not job_data.get('token'):
                        print(f"🛑 Reset: Corrupted job hash for token {token} - deleting")
                        self.redis.delete(job_key)
                        self.redis.srem(self._get_processing_key(job_type), token)
                        continue

                    # Reset job status
                    self.redis.hset(job_key, 'status', 'queued')
                    self.redis.hset(job_key, 'locked_at', '')
                    self.redis.hset(job_key, 'error', f'Job timeout after {stuck_duration} - reset to queued')
                    
                    # Remove from processing
                    self.redis.srem(self._get_processing_key(job_type), token)
                    
                    # Add back to queue
                    score = time.time() + 5000000  # Default priority
                    self.redis.zadd(self._get_queue_key(job_type), {token: score})
                    
                    reset_count += 1
                    print(f"🔄 Reset stuck job: {token} ({job_type}) - was stuck for {stuck_duration}")
                    
                except Exception as e:
                    print(f"❌ Error resetting stuck job {token}: {e}")
            
            return reset_count
        except Exception as e:
            print(f"❌ Error resetting stuck jobs: {e}")
            return 0

    def cleanup_expired_jobs(self) -> int:
        """Clean up expired jobs"""
        try:
            cleanup_count = 0
            
            # Get all job keys
            job_keys = self.redis.keys(f"{self.job_prefix}*")
            
            for job_key in job_keys:
                token = job_key.replace(self.job_prefix, '')
                
                # Check TTL
                ttl = self.redis.ttl(job_key)
                if ttl == -1:  # No TTL set
                    # Set TTL for old jobs
                    self.redis.expire(job_key, DEFAULT_JOB_TTL_SEC)
                elif ttl == -2:  # Key doesn't exist
                    continue
                elif ttl == 0:  # Expired
                    # Remove expired job
                    self.redis.delete(job_key)
                    
                    # Remove from all queues
                    job_types = ['process_file', 'new_photo', 'pd_lost', 'auto_upload']
                    for job_type in job_types:
                        self.redis.zrem(self._get_queue_key(job_type), token)
                        self.redis.srem(self._get_processing_key(job_type), token)
                    
                    cleanup_count += 1
                    print(f"🧹 Cleaned up expired job: {token}")
            
            return cleanup_count
        except Exception as e:
            print(f"❌ Error cleaning up expired jobs: {e}")
            return 0

    # Retry System Methods
    def get_retryable_jobs(self) -> list:
        """Get jobs that are ready for retry"""
        try:
            now = datetime.utcnow()
            retry_interval = int(os.getenv('RETRY_INTERVAL_SEC', '43200'))  # 12 hours default
            max_retry_count = int(os.getenv('MAX_RETRY_COUNT', '2'))
            
            ready_jobs = []
            all_retry_jobs = []
            retry_jobs = self.redis.zrange(self._get_retry_key(), 0, -1, withscores=True)
            
            for job_json, score in retry_jobs:
                try:
                    job_data = json.loads(job_json)
                    retry_count = job_data.get('retry_count', 0)
                    
                    # Calculate time until retry
                    next_retry_time = datetime.fromtimestamp(score)
                    time_until_retry = next_retry_time - now
                    time_until_retry_hours = max(0, time_until_retry.total_seconds() / 3600)
                    
                    # Add time calculation to job data
                    job_data['time_until_retry_hours'] = round(time_until_retry_hours, 2)
                    
                    # Check if job is ready for retry
                    if retry_count < max_retry_count and score <= time.time():
                        ready_jobs.append(job_data)
                    
                    # Add all jobs to the list for detailed reporting
                    all_retry_jobs.append(job_data)
                        
                except json.JSONDecodeError:
                    continue
            
            # Return all retry jobs (not just ready ones) for detailed metrics
            return all_retry_jobs
        except Exception as e:
            print(f"❌ Error getting retryable jobs: {e}")
            return []

    def process_retry_jobs(self) -> int:
        """Process jobs that are ready for retry"""
        try:
            now_score = time.time()
            ready_tokens = self.redis.zrangebyscore(self._get_retry_key(), '-inf', now_score)
            if not ready_tokens:
                return 0

            processed_count = 0
            max_retry_count = int(os.getenv('MAX_RETRY_COUNT', '2'))

            for token in ready_tokens:
                payload = self.redis.hget(self._get_retry_payload_key(), token)
                if not payload:
                    print(f"⚠️ Missing retry payload for token {token}, removing from retry queue")
                    self.redis.zrem(self._get_retry_key(), token)
                    continue

                try:
                    job_data = json.loads(payload)
                except json.JSONDecodeError as e:
                    print(f"❌ Error decoding retry job payload for token {token}: {e}")
                    if self.redis.zrem(self._get_retry_key(), token):
                        self.redis.hdel(self._get_retry_payload_key(), token)
                    continue

                job_type = job_data.get('job_type')
                retry_count = job_data.get('retry_count', 0)

                if job_type != 'process_file':
                    print(f"⚠️ Skipping retry job with invalid data: {job_data}")
                    if self.redis.zrem(self._get_retry_key(), token):
                        self.redis.hdel(self._get_retry_payload_key(), token)
                    continue

                if retry_count >= max_retry_count:
                    print(f"🛑 Retry job {token} exceeded max retry count ({max_retry_count}) - removing")
                    if self.redis.zrem(self._get_retry_key(), token):
                        self.redis.hdel(self._get_retry_payload_key(), token)
                    continue

                file_id = job_data.get('file_id', '')
                file_info_str = job_data.get('file_info', '{}')

                try:
                    file_info = json.loads(file_info_str) if file_info_str else None
                except json.JSONDecodeError:
                    file_info = None

                try:
                    if self.redis.zrem(self._get_retry_key(), token):
                        self.redis.hdel(self._get_retry_payload_key(), token)
                        self.enqueue_process(token, file_id, file_info, is_reupload=True)
                        processed_count += 1
                        print(f"🔄 Re-queued retry job: {token} (attempt {retry_count + 1})")
                except Exception as e:
                    print(f"❌ Error processing retry job {token}: {e}")

            return processed_count
        except Exception as e:
            print(f"❌ Error processing retry jobs: {e}")
            return 0

    def get_retry_stats(self) -> dict:
        """Get retry system statistics"""
        try:
            now = datetime.utcnow()
            retry_interval = int(os.getenv('RETRY_INTERVAL_SEC', '43200'))  # 12 hours default
            max_retry_count = int(os.getenv('MAX_RETRY_COUNT', '2'))
            
            retry_jobs = self.redis.zrange(self._get_retry_key(), 0, -1, withscores=True)
            
            waiting_retry = len(retry_jobs)
            ready_for_retry_now = 0
            retry_by_count = {}
            
            for token, score in retry_jobs:
                payload = self.redis.hget(self._get_retry_payload_key(), token)
                if not payload:
                    continue

                try:
                    job_data = json.loads(payload)
                except json.JSONDecodeError:
                    continue

                retry_count = job_data.get('retry_count', 0)

                # Count by retry attempt
                retry_by_count[str(retry_count)] = retry_by_count.get(str(retry_count), 0) + 1

                # Check if ready for retry
                if retry_count < max_retry_count and score <= time.time():
                    ready_for_retry_now += 1
            
            return {
                'enabled': True,
                'waiting_retry': waiting_retry,
                'ready_for_retry_now': ready_for_retry_now,
                'retry_interval_hours': retry_interval / 3600,
                'max_retry_count': max_retry_count,
                'retry_by_count': retry_by_count
            }
        except Exception as e:
            print(f"❌ Error getting retry stats: {e}")
            return {
                'enabled': False,
                'error': str(e)
            }
