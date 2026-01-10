import os
import time
from datetime import datetime, timedelta
from flask import Blueprint, jsonify
from plugins.database import Database
from queueing.redis_queue import RedisJobQueue
from api.common import validate_api_key
from utils.logging_config import get_metrics_logger
from utils.redis_client import get_redis_client
from config import ENABLE_BUZZHEAVIER, BUZZHEAVIER_ACCOUNT_ID

metrics_api = Blueprint('metrics_api', __name__)
db = Database()


@metrics_api.route('/<api_key>/metrics', methods=['GET'])
def get_metrics(api_key):
    """Get comprehensive system metrics."""
    metrics_logger = get_metrics_logger()
    
    try:
        # Validate API key
        if not validate_api_key(api_key):
            metrics_logger.info("Invalid API key used for metrics", extra={
                "api_key": api_key[:8] + "...",
                "endpoint": "metrics",
                "status": "unauthorized"
            })
            return jsonify({
                'success': False,
                'error': 'Invalid or inactive API key'
            }), 401

        # System metrics
        metrics = {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'system': {
                'uptime': 'active',  # Could be enhanced with actual uptime tracking
                'version': 'v2',
                'redis_queue_enabled': True
            },
            'database': {},
            'queue': {},
            'processing': {},
            'storage_providers': {}
        }

        # Database metrics
        try:
            db_ping = db.ping()
            metrics['database'] = {
                'status': 'connected' if db_ping else 'disconnected',
                'collections': {
                    'links': {
                        'total': db.links.count_documents({}),
                        'pending': db.links.count_documents({'status': 'pending'}),
                        'completed': db.links.count_documents({'status': 'completed'}),
                        'failed': db.links.count_documents({'status': 'failed'})
                    },
                    'new_photo_links': {
                        'total': db.new_photo_links.count_documents({}),
                        'pending': db.new_photo_links.count_documents({'status': 'pending'}),
                        'completed': db.new_photo_links.count_documents({'status': 'completed'}),
                        'failed': db.new_photo_links.count_documents({'status': 'failed'})
                    },
                    'pd_lost': {
                        'total': db.pd_lost.count_documents({}),
                        'pending': db.pd_lost.count_documents({'$or': [{'skip': {'$exists': False}}, {'skip': False}]}),
                        'skipped': db.pd_lost.count_documents({'skip': True})
                    }
                }
            }
        except Exception as e:
            metrics['database'] = {'status': 'error', 'error': str(e)}

        # Redis queue metrics
        try:
            from queueing.redis_queue import RedisJobQueue
            rq = RedisJobQueue()
            
            queue_stats = {}
            total_queued = 0
            total_processing = 0
            
            # Get detailed processing information
            processing_details = {}
            
            for job_type in ['process_file', 'new_photo', 'pd_lost', 'auto_upload']:
                queued_count = rq.redis.zcard(rq._get_queue_key(job_type))
                processing_count = rq.redis.scard(rq._get_processing_key(job_type))
                
                # Get detailed info about currently processing jobs
                processing_jobs = []
                if processing_count > 0:
                    processing_keys = rq.redis.smembers(rq._get_processing_key(job_type))
                    for job_key in processing_keys:
                        try:
                            job_data = rq.redis.hgetall(rq._get_job_key(job_key))
                            if job_data:
                                # Calculate processing duration using 'locked_at' field
                                locked_at_str = job_data.get('locked_at')
                                duration = "Unknown"
                                if locked_at_str:
                                    try:
                                        locked_at = datetime.fromisoformat(locked_at_str)
                                        duration_seconds = (datetime.utcnow() - locked_at).total_seconds()
                                        if duration_seconds < 60:
                                            duration = f"{duration_seconds:.0f}s"
                                        elif duration_seconds < 3600:
                                            duration = f"{duration_seconds/60:.1f}m"
                                        else:
                                            duration = f"{duration_seconds/3600:.1f}h"
                                    except:
                                        duration = "Unknown"
                                
                                processing_jobs.append({
                                    'token': job_data.get('token', 'Unknown'),
                                    'file_id': job_data.get('file_id', 'Unknown'),
                                    'duration': duration,
                                    'start_time': locked_at_str,  # Use locked_at as start_time for display
                                    'job_type': job_type
                                })
                        except Exception as e:
                            processing_jobs.append({
                                'error': str(e),
                                'job_type': job_type
                            })
                
                queue_stats[job_type] = {
                    'queued': queued_count,
                    'processing': processing_count,
                    'processing_details': processing_jobs
                }
                
                total_queued += queued_count
                total_processing += processing_count
                
                # Add to overall processing details
                if processing_jobs:
                    processing_details[job_type] = processing_jobs
                
            # Get retry system stats with detailed job information
            retry_stats = rq.get_retry_stats()
            
            # Get detailed retry job information
            retry_jobs = rq.get_retryable_jobs()
            retry_job_details = []
            
            for job in retry_jobs:
                retry_job_details.append({
                    'token': job.get('token', 'Unknown'),
                    'job_type': job.get('job_type', 'Unknown'),
                    'retry_count': job.get('retry_count', 0),
                    'error': job.get('error', 'Unknown error'),
                    'failed_at': job.get('failed_at', 'Unknown'),
                    'next_retry_at': job.get('next_retry_at', 'Unknown'),
                    'time_until_retry_hours': job.get('time_until_retry_hours', 0)
                })
            
            # Add detailed retry jobs to retry stats
            retry_stats['retry_jobs'] = retry_job_details
            
            metrics['queue'] = {
                'enabled': True,
                'type': 'redis',
                'stats': queue_stats,
                'total_queued': total_queued,
                'total_processing': total_processing,
                'processing_details': processing_details,
                'retry_system': retry_stats
            }
            
        except Exception as e:
            metrics['queue'] = {'enabled': True, 'type': 'redis', 'error': str(e)}

        # Processing metrics (last 24 hours)
        try:
            last_24h = datetime.utcnow() - timedelta(hours=24)
            
            # Recent processing statistics
            recent_links = list(db.links.find(
                {'created_time': {'$gte': last_24h}},
                {'status': 1, 'created_time': 1, 'pixeldrain_id': 1, 'buzzheavier_id': 1, 'gphotos_id': 1}
            ))
            
            processing_stats = {
                'last_24h': {
                    'total_processed': len(recent_links),
                    'successful': len([l for l in recent_links if l.get('status') == 'completed']),
                    'failed': len([l for l in recent_links if l.get('status') == 'failed']),
                    'pending': len([l for l in recent_links if l.get('status') == 'pending'])
                }
            }
            
            # Success rates by provider
            completed_links = [l for l in recent_links if l.get('status') == 'completed']
            if completed_links:
                processing_stats['last_24h']['provider_success_rates'] = {
                    'pixeldrain': len([l for l in completed_links if l.get('pixeldrain_id')]) / len(completed_links),
                    'buzzheavier': len([l for l in completed_links if l.get('buzzheavier_id')]) / len(completed_links),
                    'gphotos': len([l for l in completed_links if l.get('gphotos_id')]) / len(completed_links)
                }
            
            metrics['processing'] = processing_stats
            
        except Exception as e:
            metrics['processing'] = {'error': str(e)}

        # Storage provider status
        try:
            metrics['storage_providers'] = {
                'pixeldrain': {
                    'enabled': os.getenv('ENABLE_PIXELDRAIN', 'true').lower() == 'true',
                    'api_keys_configured': len([k for k in os.getenv('PIXELDRAIN_API_KEY', '').split(',') if k.strip()])
                },
                'buzzheavier': {
                    'enabled': ENABLE_BUZZHEAVIER,
                    'api_key_configured': bool(BUZZHEAVIER_ACCOUNT_ID)
                },
                'google_photos': {
                    'enabled': os.getenv('ENABLE_GPHOTOS', 'true').lower() == 'true'
                },
                'new_google_photos': {
                    'enabled': os.getenv('NEW_ENABLE_GPHOTOS', 'true').lower() == 'true',
                    'auth_configured': bool(os.getenv('GP_AUTH_DATA_NEW'))
                }
            }
        except Exception as e:
            metrics['storage_providers'] = {'error': str(e)}

        # Redis metrics
        try:
            redis_client = get_redis_client()
            redis_info = redis_client.get_info()
            
            metrics['redis'] = {
                'status': 'connected' if redis_client.ping() else 'disconnected',
                'version': redis_info.get('redis_version', 'unknown'),
                'used_memory_human': redis_info.get('used_memory_human', 'unknown'),
                'connected_clients': redis_info.get('connected_clients', 0),
                'total_commands_processed': redis_info.get('total_commands_processed', 0),
                'keyspace_hits': redis_info.get('keyspace_hits', 0),
                'keyspace_misses': redis_info.get('keyspace_misses', 0),
                'uptime_in_seconds': redis_info.get('uptime_in_seconds', 0),
                'uptime_in_days': redis_info.get('uptime_in_days', 0)
            }
            
            # Calculate hit rate
            if metrics['redis']['keyspace_hits'] > 0 or metrics['redis']['keyspace_misses'] > 0:
                total_ops = metrics['redis']['keyspace_hits'] + metrics['redis']['keyspace_misses']
                metrics['redis']['hit_rate_percentage'] = round((metrics['redis']['keyspace_hits'] / total_ops) * 100, 2)
            else:
                metrics['redis']['hit_rate_percentage'] = 0
                
        except Exception as e:
            metrics['redis'] = {'status': 'error', 'error': str(e)}

        # Log metrics access (without full data to reduce log size)
        metrics_logger.info("Metrics API accessed", extra={
            "api_key": api_key[:8] + "...",
            "endpoint": "metrics", 
            "status": "success"
        })
        
        return jsonify({
            'success': True,
            'data': metrics
        })

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@metrics_api.route('/<api_key>/health', methods=['GET'])
def enhanced_health_check(api_key):
    """Enhanced health check with external dependencies."""
    try:
        # Basic validation
        if not validate_api_key(api_key):
            return jsonify({
                'success': False,
                'error': 'Invalid or inactive API key'
            }), 401

        health_status = {
            'status': 'healthy',
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'version': 'v2',
            'checks': {}
        }

        # Database health
        try:
            db_ping = db.ping()
            health_status['checks']['database'] = {
                'status': 'pass' if db_ping else 'fail',
                'response_time_ms': 0  # Could add actual timing
            }
        except Exception as e:
            health_status['checks']['database'] = {
                'status': 'fail',
                'error': str(e)
            }
            health_status['status'] = 'degraded'

        # Redis queue health
        try:
            from queueing.redis_queue import RedisJobQueue
            rq = RedisJobQueue()
            
            # Check Redis queue health
            total_queued = 0
            total_processing = 0
            processing_summary = {}
            queue_breakdown = {}
            
            for job_type in ['process_file', 'new_photo', 'pd_lost', 'auto_upload']:
                queued_count = rq.redis.zcard(rq._get_queue_key(job_type))
                processing_count = rq.redis.scard(rq._get_processing_key(job_type))
                
                total_queued += queued_count
                total_processing += processing_count
                
                # Add to queue breakdown
                queue_breakdown[job_type] = {
                    'queued': queued_count,
                    'processing': processing_count,
                    'total': queued_count + processing_count
                }
                
                # Get processing details for health check
                if processing_count > 0:
                    processing_keys = rq.redis.smembers(rq._get_processing_key(job_type))
                    job_details = []
                    
                    for job_key in processing_keys:
                        try:
                            job_data = rq.redis.hgetall(rq._get_job_key(job_key))
                            if job_data:
                                locked_at_str = job_data.get('locked_at')
                                duration = "Unknown"
                                if locked_at_str:
                                    try:
                                        locked_at = datetime.fromisoformat(locked_at_str)
                                        duration_seconds = (datetime.utcnow() - locked_at).total_seconds()
                                        if duration_seconds < 60:
                                            duration = f"{duration_seconds:.0f}s"
                                        elif duration_seconds < 3600:
                                            duration = f"{duration_seconds/60:.1f}m"
                                        else:
                                            duration = f"{duration_seconds/3600:.1f}h"
                                    except:
                                        duration = "Unknown"
                                
                                job_details.append({
                                    'token': job_data.get('token', 'Unknown'),
                                    'duration': duration
                                })
                        except Exception as e:
                            job_details.append({'error': str(e)})
                    
                    processing_summary[job_type] = {
                        'count': processing_count,
                        'jobs': job_details
                    }
            
            # Get retry system stats with detailed job information
            retry_stats = rq.get_retry_stats()
            
            # Get detailed retry job information
            retry_jobs = rq.get_retryable_jobs()
            retry_job_details = []
            
            for job in retry_jobs:
                retry_job_details.append({
                    'token': job.get('token', 'Unknown'),
                    'job_type': job.get('job_type', 'Unknown'),
                    'retry_count': job.get('retry_count', 0),
                    'error': job.get('error', 'Unknown error'),
                    'failed_at': job.get('failed_at', 'Unknown'),
                    'next_retry_at': job.get('next_retry_at', 'Unknown'),
                    'time_until_retry_hours': job.get('time_until_retry_hours', 0)
                })
            
            # Add detailed retry jobs to retry stats
            retry_stats['retry_jobs'] = retry_job_details
            
            health_status['checks']['redis_queue'] = {
                    'status': 'pass',
                'total_queued_jobs': total_queued,
                'total_processing_jobs': total_processing,
                'queue_breakdown': queue_breakdown,
                'processing_summary': processing_summary,
                'retry_system': retry_stats
            }
            
        except Exception as e:
            health_status['checks']['redis_queue'] = {
                'status': 'fail',
                'error': str(e)
            }
            health_status['status'] = 'degraded'

        # Redis health check
        try:
            redis_client = get_redis_client()
            redis_ping = redis_client.ping()
            redis_info = redis_client.get_info()
            
            health_status['checks']['redis'] = {
                'status': 'pass' if redis_ping else 'fail',
                'version': redis_info.get('redis_version', 'unknown'),
                'connected_clients': redis_info.get('connected_clients', 0),
                'used_memory_human': redis_info.get('used_memory_human', 'unknown')
            }
            
            if not redis_ping:
                health_status['status'] = 'degraded'
                
        except Exception as e:
            health_status['checks']['redis'] = {
                'status': 'fail',
                'error': str(e)
            }
            health_status['status'] = 'degraded'

        # External services health (basic config check)
        external_services = {
            'pixeldrain': bool(os.getenv('PIXELDRAIN_API_KEY')),
            'buzzheavier': bool(BUZZHEAVIER_ACCOUNT_ID),
            'google_drive': os.path.exists('private/service.json'),
            'mysql_status': all([
                os.getenv('MYSQL_HOST'),
                os.getenv('MYSQL_USER'),
                os.getenv('MYSQL_PASSWORD')
            ]) if os.getenv('MYSQL_HOST') else True  # Optional service
        }

        health_status['checks']['external_services'] = {}
        for service, configured in external_services.items():
            health_status['checks']['external_services'][service] = {
                'status': 'pass' if configured else 'warn',
                'configured': configured
            }

        return jsonify(health_status)

    except Exception as e:
        return jsonify({
            'status': 'fail',
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'error': str(e)
        }), 500
