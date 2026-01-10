import os
import redis
import json
import time
from datetime import datetime
from typing import Optional, Dict, Any

class RedisClient:
    """Redis client for job queue management"""
    
    def __init__(self):
        # Use Redis URL if available, otherwise fall back to individual parameters
        redis_url = os.getenv('REDIS_URL', 'redis://default:tcPslBEjuOQIbVDgkugcpC3tOFE0N2pY@redis-17574.c17.us-east-1-4.ec2.redns.redis-cloud.com:17574')
        
        if redis_url:
            self.redis = redis.from_url(
                redis_url,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True,
                health_check_interval=30
            )
        else:
            # Fallback to individual parameters
            self.redis = redis.Redis(
                host=os.getenv('REDIS_HOST', 'localhost'),
                port=int(os.getenv('REDIS_PORT', 6379)),
                password=os.getenv('REDIS_PASSWORD'),
                db=int(os.getenv('REDIS_DB', 0)),
                decode_responses=True,
                ssl=True,
                ssl_cert_reqs=None,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True,
                health_check_interval=30
            )
        
        # Test connection (only show message once per process)
        try:
            self.redis.ping()
            # Only show Redis connection message if not already shown
            if not hasattr(RedisClient, '_connection_shown'):
                from rich.console import Console
                from rich.text import Text
                console = Console()
                
                success_text = Text()
                success_text.append("✅ ", style="bold bright_green")
                success_text.append("Redis connection ", style="bold bright_blue")
                success_text.append("successful!", style="bold bright_green")
                console.print(success_text)
                RedisClient._connection_shown = True
        except Exception as e:
            from rich.console import Console
            from rich.text import Text
            console = Console()
            
            error_text = Text()
            error_text.append("❌ ", style="bold bright_red")
            error_text.append("Redis connection ", style="bold bright_blue")
            error_text.append("failed: ", style="bold bright_red")
            error_text.append(str(e), style="bright_red")
            console.print(error_text)
            raise
    
    def ping(self) -> bool:
        """Test Redis connection"""
        try:
            return self.redis.ping()
        except Exception as e:
            print(f"Redis ping failed: {e}")
            return False
    
    def get_info(self) -> Dict[str, Any]:
        """Get Redis server info"""
        try:
            return self.redis.info()
        except Exception as e:
            print(f"Failed to get Redis info: {e}")
            return {}

# Global Redis client instance
redis_client = RedisClient()

def get_redis_client() -> RedisClient:
    """Get Redis client instance"""
    return redis_client
