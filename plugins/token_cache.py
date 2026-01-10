"""
Token cache manager for service account access tokens
Handles caching, expiry checking, and automatic refresh
"""

import time
from typing import Optional, Dict, Any
from google.oauth2 import service_account
from google.auth.transport.requests import Request


class TokenCache:
    """Cache manager for service account access tokens"""
    
    # Class-level cache shared across all instances
    _shared_cache = {}  # {sa_email: {'token': str, 'expiry': timestamp, 'credentials': obj}}
    
    def __init__(self):
        """Initialize token cache"""
        self._cache = TokenCache._shared_cache  # Use shared cache
        self._buffer_seconds = 60  # Refresh token 60 seconds before expiry
    
    def get_token(self, sa_info: Dict[str, Any], scopes: list) -> str:
        """
        Get access token for service account (from cache or fresh)
        
        Args:
            sa_info: Service account JSON info
            scopes: OAuth scopes list
            
        Returns:
            Access token string
        """
        sa_email = sa_info.get('client_email', 'unknown')
        current_time = time.time()
        
        # Check if we have a valid cached token
        if sa_email in self._cache:
            cached_data = self._cache[sa_email]
            
            # Check if token is still valid (with buffer)
            if cached_data['expiry'] > current_time + self._buffer_seconds:
                remaining_time = int(cached_data['expiry'] - current_time)
                print(f"[CACHE] Using cached token for {sa_email[:30]}... (valid for {remaining_time}s)")
                return cached_data['token']
            else:
                # Token expired or about to expire, try to refresh
                print(f"[CACHE] Token expired/expiring for {sa_email[:30]}..., refreshing...")
                try:
                    credentials = cached_data['credentials']
                    credentials.refresh(Request())
                    
                    # Update cache with refreshed token
                    if credentials.token and credentials.expiry:
                        expiry_timestamp = credentials.expiry.timestamp()
                        self._cache[sa_email] = {
                            'token': credentials.token,
                            'expiry': expiry_timestamp,
                            'credentials': credentials
                        }
                        remaining_time = int(expiry_timestamp - current_time)
                        print(f"[CACHE] Token refreshed for {sa_email[:30]}... (valid for {remaining_time}s)")
                        return credentials.token
                except Exception as e:
                    print(f"[CACHE] Failed to refresh token: {str(e)}, getting new token...")
                    # If refresh fails, get new token (fallthrough to below)
        
        # No valid cache, get new token
        print(f"[CACHE] Getting new token for {sa_email[:30]}...")
        credentials = service_account.Credentials.from_service_account_info(
            sa_info,
            scopes=scopes
        )
        credentials.refresh(Request())
        
        # Cache the token
        if credentials.token and credentials.expiry:
            expiry_timestamp = credentials.expiry.timestamp()
            self._cache[sa_email] = {
                'token': credentials.token,
                'expiry': expiry_timestamp,
                'credentials': credentials
            }
            cache_duration = int(expiry_timestamp - current_time)
            print(f"[CACHE] Cached new token for {sa_email[:30]}... (valid for {cache_duration}s)")
            return credentials.token
        
        # Fallback (shouldn't reach here)
        return credentials.token if credentials.token else ""
    
    def clear_cache(self, sa_email: Optional[str] = None):
        """
        Clear cache for specific service account or all
        
        Args:
            sa_email: Service account email to clear (None = clear all)
        """
        if sa_email:
            if sa_email in self._cache:
                del self._cache[sa_email]
                print(f"[CACHE] Cleared cache for {sa_email[:30]}...")
        else:
            self._cache.clear()
            print(f"[CACHE] Cleared all cached tokens")
    
    def get_cache_info(self) -> Dict[str, Any]:
        """
        Get information about current cache state
        
        Returns:
            Dictionary with cache statistics
        """
        current_time = time.time()
        info = {
            'total_cached': len(self._cache),
            'accounts': []
        }
        
        for sa_email, data in self._cache.items():
            remaining = int(data['expiry'] - current_time)
            info['accounts'].append({
                'email': sa_email,
                'expires_in': remaining,
                'is_valid': remaining > self._buffer_seconds
            })
        
        return info

