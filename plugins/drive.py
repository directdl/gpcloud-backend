import os
import json
import humanize
import tempfile
import time
from google.oauth2 import service_account
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError
from io import FileIO
from .token_manager import TokenManager
from .token_cache import TokenCache
from google.oauth2.credentials import Credentials as OAuthCredentials

class DriveFileNotFoundError(Exception):
    pass

class DriveQuotaExceededError(Exception):
    pass

class GoogleDrive:
    def __init__(self):
        self.service = None
        self.token_manager = TokenManager()
        self.token_cache = TokenCache()  # Token cache for service accounts
        self._file_info_cache = {}
        self._cache_size_limit = 100  # Limit cache to 100 entries
        
        # File paths
        self.private_dir = os.path.join(os.path.dirname(__file__), '..', 'private')
        self.service_accounts_file = os.path.join(self.private_dir, 'service.json')
        self.token_pickle_file = os.path.join(self.private_dir, 'token.pickle')
        
        # Service account management
        self.service_accounts = self._load_service_accounts()
        self.current_sa_index = 0
        self.sa_count = 0
        self.max_sa_switches = 3  # Maximum service account switches per download (then token.pickle)
        self.used_sa_indices = set()  # Track which SAs have been used
    
    def _sanitize_filename(self, filename):
        """Sanitize filename for Windows compatibility by replacing invalid characters"""
        # Windows invalid characters: < > : " | ? * \ /
        # Also replace control characters (0-31) and DEL (127)
        invalid_chars = '<>:"|?*\\/' 
        sanitized = filename
        
        for char in invalid_chars:
            sanitized = sanitized.replace(char, '_')
        
        # Replace control characters
        sanitized = ''.join(char if ord(char) >= 32 and ord(char) != 127 else '_' for char in sanitized)
        
        # Remove leading/trailing spaces and dots (Windows doesn't allow these)
        sanitized = sanitized.strip(' .')
        
        # Ensure filename is not empty
        if not sanitized:
            sanitized = 'unnamed_file'
            
        return sanitized
    
    def _load_service_accounts(self):
        """Load service accounts from JSON file with lazy loading and caching"""
        # Use class-level cache to avoid repeated file reads
        if not hasattr(self.__class__, '_service_accounts_cache'):
            try:
                # Use the configured path
                service_file = self.service_accounts_file

                if os.path.exists(service_file):
                    print(f"Loading service accounts from: {service_file}")
                    with open(service_file, 'r') as f:
                        data = json.load(f)
                        # Return list of service accounts for rotation
                        if isinstance(data, list):
                            self.__class__._service_accounts_cache = data
                        elif isinstance(data, dict) and 'type' in data:
                            self.__class__._service_accounts_cache = [data]  # Single service account
                        else:
                            self.__class__._service_accounts_cache = []
                else:
                    print(f"Error: service.json file not found at {service_file}")
                    self.__class__._service_accounts_cache = []

            except Exception as e:
                print(f"Error loading service accounts: {str(e)}")
                self.__class__._service_accounts_cache = []
        
        return self.__class__._service_accounts_cache
            
    def _get_service_account_credentials(self):
        """Get credentials for current service account"""
        if not self.service_accounts:
            raise Exception("No service accounts available")
            
        sa = self.service_accounts[self.current_sa_index]
        credentials = service_account.Credentials.from_service_account_info(
            sa,
            scopes=['https://www.googleapis.com/auth/drive.readonly']
        )
        credentials.refresh(Request())
        return credentials
        
    def _switch_service_account(self):
        """Switch to next random service account (max 3 different SAs)"""
        if self.sa_count >= self.max_sa_switches:
            raise DriveQuotaExceededError(f"Maximum service account switches reached ({self.sa_count}/{self.max_sa_switches})")
        
        # Get available SA indices (not yet used)
        available_indices = [i for i in range(len(self.service_accounts)) if i not in self.used_sa_indices]
        
        if not available_indices:
            raise DriveQuotaExceededError(f"All {len(self.service_accounts)} service accounts already tried")
        
        # Pick random SA from available ones
        import random
        self.current_sa_index = random.choice(available_indices)
        self.used_sa_indices.add(self.current_sa_index)
        self.sa_count += 1
        self.service = None  # Force service recreation
        # SA switched silently
        
    def _get_service(self):
        """Get or create Google Drive service"""
        if self.service is None:
            # Check if we should use token.pickle (final attempt)
            if self.current_sa_index == -1:
                try:
                    import pickle
                    from google.auth.transport.requests import Request
                    
                    if not os.path.exists(self.token_pickle_file):
                        raise Exception("token.pickle file not found")
                    
                    with open(self.token_pickle_file, 'rb') as token:
                        credentials = pickle.load(token)
                    
                    # Refresh if expired
                    if not credentials.valid:
                        if credentials.expired and credentials.refresh_token:
                            credentials.refresh(Request())
                        else:
                            raise Exception("Token.pickle credentials are invalid and cannot be refreshed")
                    
                    self.service = build('drive', 'v3', credentials=credentials, cache_discovery=False)
                    print("Google Drive service created with token.pickle (fallback)")
                    return self.service
                except Exception as e:
                    print(f"Error creating service with token.pickle: {str(e)}")
                    raise Exception(f"Failed to create service with token.pickle: {str(e)}")
            
            # Try rotating across service accounts on init failure
            attempts = 0
            total_accounts = len(self.service_accounts) if self.service_accounts else 0
            last_error = None
            while attempts < max(1, total_accounts):
                try:
                    credentials = self._get_service_account_credentials()
                    # Disable discovery cache to avoid oauth2client cache warnings
                    self.service = build('drive', 'v3', credentials=credentials, cache_discovery=False)
                    print(f"Google Drive service created with service account: {credentials.service_account_email}")
                    return self.service
                except Exception as e:
                    last_error = e
                    idx = (self.current_sa_index % total_accounts) + 1 if total_accounts else 0
                    print(f"Error creating service with service account (index {idx}/{total_accounts}): {str(e)}")
                    # Rotate to next service account if available
                    if total_accounts > 1:
                        try:
                            self._switch_service_account()
                        except Exception as switch_err:
                            print(f"Failed to switch service account: {str(switch_err)}")
                    attempts += 1
                    continue

            # All service accounts failed, try token fallback
            try:
                token = self.token_manager.load_token()
                if token:
                    # Build client with explicit bearer token credentials
                    bearer_credentials = OAuthCredentials(token=token)
                    self.service = build('drive', 'v3', credentials=bearer_credentials, cache_discovery=False)
                    print("Google Drive service created with token.pickle (fallback)")
                else:
                    raise Exception("No token available")
            except Exception as token_error:
                print(f"Token fallback also failed: {str(token_error)}")
                raise Exception(f"Google Drive authentication failed. Please check service.json file. Last error: {str(last_error)}")
        return self.service
    
    def _cleanup_cache(self):
        """Cleanup file info cache to prevent memory leaks"""
        if len(self._file_info_cache) > self._cache_size_limit:
            # Remove oldest entries
            items_to_remove = len(self._file_info_cache) - self._cache_size_limit
            oldest_keys = list(self._file_info_cache.keys())[:items_to_remove]
            for key in oldest_keys:
                del self._file_info_cache[key]
            print(f"Cleaned up {items_to_remove} cache entries")
    
    def get_file_info(self, file_id, token=None):
        """Get file information from database first, then API fallback"""
        # Check cache first (fastest path)
        if file_id in self._file_info_cache:
            return self._file_info_cache[file_id]
            
        # NEW: Try database first if token provided
        if token:
            try:
                from plugins.db_utils import DBUtils
                db_utils = DBUtils()
                
                # Get file info from database
                db_file_info = db_utils.get_file_info_by_token(token)
                
                if db_file_info and db_file_info.get('filename'):
                    file_info = {
                        'filename': db_file_info['filename'],
                        'filesize': db_file_info.get('filesize', 'Unknown'),
                        'raw_size': db_file_info.get('raw_size', 0),
                        'file_id': file_id,
                        'filetype': db_file_info.get('filetype', 'application/octet-stream')
                    }
                    # Add to cache
                    self._file_info_cache[file_id] = file_info
                    print(f"File info retrieved from database: {file_info['filename']}")
                    return file_info
            except Exception as e:
                print(f"Database file info lookup failed: {str(e)}")
        
        # Fallback 1: Use fast FileInfo API
        fileinfo_api_key = os.getenv('FILEINFO_API')
        
        if fileinfo_api_key:
            try:
                # Use fast FileInfo API
                return self._get_file_info_via_api(file_id, fileinfo_api_key)
            except Exception as e:
                print(f"FileInfo API failed, falling back to service account: {str(e)}")
                # Fallback to service account method
        
        # Original service account method
        max_retries = 2  # Reduced retries for faster response
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                service = self._get_service()

                # Get file metadata with shared drive support (optimized)
                try:
                    file = service.files().get(
                        fileId=file_id,
                        fields='name,size,id,mimeType',
                        supportsAllDrives=True
                    ).execute()
                except TypeError as e:
                    # Fallback for older API versions
                    if 'includeItemsFromAllDrives' in str(e):
                        file = service.files().get(
                            fileId=file_id,
                            fields='name,size,id,mimeType'
                        ).execute()
                    else:
                        raise e

                # Safe field extraction with fallbacks
                filename = file.get('name', f'service_account_download_{file_id}.bin')
                size = file.get('size', '0')
                try:
                    raw_size = int(size) if size else 0
                    filesize = humanize.naturalsize(raw_size) if raw_size > 0 else 'Unknown'
                except (ValueError, TypeError):
                    raw_size = 0
                    filesize = 'Unknown'
                
                file_info = {
                    'filename': filename,
                    'filesize': filesize,
                    'raw_size': raw_size,
                    'file_id': file.get('id', file_id),
                    'filetype': file.get('mimeType', 'application/octet-stream')
                }

                # Add to cache and cleanup if needed
                self._file_info_cache[file_id] = file_info
                self._cleanup_cache()

                return file_info

            except HttpError as err:
                                        # Enhanced error handling with proper filename fallback
                if err.resp.status == 404:
                    raise DriveFileNotFoundError('File not found or no access')
                elif err.resp.status == 403:
                    error_reason = err.error_details[0]['reason'] if hasattr(err, 'error_details') else 'unknown'
                    if error_reason in ['dailyLimitExceeded', 'downloadQuotaExceeded', 'userRateLimitExceeded']:
                        # Try switching service account
                        self._switch_service_account()
                        retry_count += 1
                        continue
                    else:
                        raise Exception(f'Access denied: {error_reason}')
                elif err.resp.status in [500, 502, 503, 504, 429] and retry_count < max_retries - 1:
                    # Retry on server errors
                    retry_count += 1
                    time.sleep(2 ** retry_count)  # Exponential backoff
                    continue
                else:
                    raise Exception(f'Failed to get file info: {err}')
            except Exception as e:
                if retry_count < max_retries - 1:
                    retry_count += 1
                    time.sleep(2 ** retry_count)  # Exponential backoff
                    continue
                else:
                    raise Exception(f"Request error: {str(e)}")
    
        raise Exception("Maximum retries exceeded")
    
    def _get_file_info_via_api(self, file_id, api_key):
        """Get file info using fast FileInfo API with safe error handling"""
        import requests
        
        try:
            # Fast API endpoint for file info
            url = f"https://www.googleapis.com/drive/v3/files/{file_id}"
            params = {
                'key': api_key,
                'fields': 'name,size,id,mimeType',
                'supportsAllDrives': 'true'
            }
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            file_data = response.json()
            
            # Validate response has required fields
            if not file_data or not isinstance(file_data, dict):
                raise Exception("Invalid API response: empty or malformed data")
            
            if 'name' not in file_data:
                raise Exception("Invalid API response: missing 'name' field")
            
            # Safe field extraction with fallbacks
            filename = file_data.get('name', f'api_download_{file_id}.bin')
            size = file_data.get('size', '0')
            try:
                raw_size = int(size) if size else 0
                filesize = humanize.naturalsize(raw_size) if raw_size > 0 else 'Unknown'
            except (ValueError, TypeError):
                raw_size = 0
                filesize = 'Unknown'
            
            file_info = {
                'filename': filename,
                'filesize': filesize,
                'raw_size': raw_size,
                'file_id': file_data.get('id', file_id),
                'filetype': file_data.get('mimeType', 'application/octet-stream')
            }
            
            # Add to cache
            self._file_info_cache[file_id] = file_info
            self._cleanup_cache()
            
            print(f"File info retrieved via API: {file_info['filename']}")
            return file_info
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"API request failed: {str(e)}")
        except ValueError as e:
            raise Exception(f"API response parsing failed: {str(e)}")
        except Exception as e:
            raise Exception(f"FileInfo API error: {str(e)}")
    
    def download_file(self, file_id, token, custom_dir=None, file_info=None, job_type=None):
        """Download a file from Google Drive using service accounts with worker fallback
        Args:
            file_id: The Google Drive file ID
            token: The unique token for this download
            custom_dir: Optional custom directory path (e.g. 'buzzheavier' for BuzzHeavier downloads)
            file_info: Optional pre-fetched file metadata to avoid refetching
            job_type: Optional job type to determine download strategy
        """
        from utils.logging_config import track_performance
        import logging
        
        logger = logging.getLogger(__name__)
        logger.info(f"Starting download", extra={"file_id": file_id, "token": token, "custom_dir": custom_dir, "job_type": job_type})
        
        # Try service account method first
        try:
            print("🔧 Attempting service account download...")
            return self._download_with_service_accounts(file_id, token, custom_dir, file_info)
        except Exception as sa_error:
            print(f"❌ Service account download failed: {str(sa_error)}")
            
            # Try worker-based direct download as second fallback
            try:
                print("🌐 Falling back to worker-based download...")
                return self._download_with_workers(file_id, token, custom_dir, file_info)
            except Exception as worker_error:
                print(f"❌ Worker download also failed: {str(worker_error)}")
                
                # Try API-based downloader as final fallback (ONLY for process_file jobs)
                if job_type == 'process_file':
                    try:
                        print("🌐 Falling back to API-based download (process_file only)...")
                        return self._download_with_api(file_id, token, custom_dir, file_info)
                    except Exception as api_error:
                        print(f"❌ API download also failed: {str(api_error)}")
                        # All methods failed - create clean error message without nested exceptions
                        error_message = f"All download methods failed. Service account: {str(sa_error)}, Worker: {str(worker_error)}, API: {str(api_error)}"
                        # Create a clean exception without traceback nesting
                        final_error = Exception(error_message)
                        final_error.__cause__ = None  # Prevent traceback chaining
                        raise final_error
                else:
                    # For non-process_file jobs, don't try API downloader
                    print(f"⚠️ Skipping API downloader for job type: {job_type}")
                    # Create clean error message without nested exceptions
                    error_message = f"All download methods failed. Service account: {str(sa_error)}, Worker: {str(worker_error)}"
                    # Create a clean exception without traceback nesting
                    final_error = Exception(error_message)
                    final_error.__cause__ = None  # Prevent traceback chaining
                    raise final_error
    
    def _download_with_service_accounts(self, file_id, token, custom_dir=None, file_info=None):
        """Download using service accounts with aria2c"""
        max_retries = 4  # 3 SA attempts + 1 token.pickle attempt
        retry_count = 0
        
        while retry_count < max_retries:
            # Check if this is the final attempt (token.pickle only)
            if retry_count == max_retries - 1:
                # Final token.pickle attempt
                self.service = None  # Force service recreation with token.pickle
                self.current_sa_index = -1  # Indicate token.pickle usage
            else:
                # Reset service account state for each major retry (but keep global used tracking)
                self.sa_count = 0
                self.service = None
                
                # Pick random initial SA (avoid previously used ones across all attempts)
                import random
                available_indices = [i for i in range(len(self.service_accounts)) if i not in self.used_sa_indices]
                
                # If all SAs used, reset and start fresh
                if not available_indices:
                    self.used_sa_indices.clear()
                    available_indices = list(range(len(self.service_accounts)))
                
                self.current_sa_index = random.choice(available_indices)
                self.used_sa_indices.add(self.current_sa_index)
                # Clean startup log
            
            try:
                # Create downloads directory if it doesn't exist
                if custom_dir:
                    downloads_dir = os.path.join(os.getcwd(), 'downloads', custom_dir, token)
                else:
                    downloads_dir = os.path.join(os.getcwd(), 'downloads', token)
                os.makedirs(downloads_dir, exist_ok=True)

                # Use provided file_info if available, otherwise fetch (will use cache if available)
                if file_info is None:
                    file_info = self.get_file_info(file_id, token)

                # Get filename with multiple fallbacks
                filename = file_info.get('filename') if file_info else None
                if not filename or filename.strip() == '':
                    filename = f'service_account_download_{token}.bin'
                    print(f"Using fallback filename: {filename}")

                # Sanitize filename for Windows compatibility
                sanitized_filename = self._sanitize_filename(filename)
                local_path = os.path.join(downloads_dir, sanitized_filename)
                
                # Get access token from service account (with caching)
                print("🔑 Getting access token from service account...")
                sa = self.service_accounts[self.current_sa_index]
                access_token = self.token_cache.get_token(
                    sa_info=sa,
                    scopes=['https://www.googleapis.com/auth/drive.readonly']
                )
                print(f"✅ Got access token: {access_token[:20]}...")
                
                # Build Google Drive download URL
                download_url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media&supportsAllDrives=true"
                print(f"📥 Download URL: {download_url}")
                
                # Use aria2c with Authorization header for download
                from plugins.aria2c_config import Aria2cDownloader
                downloader = Aria2cDownloader(timeout=1800)
                
                print(f"🚀 Starting aria2c download with service account token...")
                downloaded_path = downloader.download_with_auth_header(
                    url=download_url,
                    token=access_token,
                    filename=sanitized_filename,
                    download_token=token,  # Use the original download token for directory naming
                    custom_dir=custom_dir  # Pass custom directory if provided
                )
                
                if not downloaded_path:
                    raise Exception("aria2c download failed - no file returned")
                
                # Verify download
                if os.path.exists(downloaded_path):
                    actual_size = os.path.getsize(downloaded_path)
                    if actual_size > 0:
                        print(f"✅ Service account download successful: {actual_size / (1024*1024):.1f} MB")
                        # Cleanup cache after successful download
                        self._cleanup_cache()
                        return downloaded_path, filename, f'{actual_size / (1024*1024):.1f} MB'
                    else:
                        raise Exception("Downloaded file has 0 bytes")
                else:
                    raise Exception("Downloaded file not found")
            
            except Exception as e:
                # Cleanup on errors - remove partial download if exists
                if os.path.exists(local_path):
                    try:
                        os.remove(local_path)
                        print(f"Cleaned up partial download: {local_path}")
                    except:
                        pass
                
                # Check if quota exceeded - try switching service account
                error_str = str(e).lower()
                if "quota" in error_str or "limit" in error_str or "403" in error_str:
                    if self.current_sa_index != -1 and self.sa_count < self.max_sa_switches:
                        try:
                            print(f"⚠️ Quota/limit error detected, switching service account...")
                            self._switch_service_account()
                            continue  # Retry with new SA without incrementing retry_count
                        except DriveQuotaExceededError:
                            print("All service accounts exhausted, will try token.pickle in next retry")
                
                # Retry logic
                if retry_count < max_retries - 1:
                    retry_count += 1
                    print(f"Retrying download attempt {retry_count + 1}/{max_retries} after {2 ** retry_count}s...")
                    time.sleep(2 ** retry_count)  # Exponential backoff
                    continue
                else:
                    retry_error = Exception(f"Download failed after {max_retries} attempts: {str(e)}")
                    retry_error.__cause__ = None  # Prevent traceback chaining
                    raise retry_error
        
        raise Exception("Maximum retries exceeded")
    
    def _download_with_workers(self, file_id, token, custom_dir=None, file_info=None):
        """Download using worker URLs with aria2c (16 connections)"""
        import os
        
        # Get worker URLs from environment
        worker_urls = os.getenv('WORKER_URL', '').strip()
        if not worker_urls:
            raise Exception("No worker URLs configured in WORKER_URL environment variable")
        
        # Parse multiple URLs (comma-separated)
        urls = [url.strip() for url in worker_urls.split(',') if url.strip()]
        if not urls:
            raise Exception("No valid worker URLs found")
        
        print(f"Attempting worker-based download with {len(urls)} worker(s) using aria2c")
        
        # Get filename from database using token
        filename = None
        try:
            from plugins.db_utils import DBUtils
            db_utils = DBUtils()
            filename = db_utils.get_filename_by_token(token)
            if filename:
                print(f"Using filename from database: {filename}")
        except Exception as e:
            print(f"Could not get filename from database: {e}")
        
        # Fallback filename if database lookup fails
        if not filename:
            filename = f'worker_download_{token}.bin'
            print(f"Using fallback filename: {filename}")
        
        # Sanitize filename for Windows compatibility
        sanitized_filename = self._sanitize_filename(filename)
        
        # Use aria2c for download with 16 connections
        from plugins.aria2c_config import Aria2cDownloader
        import random
        downloader = Aria2cDownloader(timeout=1800)
        
        # Retry delays: 8 seconds for first retry, 24 seconds for second retry
        retry_delays = [8, 24]
        max_retries = 2  # Total 3 attempts: 1 initial + 2 retries
        
        # Helper function to build download URL from base URL
        def build_download_url(base_url):
            if base_url.endswith('/'):
                base_url = base_url.rstrip('/')
            
            if 'download.aspx' in base_url or '/download' in base_url:
                return f"{base_url}?id={file_id}"
            else:
                return f"{base_url}/download.aspx?id={file_id}"
        
        # Try with random worker URLs - total 3 attempts (1 initial + 2 retries)
        for attempt in range(max_retries + 1):  # 0 = initial, 1 = first retry, 2 = second retry
            try:
                # Pick random worker URL for this attempt
                random_base_url = random.choice(urls)
                download_url = build_download_url(random_base_url)
                
                if attempt == 0:
                    print(f"Worker attempt {attempt + 1}/{max_retries + 1}: Trying random worker {download_url} with aria2c (16 connections)")
                else:
                    print(f"Worker retry attempt {attempt}/{max_retries}: Trying random worker {download_url} after {retry_delays[attempt-1]}s delay")
                
                # Use aria2c multi-connection download
                downloaded_path = downloader.download(
                    url=download_url,
                    token=token,
                    filename=sanitized_filename
                )
                
                if downloaded_path and os.path.exists(downloaded_path):
                    print(f"✅ Worker download successful: {sanitized_filename}")
                    return downloaded_path, filename, 'Unknown'
                else:
                    raise Exception("Download failed - no file returned")
                    
            except Exception as e:
                if attempt < max_retries:
                    # Wait before retry
                    delay = retry_delays[attempt]
                    print(f"❌ Worker attempt {attempt + 1} failed: {str(e)}. Retrying with random worker in {delay}s...")
                    time.sleep(delay)
                    continue
                else:
                    # All retries exhausted
                    print(f"❌ Worker failed after {max_retries + 1} attempts: {str(e)}")
                    break
        
        # All attempts failed
        worker_error = Exception(f"Worker download failed after {max_retries + 1} attempts with random worker URLs")
        worker_error.__cause__ = None  # Prevent traceback chaining
        raise worker_error
    
    def _download_with_api(self, file_id, token, custom_dir=None, file_info=None):
        """Download using API-based downloader as final fallback"""
        import requests
        import os
        
        # Get API configuration from environment
        api_key = os.getenv('API_DOWNLOADER_KEY', 'hridoygcloudsjruh748ufdbjdsbf123456')
        api_domain = os.getenv('API_DOWNLOADER_DOMAIN', 'https://share-instant-db.vercel.app')
        
        if not api_key or not api_domain:
            raise Exception("API downloader not configured - missing API_DOWNLOADER_KEY or API_DOWNLOADER_DOMAIN")
        
        print(f"🌐 Attempting API-based download: {file_id}")
        
        # Get filename from database using token
        filename = None
        try:
            from plugins.db_utils import DBUtils
            db_utils = DBUtils()
            filename = db_utils.get_filename_by_token(token)
            if filename:
                print(f"Using filename from database: {filename}")
        except Exception as e:
            print(f"Could not get filename from database: {e}")
        
        # Fallback filename if database lookup fails
        if not filename:
            filename = f'api_download_{token}.bin'
            print(f"Using fallback filename: {filename}")
        
        # Create downloads directory
        if custom_dir:
            downloads_dir = os.path.join(os.getcwd(), 'downloads', custom_dir, token)
        else:
            downloads_dir = os.path.join(os.getcwd(), 'downloads', token)
        os.makedirs(downloads_dir, exist_ok=True)
        
        # Sanitize filename
        sanitized_filename = self._sanitize_filename(filename)
        local_path = os.path.join(downloads_dir, sanitized_filename)
        
        try:
            # Build API URL
            api_url = f"{api_domain}/{api_key}/{file_id}"
            print(f"🔗 API URL: {api_url}")
            
            # Get download URL from API
            response = requests.get(api_url, timeout=90)
            response.raise_for_status()
            
            # Parse API response
            data = response.json()
            if not data or 'download_url' not in data:
                raise Exception("API response missing download_url")
            
            download_url = data['download_url']
            print(f"📥 Got download URL from API: {download_url[:100]}...")
            
            # Use aria2c single connection download for API
            from plugins.aria2c_config import Aria2cDownloader
            downloader = Aria2cDownloader(timeout=1800)
            
            print(f"🚀 Starting aria2c single connection download from API...")
            downloaded_path = downloader.download_single_connection(
                url=download_url,
                token=token,
                filename=sanitized_filename
            )
            
            if downloaded_path and os.path.exists(downloaded_path):
                print(f"✅ API download successful: {sanitized_filename}")
                return downloaded_path, filename, 'Unknown'
            else:
                raise Exception("aria2c single connection download failed")
                
        except requests.exceptions.RequestException as e:
            api_error = Exception(f"API download failed: {str(e)}")
            api_error.__cause__ = None  # Prevent traceback chaining
            raise api_error
        except Exception as e:
            if os.path.exists(local_path):
                os.remove(local_path)
            api_error = Exception(f"API download error: {str(e)}")
            api_error.__cause__ = None  # Prevent traceback chaining
            raise api_error
            
    def _download_as_export(self, file_id, token, custom_dir, file_info):
        """Download Google Docs as PDF export"""
        try:
            # Create downloads directory
            if custom_dir:
                downloads_dir = os.path.join(os.getcwd(), 'downloads', custom_dir, token)
            else:
                downloads_dir = os.path.join(os.getcwd(), 'downloads', token)
            os.makedirs(downloads_dir, exist_ok=True)
            
            # Sanitize filename and add PDF extension
            sanitized_filename = self._sanitize_filename(file_info['filename'])
            if not sanitized_filename.endswith('.pdf'):
                sanitized_filename = f"{sanitized_filename}.pdf"
            local_path = os.path.join(downloads_dir, sanitized_filename)
            
            # Export as PDF
            service = self._get_service()
            request = service.files().export_media(
                fileId=file_id, 
                mimeType="application/pdf"
            )
            
            fh = FileIO(local_path, "wb")
            downloader = MediaIoBaseDownload(fh, request, chunksize=100 * 1024 * 1024)
            
            done = False
            while not done:
                status, done = downloader.next_chunk()
                
            fh.close()
            
            return local_path, file_info['filename'], "PDF Export"
            
        except Exception as e:
            if 'fh' in locals():
                fh.close()
            if 'local_path' in locals() and os.path.exists(local_path):
                os.remove(local_path)
            export_error = Exception(f"Export failed: {str(e)}")
            export_error.__cause__ = None  # Prevent traceback chaining
            raise export_error
    
    def extract_file_id(self, drive_link):
        """Extract file ID from Google Drive link"""
        if 'drive.google.com' not in drive_link:
            return None
            
        file_id = None
        
        try:
            if '/file/d/' in drive_link:
                # Format: https://drive.google.com/file/d/{fileId}/view
                file_id = drive_link.split('/file/d/')[1].split('/')[0]
            elif 'id=' in drive_link:
                # Format: https://drive.google.com/open?id={fileId}
                file_id = drive_link.split('id=')[1].split('&')[0]
            
            return file_id
            
        except Exception:
            return None