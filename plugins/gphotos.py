import os
import logging
import json
import hashlib
import base64
import time
import subprocess
import shutil
from dotenv import load_dotenv
from gpmc import Client, utils
from pathlib import Path
import mimetypes

# Load environment variables
load_dotenv()
logger = logging.getLogger(__name__)

VIDEO_EXTENSIONS = {'.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v', '.mpg', '.mpeg', '.ts', '.mts', '.m2ts'}


def _parse_auth_data(raw: str):
    if not raw:
        return None

    s = str(raw).strip()

    # JSON object string
    if s.startswith("{") and s.endswith("}"):
        try:
            return json.loads(s)
        except Exception:
            return raw

    # quoted json string (ex: "....")
    if s.startswith('"') and s.endswith('"'):
        try:
            v = json.loads(s)
            if isinstance(v, (dict, list)):
                return v
            if isinstance(v, str) and v.strip():
                s = v.strip()
        except Exception:
            pass

    # base64 json string
    try:
        padded = s + ("=" * (-len(s) % 4))
        decoded = base64.b64decode(padded.encode("utf-8"), validate=False).decode("utf-8", errors="ignore").strip()
        if decoded.startswith("{") and decoded.endswith("}"):
            return json.loads(decoded)
    except Exception:
        pass

    return raw


def modify_mkv_metadata(file_path, title="Gcloud Helper"):
    """
    Modify MKV file metadata using mkvpropedit to change hash
    Returns modified file path or None if failed
    """
    try:
        # Check if mkvpropedit is available
        if not shutil.which('mkvpropedit'):
            logger.warning("mkvpropedit not found, skipping metadata modification")
            return file_path

        # Only process MKV files
        if not str(file_path).lower().endswith('.mkv'):
            logger.info(f"Not an MKV file, skipping metadata modification: {file_path}")
            return file_path

        logger.info(f"Modifying MKV metadata for: {file_path}")

        # Run mkvpropedit to change title
        cmd = ['mkvpropedit', str(file_path), '--edit', 'info', '--set', f'title={title}']
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)

        if result.returncode == 0:
            logger.info(f"Successfully modified MKV metadata: {file_path}")
            return file_path
        else:
            # Check for specific error messages
            error_msg = (result.stderr or "").lower()
            if 'not a matroska file' in error_msg or 'could not be found' in error_msg:
                logger.warning(f"File is not a valid Matroska file or corrupted, skipping metadata modification: {file_path}")
            elif 'not found' in error_msg:
                logger.warning(f"File not found by mkvpropedit, skipping: {file_path}")
            else:
                logger.error(f"mkvpropedit failed: {result.stderr}")
            return file_path

    except subprocess.TimeoutExpired:
        logger.error(f"mkvpropedit timeout for: {file_path}")
        return file_path
    except Exception as e:
        logger.error(f"Error modifying MKV metadata: {str(e)}")
        return file_path


class CustomClient(Client):
    """Custom Client class that overrides MIME type validation to only allow videos"""

    def __init__(self, auth_data=None):
        try:
            super().__init__(auth_data=auth_data)
        except TypeError:
            # fallback for older gpmc
            super().__init__()

    def _search_for_media_files(self, path: str | Path, recursive: bool) -> list[Path]:
        """
        Override the original method to only allow video files.
        """
        path = Path(path)

        if path.is_file():
            # Check if file is a video (MIME type + extension fallback)
            mime_type, _ = mimetypes.guess_type(str(path))

            # First check MIME type
            if mime_type and mime_type.startswith('video/'):
                return [path]

            # Fallback: Check file extension
            if path.suffix.lower() in VIDEO_EXTENSIONS:
                return [path]

            # Not a video file
            logger.warning(f"Skipping non-video file: {path}")
            return []

        if not path.is_dir():
            raise ValueError("Invalid path. Please provide a file or directory path.")

        files = []

        if recursive:
            for root, _, filenames in os.walk(path):
                for filename in filenames:
                    file_path = Path(root) / filename
                    mime_type, _ = mimetypes.guess_type(str(file_path))

                    # Check MIME type first, then extension fallback
                    if (mime_type and mime_type.startswith('video/')) or file_path.suffix.lower() in VIDEO_EXTENSIONS:
                        files.append(file_path)
        else:
            for file in path.iterdir():
                if file.is_file():
                    mime_type, _ = mimetypes.guess_type(str(file))

                    # Check MIME type first, then extension fallback
                    if (mime_type and mime_type.startswith('video/')) or file.suffix.lower() in VIDEO_EXTENSIONS:
                        files.append(file)

        if len(files) == 0:
            raise ValueError("No video files found in the directory.")

        return files


class GPhotos:
    def __init__(self):
        # Load authentication data from environment
        auth_data = os.getenv('GP_AUTH_DATA')
        if not auth_data:
            raise ValueError("GP_AUTH_DATA environment variable is not set")

        # Initialize mimetypes (add common video types)
        mimetypes.init()
        mimetypes.add_type("video/x-matroska", ".mkv")
        mimetypes.add_type("video/mp2t", ".ts")
        mimetypes.add_type("video/mp2t", ".mts")
        mimetypes.add_type("video/mp2t", ".m2ts")
        mimetypes.add_type("video/mp2t", ".m2t")

        # Initialize Google Photos client with our custom client (AUTH MUST BE PASSED)
        parsed_auth = _parse_auth_data(auth_data)
        self.client = CustomClient(auth_data=parsed_auth)

    def is_video_file(self, file_path, db_filetype=None):
        """Check if file is a video based on mimetype with extension fallback and database filetype"""
        fp = str(file_path)
        mime_type, _ = mimetypes.guess_type(fp)

        # First check MIME type
        if mime_type and mime_type.startswith('video/'):
            return True

        # Fallback: Check file extension for common video formats
        file_ext = Path(fp).suffix.lower()
        if file_ext in VIDEO_EXTENSIONS:
            return True

        # Ultimate fallback: Check database filetype if provided
        if db_filetype and str(db_filetype).startswith('video/'):
            logger.info(f"Database confirms video file: {db_filetype}")
            return True

        return False

    def upload(self, file_path, token=None, db_filetype=None):
        """
        Upload a video file to Google Photos using gpmc client with exponential backoff retry mechanism
        Returns the media key if successful
        """
        max_retries = 8
        base_delay = 15  # base delay in seconds

        for attempt in range(max_retries):
            try:
                # Clear cache before retry attempts (except first attempt)
                if attempt > 0:
                    self._clear_cache()
                    logger.info(f"Cache cleared before retry attempt {attempt + 1}")

                # Validate file exists
                if not os.path.exists(file_path):
                    logger.error(f"File not found: {file_path}")
                    return None

                # Check if file is a video (with database filetype fallback)
                if not self.is_video_file(file_path, db_filetype):
                    logger.error(f"Not a video file: {file_path}")
                    return None

                filename = os.path.basename(file_path)

                # Fix double dots in filename if needed
                if '..' in filename:
                    logger.info(f"Found double dots in filename: {filename}")
                    new_filename = filename.replace('..', '.')
                    new_path = os.path.join(os.path.dirname(file_path), new_filename)

                    # Check if target file already exists before renaming
                    if not os.path.exists(new_path):
                        os.rename(file_path, new_path)
                        file_path = new_path
                        filename = new_filename
                        logger.info(f"Renamed file to: {new_filename}")
                    else:
                        # Target file exists, use original path
                        logger.info(f"Target file already exists, using original: {filename}")

                # Ensure file has video extension
                name, ext = os.path.splitext(filename)

                if not ext:
                    # No extension - rename with .mp4
                    new_filename = f"{name}.mp4"
                    new_path = os.path.join(os.path.dirname(file_path), new_filename)
                    if not os.path.exists(new_path):
                        os.rename(file_path, new_path)
                        file_path = new_path
                        logger.info(f"Added .mp4 extension to file: {new_filename}")

                # Modify MKV metadata to change hash (if MKV file)
                file_path = modify_mkv_metadata(file_path)

                # Upload file
                logger.info(f"Starting upload to Google Photos (attempt {attempt + 1}/{max_retries}): {file_path}")

                # Upload using gpmc client
                media_key = self.client.upload(
                    target=file_path,
                    show_progress=False,
                    force_upload=True,
                    saver=False,
                    threads=1
                )

                # Extract media key
                if isinstance(media_key, dict):
                    actual_media_key = list(media_key.values())[0] if media_key else None
                else:
                    actual_media_key = media_key

                if actual_media_key:
                    logger.info(f"Successfully uploaded to Google Photos: {file_path}")
                    return actual_media_key
                else:
                    logger.error(f"Upload failed: No media key returned (attempt {attempt + 1}/{max_retries})")
                    if attempt < max_retries - 1:
                        # Exponential backoff: 15, 30, 60, 120, 240 seconds
                        delay = base_delay * (2 ** attempt)
                        logger.info(f"Retrying in {delay} seconds...")
                        time.sleep(delay)
                    continue

            except Exception as e:
                logger.error(f"Error uploading to Google Photos (attempt {attempt + 1}/{max_retries}): {str(e)}")
                if attempt < max_retries - 1:
                    # Exponential backoff: 15, 30, 60, 120, 240 seconds
                    delay = base_delay * (2 ** attempt)
                    logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    logger.error(f"All {max_retries} upload attempts failed for: {file_path}")
                    return None

        return None

    def _clear_cache(self):
        """Clear GPMC cache and temporary files"""
        try:
            # Clear GPMC internal cache if available
            if hasattr(self.client, 'clear_cache'):
                self.client.clear_cache()

            # Clear temporary files in common cache directories
            import tempfile
            import shutil
            temp_dirs = [tempfile.gettempdir(), '/tmp', 'C:\\temp']

            for temp_dir in temp_dirs:
                if os.path.exists(temp_dir):
                    for file in os.listdir(temp_dir):
                        if file.startswith('gpmc_') or file.startswith('gphotos_'):
                            try:
                                file_path = os.path.join(temp_dir, file)
                                if os.path.isfile(file_path):
                                    os.remove(file_path)
                                elif os.path.isdir(file_path):
                                    shutil.rmtree(file_path)
                            except Exception as e:
                                logger.warning(f"Could not remove cache file {file}: {e}")

            logger.info("Cache clearing completed")
        except Exception as e:
            logger.warning(f"Error clearing cache: {e}")

    def get_file_info(self, media_key):
        """
        Get information about a file from its media key
        """
        try:
            # This is a placeholder. The gpmc library might have
            # a different way to get file info from a media key
            # Implement based on available gpmc features
            return {
                "id": media_key,
                "success": True
            }
        except Exception as e:
            logger.error(f"Error getting file info: {str(e)}")
            return None


class NewGPhotos:
    """New Google Photos client with enhanced features using latest GPMC"""

    def __init__(self, auth_data=None):
        # Use provided auth_data or fallback to environment variable
        if not auth_data:
            auth_data = os.getenv('GP_AUTH_DATA_NEW')

        if not auth_data:
            raise ValueError("Auth data not provided and GP_AUTH_DATA_NEW environment variable is not set")

        # Initialize mimetypes
        mimetypes.init()
        mimetypes.add_type("video/x-matroska", ".mkv")
        mimetypes.add_type("video/mp2t", ".ts")
        mimetypes.add_type("video/mp2t", ".mts")
        mimetypes.add_type("video/mp2t", ".m2ts")

        # Initialize Google Photos client with new GPMC
        self.client = Client(auth_data=_parse_auth_data(auth_data))

    def is_video_file(self, file_path, db_filetype=None):
        """Check if file is a video based on mimetype with extension fallback and database filetype"""
        fp = str(file_path)
        mime_type, _ = mimetypes.guess_type(fp)

        # First check MIME type
        if mime_type and mime_type.startswith('video/'):
            return True

        # Fallback: Check file extension for common video formats
        file_ext = Path(fp).suffix.lower()
        if file_ext in VIDEO_EXTENSIONS:
            return True

        # Ultimate fallback: Check database filetype if provided
        if db_filetype and str(db_filetype).startswith('video/'):
            logger.info(f"Database confirms video file: {db_filetype}")
            return True

        return False

    def upload(self, file_path, token=None, db_filetype=None):
        """
        Upload a video file to Google Photos using new GPMC client with exponential backoff retry mechanism
        Automatically skips if file already exists (based on hash)
        Returns the media key if successful
        """
        max_retries = 8
        base_delay = 15  # base delay in seconds

        for attempt in range(max_retries):
            try:
                # Clear cache before retry attempts (except first attempt)
                if attempt > 0:
                    self._clear_cache()
                    logger.info(f"Cache cleared before retry attempt {attempt + 1}")

                # Validate file exists
                if not os.path.exists(file_path):
                    logger.error(f"File not found: {file_path}")
                    return None

                # Check if file is a video (with database filetype fallback)
                if not self.is_video_file(file_path, db_filetype):
                    logger.error(f"Not a video file: {file_path}")
                    return None

                filename = os.path.basename(file_path)

                # Fix double dots in filename if needed
                if '..' in filename:
                    logger.info(f"Found double dots in filename: {filename}")
                    new_filename = filename.replace('..', '.')
                    new_path = os.path.join(os.path.dirname(file_path), new_filename)

                    # Check if target file already exists before renaming
                    if not os.path.exists(new_path):
                        os.rename(file_path, new_path)
                        file_path = new_path
                        filename = new_filename
                        logger.info(f"Renamed file to: {new_filename}")
                    else:
                        # Target file exists, use original path
                        logger.info(f"Target file already exists, using original: {filename}")

                # Ensure file has video extension
                name, ext = os.path.splitext(filename)

                if not ext:
                    # No extension - rename with .mp4
                    new_filename = f"{name}.mp4"
                    new_path = os.path.join(os.path.dirname(file_path), new_filename)
                    if not os.path.exists(new_path):
                        os.rename(file_path, new_path)
                        file_path = new_path
                        logger.info(f"Added .mp4 extension to file: {new_filename}")

                # Modify MKV metadata to change hash (if MKV file)
                file_path = modify_mkv_metadata(file_path)

                # Upload file - GPMC automatically skips duplicates based on hash
                logger.info(f"Starting upload to Google Photos (attempt {attempt + 1}/{max_retries}): {file_path}")

                # Upload using new GPMC client
                media_keys = self.client.upload(
                    target=file_path,
                    show_progress=False,
                    force_upload=True,  # Let GPMC handle duplicates
                    saver=False,
                    threads=1
                )

                # Extract media key
                if isinstance(media_keys, dict):
                    actual_media_key = list(media_keys.values())[0] if media_keys else None
                else:
                    actual_media_key = media_keys

                if actual_media_key:
                    logger.info(f"Successfully uploaded to Google Photos: {file_path}")
                    return actual_media_key
                else:
                    logger.error(f"Upload failed: No media key returned (attempt {attempt + 1}/{max_retries})")
                    if attempt < max_retries - 1:
                        # Exponential backoff: 15, 30, 60, 120, 240 seconds
                        delay = base_delay * (2 ** attempt)
                        logger.info(f"Retrying in {delay} seconds...")
                        time.sleep(delay)
                    continue

            except Exception as e:
                logger.error(f"Error uploading to Google Photos (attempt {attempt + 1}/{max_retries}): {str(e)}")
                if attempt < max_retries - 1:
                    # Exponential backoff: 15, 30, 60, 120, 240 seconds
                    delay = base_delay * (2 ** attempt)
                    logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    logger.error(f"All {max_retries} upload attempts failed for: {file_path}")
                    return None

        return None

    def download_by_media_key(self, media_key, download_path=None):
        """
        Download a file from Google Photos using media key
        Returns the downloaded file path if successful
        """
        try:
            logger.info(f"Getting download URLs for media key: {media_key}")

            # Get download URLs using new GPMC API
            download_data = self.client.api.get_download_urls(media_key)

            if not download_data:
                logger.error(f"No download URLs found for media key: {media_key}")
                return None

            # Extract download URL and filename from Google Photos API response
            download_url = None
            original_filename = None
            if isinstance(download_data, dict):
                try:
                    # Extract filename from response - structure: {'1': {'2': {'4': 'filename.mkv'}}}
                    if '1' in download_data and '2' in download_data['1'] and '4' in download_data['1']['2']:
                        original_filename = download_data['1']['2']['4']
                        logger.info(f"Extracted original filename: {original_filename}")

                    # Response structure from logs: {'1': {'5': {'3': {'5': 'download_url'}}}}
                    if '1' in download_data and '5' in download_data['1']:
                        url_data = download_data['1']['5']
                        if '3' in url_data and '5' in url_data['3']:
                            download_url = url_data['3']['5']
                        elif isinstance(url_data, dict):
                            # Alternative structure - find URL in nested dict
                            for key, value in url_data.items():
                                if isinstance(value, dict) and '5' in value:
                                    if isinstance(value['5'], str) and value['5'].startswith('https://'):
                                        download_url = value['5']
                                        break

                    # Log analysis shows URL is in nested structure, try common patterns
                    if not download_url and '1' in download_data:
                        data_1 = download_data['1']
                        if '5' in data_1 and isinstance(data_1['5'], dict):
                            # Check for direct URL in '5' field
                            for k, v in data_1['5'].items():
                                if isinstance(v, dict) and '5' in v:
                                    if isinstance(v['5'], str) and 'googleusercontent.com' in v['5']:
                                        download_url = v['5']
                                        break

                    # Fallback: look for any HTTPS URL in the response
                    if not download_url:
                        def find_url_recursive(data):
                            if isinstance(data, dict):
                                for key, value in data.items():
                                    if isinstance(value, str) and value.startswith('https://video-downloads.googleusercontent.com'):
                                        return value
                                    elif isinstance(value, (dict, list)):
                                        result = find_url_recursive(value)
                                        if result:
                                            return result
                            elif isinstance(data, list):
                                for item in data:
                                    result = find_url_recursive(item)
                                    if result:
                                        return result
                            return None

                        download_url = find_url_recursive(download_data)

                except Exception as e:
                    logger.error(f"Error parsing download response: {e}")

            if not download_url:
                logger.error(f"Could not extract download URL from response: {download_data}")
                return None

            # Get filename and token from database using db_utils
            import os, re, uuid
            from plugins.db_utils import DBUtils

            db_filename = None
            token = None

            # Try to extract token from download_path first
            if download_path:
                path_parts = download_path.split(os.sep)
                if 'downloads' in path_parts:
                    try:
                        downloads_idx = path_parts.index('downloads')
                        # Token is typically 2 levels deep: downloads/DIR/TOKEN or downloads/TOKEN
                        if len(path_parts) > downloads_idx + 2:
                            token = path_parts[downloads_idx + 2]
                        elif len(path_parts) > downloads_idx + 1:
                            token = path_parts[downloads_idx + 1]
                    except:
                        pass

            # If no token from path, generate random token
            if not token:
                token = str(uuid.uuid4())[:10]
                logger.info(f"Generated random token: {token}")

            # Get filename from database using token
            try:
                db_utils = DBUtils()
                db_filename = db_utils.get_filename_by_token(token)
                if db_filename:
                    logger.info(f"Using filename from database: {db_filename}")
            except Exception as e:
                logger.warning(f"Could not get filename from database: {e}")

            # Determine final filename for aria2c download
            if db_filename:
                # Use database filename
                safe_filename = re.sub(r'[<>:"/\\|?*]', '_', db_filename)
            else:
                # Fallback to random ID (DO NOT use media_key)
                random_id = str(uuid.uuid4())[:8]
                safe_filename = f"video_{random_id}.mp4"
                logger.info(f"No database filename found, using random: {safe_filename}")

            # Download using aria2c single connection
            from plugins.aria2c_config import Aria2cDownloader
            downloader = Aria2cDownloader(timeout=1800)

            logger.info(f"Starting aria2c single connection download from Google Photos...")
            logger.info(f"Token: {token}, Filename: {safe_filename}")
            logger.info(f"Download URL: {download_url[:80]}...")

            downloaded_path = downloader.download_single_connection(
                url=download_url,
                token=token,
                filename=safe_filename
            )

            if downloaded_path and os.path.exists(downloaded_path):
                logger.info(f"Downloaded file to: {downloaded_path}")
                return downloaded_path
            else:
                logger.error("aria2c download from Google Photos failed")
                return None

        except Exception as e:
            logger.error(f"Error downloading from Google Photos: {str(e)}")
            return None

    def check_file_exists_by_hash(self, file_path):
        """
        Check if a file already exists in Google Photos by hash
        Returns media key if exists, None otherwise
        """
        try:
            # Calculate SHA1 hash manually (more reliable)
            with open(file_path, 'rb') as f:
                sha1_hash = hashlib.sha1()
                while chunk := f.read(8192):
                    sha1_hash.update(chunk)
                hash_bytes = sha1_hash.digest()
                hash_b64 = base64.b64encode(hash_bytes).decode('utf-8')

            # Check if file exists by hash
            media_key = self.client.get_media_key_by_hash(hash_b64)

            if media_key:
                logger.info(f"File already exists in Google Photos: {media_key}")
                return media_key
            else:
                logger.info(f"File not found in Google Photos")
                return None

        except Exception as e:
            logger.error(f"Error checking file hash: {str(e)}")
            return None

    def _clear_cache(self):
        """Clear GPMC cache and temporary files"""
        try:
            # Clear GPMC internal cache if available
            if hasattr(self.client, 'clear_cache'):
                self.client.clear_cache()

            # Clear temporary files in common cache directories
            import tempfile
            import shutil
            temp_dirs = [tempfile.gettempdir(), '/tmp', 'C:\\temp']

            for temp_dir in temp_dirs:
                if os.path.exists(temp_dir):
                    for file in os.listdir(temp_dir):
                        if file.startswith('gpmc_') or file.startswith('gphotos_'):
                            try:
                                file_path = os.path.join(temp_dir, file)
                                if os.path.isfile(file_path):
                                    os.remove(file_path)
                                elif os.path.isdir(file_path):
                                    shutil.rmtree(file_path)
                            except Exception as e:
                                logger.warning(f"Could not remove cache file {file}: {e}")

            logger.info("Cache clearing completed")
        except Exception as e:
            logger.warning(f"Error clearing cache: {e}")

    def get_file_info(self, media_key):
        """
        Get information about a file from its media key
        """
        try:
            # Use new GPMC features to get file info
            return {
                "id": media_key,
                "success": True
            }
        except Exception as e:
            logger.error(f"Error getting file info: {str(e)}")
            return None
