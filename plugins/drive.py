import os
import json
import humanize
import tempfile
import time
import mimetypes
import pickle
import requests
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError
from io import FileIO
from .token_manager import TokenManager
from google.oauth2.credentials import Credentials as OAuthCredentials


class DriveFileNotFoundError(Exception):
    pass


class DriveQuotaExceededError(Exception):
    pass


class GoogleDrive:
    def __init__(self):
        self.service = None
        self.token_manager = TokenManager()
        self._file_info_cache = {}
        self._cache_size_limit = 100

        self.private_dir = os.path.join(os.path.dirname(__file__), '..', 'private')
        self.token_pickle_file = os.path.join(self.private_dir, 'token.pickle')

    def _sanitize_filename(self, filename):
        invalid_chars = '<>:"|?*\\/'
        sanitized = filename
        for char in invalid_chars:
            sanitized = sanitized.replace(char, '_')
        sanitized = ''.join(char if ord(char) >= 32 and ord(char) != 127 else '_' for char in sanitized)
        sanitized = sanitized.strip(' .')
        if not sanitized:
            sanitized = 'unnamed_file'
        return sanitized

    def _cleanup_cache(self):
        if len(self._file_info_cache) > self._cache_size_limit:
            items_to_remove = len(self._file_info_cache) - self._cache_size_limit
            oldest_keys = list(self._file_info_cache.keys())[:items_to_remove]
            for key in oldest_keys:
                del self._file_info_cache[key]
            print(f"Cleaned up {items_to_remove} cache entries")

    def _get_service(self):
        if self.service is not None:
            return self.service

        last_error = None

        try:
            if os.path.exists(self.token_pickle_file):
                with open(self.token_pickle_file, 'rb') as token:
                    credentials = pickle.load(token)
                if hasattr(credentials, "valid") and not credentials.valid:
                    if getattr(credentials, "expired", False) and getattr(credentials, "refresh_token", None):
                        credentials.refresh(Request())
                    else:
                        raise Exception("token.pickle credentials are invalid and cannot be refreshed")
                self.service = build('drive', 'v3', credentials=credentials, cache_discovery=False)
                print("Google Drive service created with token.pickle")
                return self.service
        except Exception as e:
            last_error = e
            print(f"Error creating service with token.pickle: {str(e)}")

        try:
            token = self.token_manager.load_token()
            if token:
                bearer_credentials = OAuthCredentials(token=token)
                self.service = build('drive', 'v3', credentials=bearer_credentials, cache_discovery=False)
                print("Google Drive service created with bearer token (TokenManager)")
                return self.service
            raise Exception("No token available from TokenManager")
        except Exception as e:
            last_error = e
            print(f"Error creating service with TokenManager token: {str(e)}")

        raise Exception(f"Google Drive authentication failed. Last error: {str(last_error)}")

    def _is_video_file(self, filename, filetype=""):
        ft = (filetype or "").lower()
        if ft.startswith("video/"):
            return True
        ext = os.path.splitext(filename or "")[1].lower()
        return ext in {".mp4", ".mkv", ".webm", ".avi", ".mov", ".m4v", ".ts"}

    def _get_file_info_via_worker_head(self, file_id, token=None):
        worker_urls = os.getenv('WORKER_URL', '').strip()
        if not worker_urls:
            raise Exception("No worker URLs configured in WORKER_URL")
        urls = [u.strip().rstrip("/") for u in worker_urls.split(",") if u.strip()]
        if not urls:
            raise Exception("No valid worker URLs found")

        base_url = urls[0]
        head_url = f"{base_url}/id/{file_id}"

        try:
            r = requests.head(head_url, allow_redirects=True, timeout=20, verify=False)
            cd = r.headers.get("Content-Disposition", "") or ""
            ct = r.headers.get("Content-Type", "") or ""
            cl = r.headers.get("Content-Length", "") or ""

            filename = None
            if "filename*=" in cd:
                try:
                    part = cd.split("filename*=")[1].split(";")[0].strip()
                    if part.lower().startswith("utf-8''"):
                        part = part[7:]
                    filename = requests.utils.unquote(part).strip().strip('"')
                except Exception:
                    filename = None
            if not filename and "filename=" in cd:
                try:
                    filename = cd.split("filename=")[1].split(";")[0].strip().strip('"')
                except Exception:
                    filename = None

            raw_size = int(cl) if str(cl).isdigit() else 0
            filesize = humanize.naturalsize(raw_size) if raw_size > 0 else 'Unknown'

            if not filename:
                filename = f'worker_head_{file_id}.bin'

            file_info = {
                'filename': filename,
                'filesize': filesize,
                'raw_size': raw_size,
                'file_id': file_id,
                'filetype': ct.split(";")[0].strip() if ct else 'application/octet-stream'
            }

            self._file_info_cache[file_id] = file_info
            self._cleanup_cache()
            print(f"File info retrieved via Worker HEAD: {file_info['filename']}")
            return file_info

        except Exception as e:
            raise Exception(f"Worker HEAD file info failed: {str(e)}")

    def get_file_info(self, file_id, token=None):
        if file_id in self._file_info_cache:
            return self._file_info_cache[file_id]

        if token:
            try:
                from plugins.db_utils import DBUtils
                db_utils = DBUtils()
                db_file_info = db_utils.get_file_info_by_token(token)
                if db_file_info and db_file_info.get('filename'):
                    file_info = {
                        'filename': db_file_info['filename'],
                        'filesize': db_file_info.get('filesize', 'Unknown'),
                        'raw_size': db_file_info.get('raw_size', 0),
                        'file_id': file_id,
                        'filetype': db_file_info.get('filetype', 'application/octet-stream')
                    }
                    self._file_info_cache[file_id] = file_info
                    self._cleanup_cache()
                    print(f"File info retrieved from database: {file_info['filename']}")
                    return file_info
            except Exception as e:
                print(f"Database file info lookup failed: {str(e)}")

        fileinfo_api_key = os.getenv('FILEINFO_API')
        if fileinfo_api_key:
            try:
                return self._get_file_info_via_api(file_id, fileinfo_api_key)
            except Exception as e:
                print(f"FileInfo API failed, falling back: {str(e)}")

        try:
            return self._get_file_info_via_worker_head(file_id, token=token)
        except Exception as e:
            print(f"Worker HEAD failed, trying Drive API metadata: {str(e)}")

        max_retries = 2
        retry_count = 0

        while retry_count < max_retries:
            try:
                service = self._get_service()
                try:
                    file = service.files().get(
                        fileId=file_id,
                        fields='name,size,id,mimeType',
                        supportsAllDrives=True
                    ).execute()
                except TypeError as e:
                    if 'includeItemsFromAllDrives' in str(e):
                        file = service.files().get(
                            fileId=file_id,
                            fields='name,size,id,mimeType'
                        ).execute()
                    else:
                        raise e

                filename = file.get('name', f'drive_api_meta_{file_id}.bin')
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

                self._file_info_cache[file_id] = file_info
                self._cleanup_cache()
                return file_info

            except HttpError as err:
                if err.resp.status == 404:
                    raise DriveFileNotFoundError('File not found or no access')
                elif err.resp.status == 403:
                    raise Exception('Access denied (403)')
                elif err.resp.status in [500, 502, 503, 504, 429] and retry_count < max_retries - 1:
                    retry_count += 1
                    time.sleep(2 ** retry_count)
                    continue
                else:
                    raise Exception(f'Failed to get file info: {err}')
            except Exception as e:
                if retry_count < max_retries - 1:
                    retry_count += 1
                    time.sleep(2 ** retry_count)
                    continue
                raise Exception(f"Request error: {str(e)}")

        raise Exception("Maximum retries exceeded")

    def _get_file_info_via_api(self, file_id, api_key):
        try:
            url = f"https://www.googleapis.com/drive/v3/files/{file_id}"
            params = {
                'key': api_key,
                'fields': 'name,size,id,mimeType',
                'supportsAllDrives': 'true'
            }
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()

            file_data = response.json()
            if not file_data or not isinstance(file_data, dict):
                raise Exception("Invalid API response: empty or malformed data")
            if 'name' not in file_data:
                raise Exception("Invalid API response: missing 'name' field")

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
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"Starting download", extra={"file_id": file_id, "token": token, "custom_dir": custom_dir, "job_type": job_type})

        if file_info is None:
            try:
                file_info = self.get_file_info(file_id, token)
            except Exception as e:
                print(f"⚠️ get_file_info failed: {e}")
                file_info = None

        filename_guess = (file_info or {}).get("filename") or f"download_{token}.bin"
        filetype_guess = (file_info or {}).get("filetype") or ""
        is_video = self._is_video_file(filename_guess, filetype_guess)

        api_allowed = (job_type == 'process_file') and is_video

        if api_allowed:
            try:
                print("🌐 Attempting API-based download (video + process_file)...")
                return self._download_with_api(file_id, token, custom_dir, file_info)
            except Exception as api_error:
                print(f"❌ API download failed: {str(api_error)}")
                try:
                    print("🌐 Falling back to worker-based download...")
                    return self._download_with_workers(file_id, token, custom_dir, file_info)
                except Exception as worker_error:
                    print(f"❌ Worker download also failed: {str(worker_error)}")
                    error_message = f"All download methods failed. API: {str(api_error)}, Worker: {str(worker_error)}"
                    final_error = Exception(error_message)
                    final_error.__cause__ = None
                    raise final_error
        else:
            try:
                print("🌐 Attempting worker-based download...")
                return self._download_with_workers(file_id, token, custom_dir, file_info)
            except Exception as worker_error:
                print(f"❌ Worker download failed: {str(worker_error)}")
                if job_type == 'process_file' and is_video:
                    try:
                        print("🌐 Worker failed, trying API as last chance (video + process_file)...")
                        return self._download_with_api(file_id, token, custom_dir, file_info)
                    except Exception as api_error:
                        print(f"❌ API download also failed: {str(api_error)}")
                        error_message = f"All download methods failed. Worker: {str(worker_error)}, API: {str(api_error)}"
                        final_error = Exception(error_message)
                        final_error.__cause__ = None
                        raise final_error
                error_message = f"Worker download failed. {str(worker_error)}"
                final_error = Exception(error_message)
                final_error.__cause__ = None
                raise final_error

    def _download_with_workers(self, file_id, token, custom_dir=None, file_info=None):
        worker_urls = os.getenv('WORKER_URL', '').strip()
        if not worker_urls:
            raise Exception("No worker URLs configured in WORKER_URL environment variable")

        urls = [url.strip() for url in worker_urls.split(',') if url.strip()]
        if not urls:
            raise Exception("No valid worker URLs found")

        print(f"Attempting worker-based download with {len(urls)} worker(s) using aria2c")

        filename = None
        try:
            from plugins.db_utils import DBUtils
            db_utils = DBUtils()
            filename = db_utils.get_filename_by_token(token)
            if filename:
                print(f"Using filename from database: {filename}")
        except Exception as e:
            print(f"Could not get filename from database: {e}")

        if not filename:
            if file_info and file_info.get("filename"):
                filename = file_info.get("filename")
            else:
                filename = f'worker_download_{token}.bin'
            print(f"Using fallback filename: {filename}")

        sanitized_filename = self._sanitize_filename(filename)

        from plugins.aria2c_config import Aria2cDownloader
        import random
        downloader = Aria2cDownloader(timeout=1800)

        retry_delays = [8, 24]
        max_retries = 2

        def build_download_url(base_url):
            base_url = (base_url or "").strip().rstrip("/")
            if not base_url:
                raise Exception("Empty worker base URL")
            return f"{base_url}/id/{file_id}"

        for attempt in range(max_retries + 1):
            try:
                random_base_url = random.choice(urls)
                download_url = build_download_url(random_base_url)

                if attempt == 0:
                    print(f"Worker attempt {attempt + 1}/{max_retries + 1}: Trying random worker {download_url} with aria2c")
                else:
                    print(f"Worker retry attempt {attempt}/{max_retries}: Trying random worker {download_url} after {retry_delays[attempt-1]}s delay")

                downloaded_path = downloader.download(
                    url=download_url,
                    token=token,
                    filename=sanitized_filename
                )

                if downloaded_path and os.path.exists(downloaded_path):
                    print(f"✅ Worker download successful: {sanitized_filename}")
                    return downloaded_path, filename, 'Unknown'
                raise Exception("Download failed - no file returned")

            except Exception as e:
                if attempt < max_retries:
                    delay = retry_delays[attempt]
                    print(f"❌ Worker attempt {attempt + 1} failed: {str(e)}. Retrying in {delay}s...")
                    time.sleep(delay)
                    continue
                print(f"❌ Worker failed after {max_retries + 1} attempts: {str(e)}")
                break

        worker_error = Exception(f"Worker download failed after {max_retries + 1} attempts with random worker URLs")
        worker_error.__cause__ = None
        raise worker_error

    def _download_with_api(self, file_id, token, custom_dir=None, file_info=None):
        api_key = os.getenv('API_DOWNLOADER_KEY', 'hridoygcloudsjruh748ufdbjdsbf123456')
        api_domain = os.getenv('API_DOWNLOADER_DOMAIN', 'https://share-instant-db.vercel.app')

        if not api_key or not api_domain:
            raise Exception("API downloader not configured - missing API_DOWNLOADER_KEY or API_DOWNLOADER_DOMAIN")

        print(f"🌐 Attempting API-based download: {file_id}")

        filename = None
        try:
            from plugins.db_utils import DBUtils
            db_utils = DBUtils()
            filename = db_utils.get_filename_by_token(token)
            if filename:
                print(f"Using filename from database: {filename}")
        except Exception as e:
            print(f"Could not get filename from database: {e}")

        if not filename:
            if file_info and file_info.get("filename"):
                filename = file_info.get("filename")
            else:
                filename = f'api_download_{token}.bin'
            print(f"Using fallback filename: {filename}")

        if custom_dir:
            downloads_dir = os.path.join(os.getcwd(), 'downloads', custom_dir, token)
        else:
            downloads_dir = os.path.join(os.getcwd(), 'downloads', token)
        os.makedirs(downloads_dir, exist_ok=True)

        sanitized_filename = self._sanitize_filename(filename)
        local_path = os.path.join(downloads_dir, sanitized_filename)

        try:
            api_url = f"{api_domain}/{api_key}/{file_id}"
            print(f"🔗 API URL: {api_url}")

            response = requests.get(api_url, timeout=90)
            response.raise_for_status()

            data = response.json()
            if not data or 'download_url' not in data:
                raise Exception("API response missing download_url")

            download_url = data['download_url']
            print(f"📥 Got download URL from API: {download_url[:100]}...")

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
            raise Exception("aria2c single connection download failed")

        except requests.exceptions.RequestException as e:
            api_error = Exception(f"API download failed: {str(e)}")
            api_error.__cause__ = None
            raise api_error
        except Exception as e:
            if os.path.exists(local_path):
                try:
                    os.remove(local_path)
                except Exception:
                    pass
            api_error = Exception(f"API download error: {str(e)}")
            api_error.__cause__ = None
            raise api_error

    def _download_as_export(self, file_id, token, custom_dir, file_info):
        try:
            if custom_dir:
                downloads_dir = os.path.join(os.getcwd(), 'downloads', custom_dir, token)
            else:
                downloads_dir = os.path.join(os.getcwd(), 'downloads', token)
            os.makedirs(downloads_dir, exist_ok=True)

            sanitized_filename = self._sanitize_filename(file_info['filename'])
            if not sanitized_filename.endswith('.pdf'):
                sanitized_filename = f"{sanitized_filename}.pdf"
            local_path = os.path.join(downloads_dir, sanitized_filename)

            service = self._get_service()
            request = service.files().export_media(fileId=file_id, mimeType="application/pdf")

            fh = FileIO(local_path, "wb")
            downloader = MediaIoBaseDownload(fh, request, chunksize=100 * 1024 * 1024)

            done = False
            while not done:
                status, done = downloader.next_chunk()

            fh.close()
            return local_path, file_info['filename'], "PDF Export"

        except Exception as e:
            try:
                if 'fh' in locals() and fh:
                    fh.close()
            except Exception:
                pass
            if 'local_path' in locals() and os.path.exists(local_path):
                try:
                    os.remove(local_path)
                except Exception:
                    pass
            export_error = Exception(f"Export failed: {str(e)}")
            export_error.__cause__ = None
            raise export_error

    def extract_file_id(self, drive_link):
        if 'drive.google.com' not in drive_link:
            return None

        file_id = None
        try:
            if '/file/d/' in drive_link:
                file_id = drive_link.split('/file/d/')[1].split('/')[0]
            elif 'id=' in drive_link:
                file_id = drive_link.split('id=')[1].split('&')[0]
            return file_id
        except Exception:
            return None
