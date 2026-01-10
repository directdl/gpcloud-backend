from __future__ import annotations

import os
import logging
import aiohttp
import asyncio
from typing import Optional

logger = logging.getLogger(__name__)


class VikingFile:
    """Direct VikingFile API client without external dependencies."""

    def __init__(self) -> None:
        """Initializes the VikingFile client."""
        self.user_hash = os.getenv('VIKINGFILE_USER_HASH', '')  # Empty for anonymous
        
        # Worker URLs for fallback download
        self.worker_urls = os.getenv('WORKER_URL', '').strip()
        
        # VikingFile API endpoints
        self.api_base = 'https://vikingfile.com/api'
        self.get_server_url = f'{self.api_base}/get-server'

    async def _get_upload_server(self) -> Optional[str]:
        """Get upload server URL from VikingFile API."""
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                async with session.get(self.get_server_url) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return data.get('server')
        except Exception as e:
            logger.error(f"❌ VikingFile: Server API error - {str(e)}")
        return None

    async def _get_photos_download_url(self, gphotos_id: str) -> Optional[str]:
        """Get Google Photos download URL using NewGPhotos client."""
        try:
            from plugins.gphotos import NewGPhotos
            from api.common import simple_decrypt
            
            # Decrypt gphotos_id if encrypted
            decrypted_id = gphotos_id
            if len(gphotos_id) > 20 and not gphotos_id.startswith('AF1Q'):
                try:
                    decrypted_id = simple_decrypt(gphotos_id)
                    if not decrypted_id:
                        return None
                except Exception as e:
                    logger.error(f"Decryption error: {e}")
                    return None
            
            # Get download URLs using NewGPhotos
            gphotos = NewGPhotos()
            download_data = gphotos.client.api.get_download_urls(decrypted_id)

            if not download_data:
                return None
            
            # Extract download URL using the EXACT same logic as working NewGPhotos.download_by_media_key
            download_url = None
            original_filename = None
            if isinstance(download_data, dict):
                try:
                    # Extract filename from response - structure: {'1': {'2': {'4': 'filename.mkv'}}}
                    if '1' in download_data and '2' in download_data['1'] and '4' in download_data['1']['2']:
                        original_filename = download_data['1']['2']['4']
                    
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
                return None

            return download_url
                
        except Exception as e:
            logger.error(f"Error getting Google Photos download URL: {e}")
            return None

    def _get_worker_download_url(self, file_id: str) -> Optional[str]:
        """Get worker download URL as fallback."""
        if not self.worker_urls:
            return None
        
        # Parse multiple URLs (comma-separated)
        urls = [url.strip() for url in self.worker_urls.split(',') if url.strip()]
        if not urls:
            return None
        
        # Use first available worker URL
        base_url = urls[0]
        if base_url.endswith('/'):
            base_url = base_url.rstrip('/')
        
        # Check if URL already has download endpoint
        if 'download.aspx' in base_url or '/download' in base_url:
            worker_url = f"{base_url}?id={file_id}"
        else:
            worker_url = f"{base_url}/download.aspx?id={file_id}"
        
        return worker_url

    async def _upload_remote_file(self, upload_server: str, download_url: str, filename: str, path: Optional[str] = None) -> dict:
        """Upload remote file to VikingFile using direct API with streaming response."""
        try:
            from aiohttp import FormData

            # Prepare form data (multipart/form-data)
            form_data = FormData()
            form_data.add_field('link', download_url)
            form_data.add_field('user', self.user_hash)  # Empty for anonymous
            form_data.add_field('name', filename)
            if path:
                form_data.add_field('path', path)

            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=1200)) as session:
                async with session.post(upload_server, data=form_data) as resp:
                    if resp.status != 200:
                        error_text = await resp.text()
                        raise Exception(f"VikingFile HTTP {resp.status}: {error_text[:100]}")

                    # Read streaming response
                    final_result = None
                    async for line in resp.content:
                        line_str = line.decode('utf-8').strip()
                        if line_str:
                            try:
                                import json
                                data = json.loads(line_str)

                                if 'progress' in data:
                                    # Log only at major milestones
                                    progress_str = data.get('progress', '0%')
                                    progress_percent = float(progress_str.rstrip('%'))
                                    milestones = [25, 50, 75, 100]
                                    if any(abs(progress_percent - milestone) < 0.1 for milestone in milestones):
                                        logger.info(f"📊 VikingFile: {progress_str} complete")

                                elif 'hash' in data and 'url' in data:
                                    final_result = data
                                    logger.info(f"✅ VikingFile: Upload complete - {data.get('hash')}")

                            except json.JSONDecodeError:
                                pass  # Skip non-JSON lines

                    if final_result:
                        return final_result
                    else:
                        raise Exception("Upload incomplete - no result received")

        except Exception as e:
            logger.error(f"❌ VikingFile: Upload error - {str(e)}")
            raise

    async def upload(
        self,
        file_path: str = None,
        token: Optional[str] = None,
        *,
        path: Optional[str] = None,
        public_share_path: Optional[str] = None,
        gphotos_id: Optional[str] = None,
        file_id: Optional[str] = None,
        filename: Optional[str] = None,
    ) -> str:
        """Upload file using remote URL with priority fallback and return VikingFile hash.
        
        Priority:
        1. Google Photos download URL (if gphotos_id provided)
        2. Worker download URL (fallback)
        
        Note: This is an optional service - no local fallback, fails gracefully
        """
        
        # Get upload server first
        upload_server = await self._get_upload_server()
        if not upload_server:
            raise Exception("Failed to get VikingFile upload server")
        
        # Priority 1: Try Google Photos remote upload
        if gphotos_id:
            try:
                photos_url = await self._get_photos_download_url(gphotos_id)
                if photos_url:
                    logger.info(f"🔗 VikingFile: Using gphotos URL...")
                    result = await self._upload_remote_file(upload_server, photos_url, filename, path)
                    file_hash = result.get('hash')
                    logger.info(f"✅ VikingFile: Success (gphotos) - {file_hash}")
                    return file_hash
                else:
                    logger.warning("⚠️ VikingFile: Google Photos URL failed")

            except Exception as e:
                logger.info(f"⚠️ VikingFile: Google Photos failed, trying worker")

        # Priority 2: Try Worker URL fallback
        if file_id:
            try:
                worker_url = self._get_worker_download_url(file_id)
                if worker_url:
                    logger.info(f"🔗 VikingFile: Using worker URL...")
                    result = await self._upload_remote_file(upload_server, worker_url, filename, path)
                    file_hash = result.get('hash')
                    logger.info(f"✅ VikingFile: Success (worker) - {file_hash}")
                    return file_hash
                else:
                    raise Exception("No worker URLs configured")

            except Exception as e:
                logger.error(f"❌ VikingFile: Worker upload failed - {str(e)}")
                raise Exception(f"VikingFile upload failed: {str(e)}")
        else:
            raise Exception("No file_id provided for worker fallback")


