import os
import subprocess
import logging
from typing import Optional

# Configure logger
logger = logging.getLogger(__name__)

class Aria2cDownloader:
    """Aria2c downloader class with multi-connection and single connection support"""
    
    def __init__(self, timeout: int = 1800):
        """
        Initialize Aria2cDownloader
        
        Args:
            timeout: Download timeout in seconds (default: 30 minutes)
        """
        self.timeout = timeout
    
    def download(self, url: str, token: str, filename: str) -> Optional[str]:
        """Download file using aria2c with multiple connections"""
        try:
            logger.info(f"🚀 Using aria2c for download: {url[:30]}...")
            
            # Create download directory
            download_dir = os.path.join(os.getcwd(), 'downloads', 'recovery', token)
            os.makedirs(download_dir, exist_ok=True)
            
            # Build aria2c command
            cmd = [
                "aria2c",
                "--max-connection-per-server=16",
                "--split=16",
                "--min-split-size=1M",
                "--continue=true",
                "--retry-wait=5",
                "--max-tries=5",
                "--connect-timeout=30",
                "--timeout=120",
                "--check-certificate=false",
                "--auto-file-renaming=false",
                "--console-log-level=error",
                "--summary-interval=0",
                "--file-allocation=none",
                "--allow-overwrite=true",
                "--dir", download_dir,
                "--out", filename,
                url
            ]
            
            logger.info(f"Starting aria2c download...")
            logger.info(f"Download directory: {download_dir}")
            logger.info(f"Output filename: {filename}")
            
            # Run aria2c command
            result = subprocess.run(
                cmd,
                capture_output=False,
                text=True,
                timeout=self.timeout
            )
            
            # Check if download was successful
            expected_file_path = os.path.join(download_dir, filename)
            
            if os.path.exists(expected_file_path):
                file_size = os.path.getsize(expected_file_path)
                if file_size > 0:
                    logger.info(f"aria2c download completed: {expected_file_path} ({file_size / (1024*1024):.1f} MB)")
                    return expected_file_path
                else:
                    logger.error("aria2c downloaded 0 bytes")
                    # Clean up empty file
                    try:
                        os.remove(expected_file_path)
                        logger.info(f"Cleaned up empty file: {expected_file_path}")
                    except:
                        pass
                    return None
            else:
                logger.error("aria2c download failed - file not created")
                return None
                
        except subprocess.TimeoutExpired:
            logger.error(f"aria2c download timed out after {self.timeout//60} minutes")
            # Clean up any partial files
            expected_file_path = os.path.join(download_dir, filename)
            if os.path.exists(expected_file_path):
                try:
                    os.remove(expected_file_path)
                    logger.info(f"Cleaned up partial file after timeout: {expected_file_path}")
                except:
                    pass
            return None
        except Exception as e:
            logger.error(f"aria2c download failed: {str(e)}")
            # Clean up any partial files
            expected_file_path = os.path.join(download_dir, filename)
            if os.path.exists(expected_file_path):
                try:
                    os.remove(expected_file_path)
                    logger.info(f"Cleaned up partial file after error: {expected_file_path}")
                except:
                    pass
            return None

    def download_single_connection(self, url: str, token: str, filename: str) -> Optional[str]:
        """Download file using aria2c with single connection (for API downloads)"""
        try:
            logger.info(f"🚀 Using aria2c with single connection for API download: {url[:30]}...")
            
            # Create download directory
            download_dir = os.path.join(os.getcwd(), 'downloads', 'recovery', token)
            os.makedirs(download_dir, exist_ok=True)
            
            # Build aria2c command with single connection
            cmd = [
                "aria2c",
                "--max-connection-per-server=1",  # Single connection for API downloads
                "--split=1",                     # No splitting
                "--min-split-size=1M",
                "--continue=true",
                "--retry-wait=5",
                "--max-tries=5",
                "--connect-timeout=30",
                "--timeout=120",
                "--check-certificate=false",
                "--auto-file-renaming=false",
                "--console-log-level=error",
                "--summary-interval=0",
                "--file-allocation=none",
                "--allow-overwrite=true",
                "--dir", download_dir,
                "--out", filename,
                url
            ]
            
            logger.info(f"Starting aria2c single connection download...")
            logger.info(f"Download directory: {download_dir}")
            logger.info(f"Output filename: {filename}")
            
            # Run aria2c command
            result = subprocess.run(
                cmd,
                capture_output=False,
                text=True,
                timeout=self.timeout
            )
            
            # Check if download was successful
            expected_file_path = os.path.join(download_dir, filename)
            
            if os.path.exists(expected_file_path):
                file_size = os.path.getsize(expected_file_path)
                if file_size > 0:
                    logger.info(f"aria2c single connection download completed: {expected_file_path} ({file_size / (1024*1024):.1f} MB)")
                    return expected_file_path
                else:
                    logger.error("aria2c single connection downloaded 0 bytes")
                    # Clean up empty file
                    try:
                        os.remove(expected_file_path)
                        logger.info(f"Cleaned up empty file: {expected_file_path}")
                    except:
                        pass
                    return None
            else:
                logger.error("aria2c single connection download failed - file not created")
                return None
                
        except subprocess.TimeoutExpired:
            logger.error(f"aria2c single connection download timed out after {self.timeout//60} minutes")
            # Clean up any partial files
            expected_file_path = os.path.join(download_dir, filename)
            if os.path.exists(expected_file_path):
                try:
                    os.remove(expected_file_path)
                    logger.info(f"Cleaned up partial file after timeout: {expected_file_path}")
                except:
                    pass
            return None
        except Exception as e:
            logger.error(f"aria2c single connection download failed: {str(e)}")
            # Clean up any partial files
            expected_file_path = os.path.join(download_dir, filename)
            if os.path.exists(expected_file_path):
                try:
                    os.remove(expected_file_path)
                    logger.info(f"Cleaned up partial file after error: {expected_file_path}")
                except:
                    pass
            return None

    def download_with_auth_header(self, url: str, token: str, filename: str, download_token: str = None, custom_dir: str = None) -> Optional[str]:
        """Download file using aria2c with single connection and Authorization header
        
        Args:
            url: Download URL
            token: Authorization Bearer token
            filename: Output filename
            download_token: Optional custom download token for directory naming (default: uses first 10 chars of auth token)
            custom_dir: Optional custom directory (e.g. 'buzzheavier')
        """
        try:
            logger.info(f"🚀 Using aria2c with Authorization header for API download: {url[:30]}...")
            
            # Create download directory - use download_token if provided, else use first 10 chars of auth token
            dir_token = download_token if download_token else token[:10]
            
            if custom_dir:
                download_dir = os.path.join(os.getcwd(), 'downloads', custom_dir, dir_token)
            else:
                download_dir = os.path.join(os.getcwd(), 'downloads', dir_token)
            
            os.makedirs(download_dir, exist_ok=True)
            
            # Build aria2c command with single connection and Authorization header
            cmd = [
                "aria2c",
                "--max-connection-per-server=8",  # Single connection for API downloads
                "--split=8",                     # No splitting
                "--min-split-size=1M",
                "--continue=true",
                "--retry-wait=5",
                "--max-tries=5",
                "--connect-timeout=30",
                "--timeout=120",
                "--check-certificate=false",
                "--auto-file-renaming=false",
                "--console-log-level=error",
                "--summary-interval=0",
                "--file-allocation=none",
                "--allow-overwrite=true",
                f"--header=Authorization: Bearer {token}",  # Add Authorization header
                "--dir", download_dir,
                "--out", filename,
                url
            ]
            
            logger.info(f"Starting aria2c download with Authorization header...")
            logger.info(f"Download directory: {download_dir}")
            logger.info(f"Output filename: {filename}")
            logger.info(f"Using Authorization header with token: {token[:10]}...")
            
            # Run aria2c command
            result = subprocess.run(
                cmd,
                capture_output=False,
                text=True,
                timeout=self.timeout
            )
            
            # Check if download was successful
            expected_file_path = os.path.join(download_dir, filename)
            
            if os.path.exists(expected_file_path):
                file_size = os.path.getsize(expected_file_path)
                if file_size > 0:
                    logger.info(f"aria2c download with Authorization header completed: {expected_file_path} ({file_size / (1024*1024):.1f} MB)")
                    return expected_file_path
                else:
                    logger.error("aria2c download with Authorization header downloaded 0 bytes")
                    # Clean up empty file
                    try:
                        os.remove(expected_file_path)
                        logger.info(f"Cleaned up empty file: {expected_file_path}")
                    except:
                        pass
                    return None
            else:
                logger.error("aria2c download with Authorization header failed - file not created")
                return None
                
        except subprocess.TimeoutExpired:
            logger.error(f"aria2c download with Authorization header timed out after {self.timeout//60} minutes")
            # Clean up any partial files
            expected_file_path = os.path.join(download_dir, filename)
            if os.path.exists(expected_file_path):
                try:
                    os.remove(expected_file_path)
                    logger.info(f"Cleaned up partial file after timeout: {expected_file_path}")
                except:
                    pass
            return None
        except Exception as e:
            logger.error(f"aria2c download with Authorization header failed: {str(e)}")
            # Clean up any partial files
            expected_file_path = os.path.join(download_dir, filename)
            if os.path.exists(expected_file_path):
                try:
                    os.remove(expected_file_path)
                    logger.info(f"Cleaned up partial file after error: {expected_file_path}")
                except:
                    pass
            return None