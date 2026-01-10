import os
import time
from datetime import datetime, timedelta
from .database import Database
from .drive import GoogleDrive, DriveFileNotFoundError
from .pixeldrain import PixelDrain
from .gphotos import NewGPhotos
import shutil
from utils.logging_config import get_modern_logger

logger = get_modern_logger("AUTO_UPLOADER")

class AutoUploader:
    def __init__(self):
        self.db = Database()
        self.pixeldrain = PixelDrain()
        # self.gphotos is not initialized here anymore as we use dynamic auth in process_file
        
        # Intervals (seconds)
        self.check_interval = int(os.getenv('CHECK_INTERVAL', '5')) * 60
        # No upload gap needed - Redis queue handles concurrency
        
        # Create downloads directory if it doesn't exist
        self.downloads_dir = os.path.join(os.getcwd(), 'downloads', 'pixeldrain')
        if not os.path.exists(self.downloads_dir):
            os.makedirs(self.downloads_dir)
            
    def _get_auth_for_email(self, email):
        """Get auth data for a specific email from plugins/old/credential.json"""
        try:
            import json
            cred_path = os.path.join(os.path.dirname(__file__), 'old', 'credential.json')
            if not os.path.exists(cred_path):
                logger.warning(f"Credential file not found at {cred_path}")
                return None
            
            with open(cred_path, 'r') as f:
                credentials = json.load(f)
            
            # Credentials is a list of dicts with 'email' and 'auth'
            if isinstance(credentials, list):
                for cred in credentials:
                    if isinstance(cred, dict) and cred.get('email') == email:
                        return cred.get('auth')
            
            logger.warning(f"No auth credential found for email: {email}")
            return None
        except Exception as e:
            logger.error(f"Error reading credentials for {email}: {e}")
            return None
        
    def cleanup(self, token):
        """Cleanup downloads directory for a token"""
        cleanup_dir = os.path.join(self.downloads_dir, token)
        try:
            if os.path.exists(cleanup_dir):
                logger.debug(f"Cleaning up directory: {cleanup_dir}")
                shutil.rmtree(cleanup_dir)
        except Exception as e:
            logger.error(f"Failed to cleanup directory: {str(e)}")
    
    def process_file(self, link_data):
        """Process a single file"""
        # Validate input data first
        if not link_data or not isinstance(link_data, dict):
            logger.error("Invalid link_data provided to process_file")
            return
            
        token = link_data.get('token')
        file_id = link_data.get('drive_id')
        filename = link_data.get('filename', 'Unknown')
        
        if not token:
            logger.error("Missing token in link_data")
            return
            
        if not file_id:
            logger.error(f"Missing drive_id for token {token}")
            return
        
        logger.info(f"📤 Processing auto-upload for {token} - {filename}")
        
        try:
            # Do NOT flip status to pending or clear existing id for auto-reupload
            try:
                local_path, filename, filesize = None, None, None
                
                # Check if this file has a photos_id and gp_id (for auth)
                photos_id = link_data.get('gphotos_id')
                gp_id_encrypted = link_data.get('gp_id')
                
                # Decrypt photos_id (Always encrypted)
                if photos_id:
                    try:
                        from api.common import simple_decrypt
                        decrypted_id = simple_decrypt(photos_id)
                        if decrypted_id:
                            photos_id = decrypted_id
                            # logger.debug("Successfully decrypted gphotos_id")
                    except Exception as e:
                        logger.warning(f"Error attempting to decrypt gphotos_id: {e}")

                gphotos_client = None
                
                # Resolve Google Photos credentials using gp_id (encrypted email)
                if photos_id and gp_id_encrypted:
                    try:
                        from api.common import simple_decrypt
                        decrypted_email = simple_decrypt(gp_id_encrypted)
                        
                        if decrypted_email:
                            logger.debug(f"Decrypted email from gp_id: {decrypted_email}")
                            
                            # Get auth for this email from credential.json
                            auth_data = self._get_auth_for_email(decrypted_email)
                            
                            if auth_data:
                                logger.info(f"Using specific credentials for: {decrypted_email}")
                                # Initialize NewGPhotos with specific auth
                                gphotos_client = NewGPhotos(auth_data=auth_data)
                            else:
                                logger.warning(f"Skipping Google Photos: No auth found for {decrypted_email}")
                        else:
                            logger.warning("Skipping Google Photos: Failed to decrypt gp_id")
                            
                    except Exception as e:
                        logger.error(f"Error resolving Google Photos credentials: {e}")
                
                # Download from Google Photos only if we have valid client/credentials
                if photos_id and gphotos_client:
                    logger.info(f"Attempting download from Google Photos: {photos_id}")
                    try:
                        download_path = os.path.join(self.downloads_dir, token, f"gphotos_{token}.mp4")
                        os.makedirs(os.path.dirname(download_path), exist_ok=True)
                        
                        downloaded_file = gphotos_client.download_by_media_key(photos_id, download_path)
                        
                        if downloaded_file and os.path.exists(downloaded_file):
                            local_path = downloaded_file
                            filename = os.path.basename(downloaded_file)
                            filesize = f"{os.path.getsize(downloaded_file) / (1024*1024):.1f} MB"
                            logger.success(f"Successfully downloaded from Google Photos: {filename}")
                        else:
                            logger.warning(f"Google Photos download failed for {photos_id} - file may not exist, falling back to Google Drive")
                    except Exception as e:
                        logger.warning(f"Google Photos download error: {e}, falling back to Google Drive")
                
                # If Google Photos download failed or no photos_id, use Google Drive
                if not local_path:
                    # Create a fresh GoogleDrive instance for each file
                    drive = GoogleDrive()
                    
                    # Download from Google Drive
                    local_path, filename, filesize = drive.download_file(file_id, token, custom_dir='pixeldrain', job_type='auto_upload')
                
                # Upload to Pixeldrain
                pixeldrain_id = self.pixeldrain.upload(local_path)
                
                # Update database with completed status and new pixeldrain_id
                self.db.update_link(
                    token,
                    status='completed',
                    pixeldrain_id=pixeldrain_id,
                    filename=filename,
                    filesize=filesize,
                    error=None
                )
                logger.success(f"Successfully processed {filename} to Pixeldrain: {pixeldrain_id}")
                
            except DriveFileNotFoundError:
                logger.error(f"File {file_id} no longer exists in Google Drive")
                self.db.update_link(token, status='not_exist', error='File no longer exists in Google Drive')
                return
            except Exception as drive_error:
                # Handle "no access" and other drive errors
                error_str = str(drive_error).lower()
                if 'not found' in error_str or 'no access' in error_str or '404' in error_str or '403' in error_str:
                    logger.error(f"File {file_id} not accessible in Google Drive: {drive_error}")
                    self.db.update_link(token, status='not_exist', error=f'File not accessible: {drive_error}')
                    return
                else:
                    # Re-raise other exceptions
                    raise drive_error
                
            except Exception as e:
                error_str = str(e).lower()
                logger.error(f"Error processing file: {str(e)}")
                
                # Check if this is a recoverable error (API issues, rate limits, etc.)
                # Recoverable: API/Auth issues that can be fixed with new keys or retry
                recoverable_errors = [
                    'rate limit', 'too many requests', 'api limit', 'quota exceeded',
                    'temporarily unavailable', 'service unavailable', 'timeout',
                    'connection', 'network', 'banned', 'suspended', 'api key',
                    'authentication_failed', 'invalid', 'revoked', 'expired',
                    'unauthorized', '401', '429', '503', 'internal', 'writing',
                    'server may be out of storage space', 'internal server error'
                ]
                
                # Non-recoverable: File/client issues that won't be fixed by retry
                non_recoverable_errors = [
                    'no_file', 'file does not exist', 'empty', 'file_too_large',
                    'payload too large', 'name_too_long', '413', '422'
                ]
                
                is_recoverable = any(keyword in error_str for keyword in recoverable_errors)
                is_non_recoverable = any(keyword in error_str for keyword in non_recoverable_errors)
                
                if is_non_recoverable:
                    # Definitely non-recoverable error (file too large, corrupt, etc.)
                    logger.warning(f"NON-RECOVERABLE Permanent failure: {token}")
                    self.db.update_link(token, status='failed', error=str(e))
                elif is_recoverable and local_path and os.path.exists(local_path):
                    # File downloaded successfully but upload failed due to API issues
                    # Save to pd_lost for retry with potentially different API keys
                    logger.info(f"RECOVERABLE Saving to pd_lost for retry: {token}")
                    try:
                        self.db.save_pd_lost(
                            token=token,
                            drive_id=file_id,
                            filename=filename,
                            filesize=filesize,
                            filetype=os.path.splitext(filename)[1] if filename else None,
                            error=f"PixelDrain API error: {str(e)}"
                        )
                        self.db.update_link(token, status='retry_pending', error=f'Saved to pd_lost: {str(e)}')
                    except Exception as save_error:
                        logger.error(f"Failed to save to pd_lost: {save_error}")
                        self.db.update_link(token, status='failed', error=str(e))
                else:
                    # Non-recoverable error (file corruption, format issues, etc.)
                    self.db.update_link(token, status='failed', error=str(e))
                return
                
        except Exception as e:
            logger.error(f"File processing failed: {str(e)}")
            self.db.update_link(token, status='failed', error=str(e))
            
        finally:
            self.cleanup(token)
    
    def run(self):
        """Main loop to process expired links"""
        logger.info("📤 Starting Pixeldrain auto upload service")
        while True:
            try:
                # Check if processing is enabled via PIXELDRAIN_AUTO_UPLOAD_PROCESS
                if os.getenv('PIXELDRAIN_AUTO_UPLOAD_PROCESS', 'true').lower() != 'true':
                    # Processing is disabled, just sleep
                    time.sleep(self.check_interval)
                    continue
                    
                # Get expired links
                expired_links = list(self.db.get_expired_links(service='PIXELDRAIN'))
                
                if expired_links:
                    logger.info(f"Found {len(expired_links)} expired Pixeldrain links")
                    for link in expired_links:
                        # Process the file - no gap needed, Redis queue handles concurrency
                        self.process_file(link)
                else:
                    logger.info("No expired Pixeldrain links found")
                    
            except Exception as e:
                logger.error(f"Error in Pixeldrain auto uploader: {str(e)}")
                
            # Sleep for configured time before checking again
            time.sleep(self.check_interval)
            
def main():
    uploader = AutoUploader()
    uploader.run()
    
if __name__ == '__main__':
    main() 