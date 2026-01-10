import os
import time
import shutil
from datetime import datetime, timedelta
from .database import Database
from .drive import GoogleDrive, DriveFileNotFoundError
from .buzzheavier import BuzzHeavier
from .gphotos import NewGPhotos
from .service.buzzheavier_service import BuzzHeavierService

class BuzzAuto:
    def __init__(self):
        self.db = Database()
        self.buzzheavier_service = BuzzHeavierService(self.db)
        self.buzzheavier = BuzzHeavier()
        self.gphotos = NewGPhotos()  # Add Google Photos client
        # Intervals (seconds)
        self.check_interval = int(os.getenv('CHECK_INTERVAL', '5')) * 60
        # No upload gap needed - Redis queue handles concurrency
        
        # Create downloads directory if it doesn't exist
        self.downloads_dir = os.path.join(os.getcwd(), 'downloads', 'buzzheavier')
        if not os.path.exists(self.downloads_dir):
            os.makedirs(self.downloads_dir)

    def cleanup(self, token):
        """Cleanup downloads directory for a token"""
        cleanup_dir = os.path.join(self.downloads_dir, token)
        try:
            if os.path.exists(cleanup_dir):
                shutil.rmtree(cleanup_dir)
        except Exception as e:
            print(f"Failed to cleanup directory: {str(e)}")

    def process_file(self, link_data):
        """Process a single file"""
        token = link_data['token']
        file_id = link_data['drive_id']
        
        try:
            # Do NOT flip status to pending or clear existing id for auto-reupload
            try:
                local_path, filename, filesize = None, None, None
                
                # Check if this file has a photos_id (alternative source)
                photos_id = link_data.get('gphotos_id')
                
                if photos_id:
                    # Decrypt gphotos_id if encrypted (base64 encoded)
                    if len(photos_id) > 20 and not photos_id.startswith('AF1Q'):  # Not a raw Google Photos ID
                        try:
                            from api.common import simple_decrypt
                            decrypted_id = simple_decrypt(photos_id)
                            if decrypted_id:
                                photos_id = decrypted_id
                                print(f"[DEBUG] Successfully decrypted gphotos_id")
                            else:
                                print(f"[WARN] Failed to decrypt gphotos_id, skipping Google Photos download")
                                photos_id = None
                        except Exception as e:
                            print(f"[ERROR] Decryption error: {e}")
                            photos_id = None
                    
                    if photos_id:
                        # Try to download from Google Photos first
                        print(f"Attempting download from Google Photos: {photos_id}")
                        try:
                            download_path = os.path.join(self.downloads_dir, token, f"gphotos_{token}.mp4")
                            os.makedirs(os.path.dirname(download_path), exist_ok=True)
                            
                            downloaded_file = self.gphotos.download_by_media_key(photos_id, download_path)
                            
                            if downloaded_file and os.path.exists(downloaded_file):
                                local_path = downloaded_file
                                filename = os.path.basename(downloaded_file)
                                filesize = f"{os.path.getsize(downloaded_file) / (1024*1024):.1f} MB"
                                print(f"Successfully downloaded from Google Photos: {filename}")
                            else:
                                print(f"Google Photos download failed for {photos_id} - file may not exist, falling back to Google Drive")
                        except Exception as e:
                            print(f"Google Photos download error: {e}, falling back to Google Drive")
                
                # If Google Photos download failed or no photos_id, use Google Drive
                if not local_path:
                    # Create a fresh GoogleDrive instance for each file
                    drive = GoogleDrive()
                    
                    # Download directly to buzzheavier directory
                    local_path, filename, filesize = drive.download_file(file_id, token, custom_dir='buzzheavier')
                
                # Upload to BuzzHeavier
                buzzheavier_id = self.buzzheavier.upload(local_path, token)
                
                # Update database with completed status
                self.db.update_link(
                    token,
                    status='completed',
                    buzzheavier_id=buzzheavier_id,
                    filename=filename,
                    filesize=filesize,
                    error=None
                )

                # Update auto upload time using the service
                self.buzzheavier_service.update_upload_time(token)
                print(f"Successfully processed {filename} to BuzzHeavier: {buzzheavier_id}")
                
            except DriveFileNotFoundError:
                print(f"File {file_id} no longer exists in Google Drive")
                self.db.update_link(token, status='not_exist', error='File no longer exists in Google Drive')
                return
            except Exception as drive_error:
                # Handle "no access" and other drive errors
                error_str = str(drive_error).lower()
                if 'not found' in error_str or 'no access' in error_str or '404' in error_str or '403' in error_str:
                    print(f"File {file_id} not accessible in Google Drive: {drive_error}")
                    self.db.update_link(token, status='not_exist', error=f'File not accessible: {drive_error}')
                    return
                else:
                    # Re-raise other exceptions
                    raise drive_error
                
            except Exception as e:
                print(f"Error processing file: {str(e)}")
                self.db.update_link(token, status='failed', error=str(e))
                return
                
        except Exception as e:
            print(f"File processing failed: {str(e)}")
            self.db.update_link(token, status='failed', error=str(e))
            
        finally:
            self.cleanup(token)

    def run(self):
        """Main loop to process expired links"""
        print("Starting BuzzHeavier auto upload service")
        while True:
            try:
                # Get expired BuzzHeavier links using the service
                expired_links = list(self.buzzheavier_service.get_expired_links())

                if expired_links:
                    print(f"Found {len(expired_links)} expired BuzzHeavier links")
                    for link in expired_links:
                        # Process the file - no gap needed, Redis queue handles concurrency
                        self.process_file(link)
                else:
                    print("No expired BuzzHeavier links found")
                    
            except Exception as e:
                print(f"Error in BuzzHeavier auto uploader: {str(e)}")
                
            # Sleep for configured time before checking again
            time.sleep(self.check_interval)
            
def main():
    uploader = BuzzAuto()
    uploader.run()
    
if __name__ == '__main__':
    main() 