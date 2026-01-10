import os
import time
from datetime import datetime
from .database import Database
from .autouploader import AutoUploader
from utils.logging_config import get_modern_logger

logger = get_modern_logger("LINK_CHECKER")

class LinkChecker:
    def __init__(self):
        self.db = Database()
        
        # Only initialize AutoUploader if Pixeldrain auto upload is enabled
        self.pixeldrain_uploader = None
        if os.getenv('PIXELDRAIN_AUTO_UPLOAD_PROCESS', 'true').lower() == 'true':
            try:
                self.pixeldrain_uploader = AutoUploader()
            except Exception as e:
                logger.warning(f"Failed to initialize AutoUploader: {e}")
        
        # Convert minutes to seconds
        self.check_interval = int(os.getenv('CHECK_INTERVAL', '5')) * 60
        self.process_timeout = int(os.getenv('PROCESS_TIMEOUT', '600'))  # 5 minutes timeout

    def can_process(self):
        """Check if we can process (Redis queue handles timing, this just checks for stuck processes)"""
        try:
            status = self.db.get_bot_status('BOT')
            if status['status'] == 'processing':
                # Check if process is stuck
                if status.get('last_process_time'):
                    last_time = status['last_process_time']
                    if isinstance(last_time, str):
                        last_time = datetime.fromisoformat(last_time.replace('Z', '+00:00'))
                    
                    time_diff = (datetime.utcnow() - last_time).total_seconds()
                    if time_diff > self.process_timeout:
                        logger.warning(f"Process stuck for {time_diff} seconds, resetting status")
                        self.db.update_bot_status('BOT', 
                            status='idle',
                            current_token=None,
                            error=f"Process timeout after {time_diff} seconds"
                        )
                        return True
                return False
                
            # Redis queue handles timing and concurrency, so always allow processing
            return True
        except Exception as e:
            logger.error(f"Error in can_process: {str(e)}")
            return False

    def process_expired_links(self):
        """Process expired links for both services using Redis queue"""
        try:
            # Import Redis queue
            from queueing.redis_queue import RedisJobQueue
            redis_queue = RedisJobQueue()
            
            # Stream expired links for both services
            
            # Check if Pixeldrain auto upload processing is enabled
            if os.getenv('PIXELDRAIN_AUTO_UPLOAD_PROCESS', 'true').lower() == 'true':
                pixeldrain_links = self.db.get_expired_links(service='PIXELDRAIN')
                
                # Process Pixeldrain links
                pd_count = 0
                for link in pixeldrain_links:
                    try:
                        job_id = redis_queue.enqueue_auto_upload(link['token'], 'pixeldrain', link)
                        logger.success(f"Enqueued Pixeldrain auto-upload: {link['token']} -> {job_id}")
                        pd_count += 1
                        
                        # Limit to avoid flooding the queue
                        if pd_count >= int(os.getenv('MAX_AUTO_UP', '2')) * 2:
                            break
                    except Exception as e:
                        logger.error(f"Failed to enqueue Pixeldrain job for {link['token']}: {e}")
            else:
                pd_count = 0
                pixeldrain_links = []
                # logger.debug("Pixeldrain auto upload processing is disabled")

            if pd_count == 0:
                logger.info("No expired links found")
            else:
                logger.success(f"Enqueued {pd_count} Pixeldrain auto-upload jobs")
                
        except Exception as e:
            logger.error(f"Error in process_expired_links: {str(e)}")

    def process_pixeldrain(self, link):
        """Process a Pixeldrain link"""
        if not self.pixeldrain_uploader:
            logger.warning("AutoUploader not initialized, skipping Pixeldrain processing")
            return
            
        print(f"Processing Pixeldrain link: {link['token']}")
        
        # Update bot status
        self.db.update_bot_status('BOT', 
            status='processing',
            current_token=link['token'],
            last_process_time=datetime.utcnow(),
            last_service='PIXELDRAIN'
        )
        
        try:
            # Process the link
            self.pixeldrain_uploader.process_file(link)
        except Exception as e:
            print(f"Error processing Pixeldrain link: {str(e)}")
            self.db.update_link(link['token'], 
                status='failed',
                error=str(e)
            )
        finally:
            # Update bot status to idle
            self.db.update_bot_status('BOT', 
                status='idle',
                current_token=None
            )

    def run(self):
        """Main loop to check and process expired links"""
        logger.info("🔗 Starting Link Checker service")
        while True:
            try:
                self.process_expired_links()
            except Exception as e:
                logger.error(f"Error in Link Checker: {str(e)}")
                # Reset bot status on critical error
                self.db.update_bot_status('BOT', 
                    status='idle',
                    current_token=None,
                    error=str(e)
                )
                
            # Sleep before next check
            time.sleep(self.check_interval)
            
def main():
    checker = LinkChecker()
    checker.run()
    
if __name__ == '__main__':
    main() 