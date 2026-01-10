import os
import asyncio
from typing import Optional

from ..vikingfile import VikingFile as VikingFileClient
from .base import StorageUploader


class VikingFileStorage(StorageUploader):
    name = "VIKINGFILE"
    id_field = "viking_id"

    def __init__(self) -> None:
        try:
            self.client = VikingFileClient()
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Failed to initialize VikingFile client: {e}")
            raise e

    def is_enabled(self) -> bool:
        return os.getenv('ENABLE_VIKINGFILE', 'false').lower() == 'true'

    def upload(self, file_path: str, token: Optional[str] = None, **kwargs) -> str:
        """Upload file using remote URLs (Google Photos only) - no local fallback."""
        # Get additional parameters for remote upload
        gphotos_id = kwargs.get('gphotos_id')
        file_id = kwargs.get('file_id')
        filename = kwargs.get('filename', os.path.basename(file_path) if file_path else None)
        
        # Run async upload in new event loop (threading safe) with timeout
        try:
            # Always create a new event loop for threading safety
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            try:
                # Use asyncio.wait_for with 300 second timeout
                result = loop.run_until_complete(
                    asyncio.wait_for(
                        self.client.upload(
                            file_path=None,  # No local file path - only remote upload
                            token=token,
                            gphotos_id=gphotos_id,
                            file_id=file_id,
                            filename=filename,
                        ),
                        timeout=300  # 5 minute timeout
                    )
                )
                return result
            finally:
                # Clean up the event loop
                loop.close()
                
        except asyncio.TimeoutError:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"⏰ VikingFile upload timeout (300s) for token {token}")
            raise Exception("VikingFile upload timeout (300 seconds)")
        except Exception as e:
            # Log the error but don't crash the main process
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"❌ VikingFile upload failed for token {token}: {str(e)}")
            raise e


