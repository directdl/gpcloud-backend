import os
from ..pixeldrain import PixelDrain as PixelDrainClient
from .base import StorageUploader


class PixelDrainStorage(StorageUploader):
    name = "PIXELDRAIN"
    id_field = "pixeldrain_id"

    def __init__(self) -> None:
        self.client = PixelDrainClient()

    def is_enabled(self) -> bool:
        return os.getenv('ENABLE_PIXELDRAIN', 'true').lower() == 'true'

    def upload(self, file_path: str, token: str | None = None) -> str:
        return self.client.upload(file_path)


