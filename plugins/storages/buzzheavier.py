import os
from config import ENABLE_BUZZHEAVIER
from ..buzzheavier import BuzzHeavier as BuzzClient
from .base import StorageUploader


class BuzzHeavierStorage(StorageUploader):
    name = "BUZZHEAVIER"
    id_field = "buzzheavier_id"

    def __init__(self) -> None:
        self.client = BuzzClient()

    def is_enabled(self) -> bool:
        return ENABLE_BUZZHEAVIER

    def upload(self, file_path: str, token: str | None = None) -> str:
        return self.client.upload(file_path, token=token)


