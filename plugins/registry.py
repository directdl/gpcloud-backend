import os
from typing import List

from .storages.base import StorageUploader
from .storages.pixeldrain import PixelDrainStorage
from .storages.buzzheavier import BuzzHeavierStorage
from .storages.vikingfile import VikingFileStorage


def get_enabled_uploaders() -> List[StorageUploader]:
    """Instantiate and return enabled storage uploaders based on env flags."""
    candidates: list[StorageUploader] = []
    try:
        pix = PixelDrainStorage()
        if pix.is_enabled():
            candidates.append(pix)
    except Exception as e:
        print(f"PixelDrain storage disabled or failed to init: {e}")

    try:
        buzz = BuzzHeavierStorage()
        if buzz.is_enabled():
            candidates.append(buzz)
    except Exception as e:
        print(f"BuzzHeavier storage disabled or failed to init: {e}")

    try:
        viking = VikingFileStorage()
        if viking.is_enabled():
            candidates.append(viking)
    except Exception as e:
        print(f"VikingFile storage disabled or failed to init: {e}")

    return candidates


