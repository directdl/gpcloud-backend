import os
import time
from abc import ABC, abstractmethod
from utils.logging_config import get_modern_logger


class BackgroundService(ABC):
    """Base class for background services with simple timing controls."""

    def __init__(self) -> None:
        self.check_interval = int(os.getenv('CHECK_INTERVAL', '5')) * 60
        self.logger = get_modern_logger(self.__class__.__name__)

    @abstractmethod
    def run_once(self) -> None:
        """Run a single iteration of the service."""
        raise NotImplementedError

    def run_forever(self) -> None:
        while True:
            try:
                self.run_once()
            except Exception as e:
                self.logger.error(f"Service error: [bright_red]{e}[/]")
            time.sleep(self.check_interval)


