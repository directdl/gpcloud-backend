from __future__ import annotations

from abc import ABC, abstractmethod


class StorageUploader(ABC):
    """Abstract interface for storage uploaders.

    Implementations MUST be side-effect free w.r.t. database. Callers handle DB updates.
    """

    name: str  # e.g., "PIXELDRAIN", "BUZZHEAVIER"
    id_field: str  # e.g., "pixeldrain_id", "buzzheavier_id"

    @abstractmethod
    def is_enabled(self) -> bool:
        """Whether this storage is enabled by environment/config."""
        raise NotImplementedError

    @abstractmethod
    def upload(self, file_path: str, token: str | None = None) -> str:
        """Upload file and return provider-specific file id."""
        raise NotImplementedError


