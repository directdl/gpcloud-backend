from __future__ import annotations

from typing import Optional

DEFAULT_MAX_FILE_SIZE_GB = 6.5

def parse_size_to_gb(filesize_str: Optional[str]) -> Optional[float]:
    """Convert a human-readable filesize string to gigabytes."""
    if not filesize_str:
        return None

    size_str = str(filesize_str).strip().upper()
    if not size_str:
        return None

    try:
        if size_str.endswith("GB"):
            return float(size_str[:-2].strip())
        if size_str.endswith("MB"):
            return float(size_str[:-2].strip()) / 1024
        if size_str.endswith("TB"):
            return float(size_str[:-2].strip()) * 1024
        if size_str.endswith("KB"):
            return float(size_str[:-2].strip()) / (1024 ** 2)
        if size_str.endswith("B"):
            return float(size_str[:-1].strip()) / (1024 ** 3)
        return float(size_str) / (1024 ** 3)
    except ValueError:
        return None

def convert_size_to_bytes(filesize_str: Optional[str]) -> Optional[int]:
    """Convert a human-readable filesize string to bytes."""
    if not filesize_str:
        return None

    size_str = str(filesize_str).strip().upper()
    if not size_str:
        return None

    try:
        if size_str.endswith("GB"):
            return int(float(size_str[:-2].strip()) * (1024 ** 3))
        if size_str.endswith("MB"):
            return int(float(size_str[:-2].strip()) * (1024 ** 2))
        if size_str.endswith("TB"):
            return int(float(size_str[:-2].strip()) * (1024 ** 4))
        if size_str.endswith("KB"):
            return int(float(size_str[:-2].strip()) * 1024)
        if size_str.endswith("B"):
            return int(float(size_str[:-1].strip()))
        return int(float(size_str))
    except (ValueError, TypeError):
        return None

def ensure_filesize_within_limit(filesize_str: Optional[str], max_size_gb: float = DEFAULT_MAX_FILE_SIZE_GB) -> None:
    """Raise ValueError if filesize exceeds the configured maximum."""
    size_gb = parse_size_to_gb(filesize_str)
    if size_gb is None:
        return

    if size_gb > max_size_gb:
        raise ValueError(
            f"File size {filesize_str} exceeds maximum limit of {max_size_gb}GB"
        )
