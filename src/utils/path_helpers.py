"""Shared helpers for working with project paths and generated files."""

from __future__ import annotations

import glob
import os
from pathlib import Path
from typing import Iterable, Optional


def ensure_directories(*directories: str) -> None:
    """Create directories if they do not already exist."""
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)


def find_latest_file(
    patterns: Iterable[str],
    *,
    sort_key=os.path.getmtime,
) -> Optional[str]:
    """Return the newest file that matches any of the provided glob patterns."""
    candidates: list[str] = []
    for pattern in patterns:
        candidates.extend(glob.glob(pattern))

    files = [path for path in candidates if os.path.isfile(path)]
    if not files:
        return None

    return max(files, key=sort_key)
