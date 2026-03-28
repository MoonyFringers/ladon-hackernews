"""Hacker News adapter for the Ladon crawler framework."""

from importlib.metadata import PackageNotFoundError, version

from .plugin import HNPlugin
from .records import CommentRecord, StoryRecord
from .repository import HNDuckDBRepository, export_parquet

__all__ = [
    "HNPlugin",
    "HNDuckDBRepository",
    "CommentRecord",
    "StoryRecord",
    "export_parquet",
]

try:
    __version__ = version("ladon-hackernews")
except PackageNotFoundError:
    __version__ = "0.0.1"  # editable install without metadata
