"""HN Firebase API constants and URL helpers.

All outbound URLs are constructed here so that the base endpoint is a
single source of truth across Source, Expander, and Sink.
"""

from __future__ import annotations

_BASE_URL = "https://hacker-news.firebaseio.com/v0"
TOP_STORIES_URL = f"{_BASE_URL}/topstories.json"


def item_url(item_id: int) -> str:
    """Return the canonical HN item JSON endpoint for ``item_id``."""
    return f"{_BASE_URL}/item/{item_id}.json"
