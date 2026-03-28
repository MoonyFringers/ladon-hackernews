"""HNSource — discovers top Hacker News story refs."""

from __future__ import annotations

import json
import logging
from collections.abc import Sequence

from ladon.networking.client import HttpClient
from ladon.plugins.errors import ChildListUnavailableError
from ladon.plugins.models import Ref

from ._api import TOP_STORIES_URL, item_url

logger = logging.getLogger(__name__)


class HNSource:
    """Discovers the top ``top`` stories from Hacker News.

    Args:
        top: Maximum number of stories to return (default: 30, max: 500).
    """

    def __init__(self, top: int = 30) -> None:
        self._top = top

    def discover(self, client: HttpClient) -> Sequence[object]:
        """Fetch the HN top-stories list and return refs for the first ``top``."""
        result = client.get(TOP_STORIES_URL)
        if not result.ok or result.value is None:
            raise ChildListUnavailableError(
                f"failed to fetch top stories: {result.error}"
            )
        ids: list[int] = json.loads(result.value)[: self._top]
        logger.info("discovered %d stories", len(ids))
        return [Ref(url=item_url(i)) for i in ids]
