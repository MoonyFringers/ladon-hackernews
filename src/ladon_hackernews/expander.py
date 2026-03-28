"""HNExpander — expands a story ref into its direct comment refs."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from ladon.networking.client import HttpClient
from ladon.plugins.errors import (
    ChildListUnavailableError,
    ExpansionNotReadyError,
)
from ladon.plugins.models import Expansion, Ref

from ._api import item_url
from .records import StoryRecord

logger = logging.getLogger(__name__)


class HNExpander:
    """Fetches a story item and returns its direct comment refs.

    The story ref's URL must point to the HN item JSON endpoint.
    ``story_id`` is forwarded to each comment ``Ref`` via ``raw`` so that
    ``HNSink`` can populate ``CommentRecord.story_id`` without a second
    lookup.
    """

    def expand(self, ref: object, client: HttpClient) -> Expansion:
        if not isinstance(ref, Ref):
            raise TypeError(f"expected Ref, got {type(ref).__name__}")
        result = client.get(ref.url)
        if not result.ok or result.value is None:
            raise ChildListUnavailableError(
                f"failed to fetch story {ref.url}: {result.error}"
            )
        item = json.loads(result.value)
        if item is None or item.get("dead") or item.get("deleted"):
            raise ExpansionNotReadyError(
                f"story {ref.url} is null, dead, or deleted"
            )

        story = StoryRecord(
            # "id" and "title" are load-bearing fields guaranteed by the HN
            # Firebase API for all story-type items; KeyError here indicates
            # an API contract violation, not a recoverable crawl error.
            id=int(item["id"]),
            title=str(item["title"]),
            url=str(item["url"]) if item.get("url") else None,
            by=str(item.get("by", "[deleted]")),
            score=int(item.get("score", 0)),
            time=datetime.fromtimestamp(
                int(item.get("time", 0)), tz=timezone.utc
            ),
            descendants=int(item.get("descendants", 0)),
            comment_ids=tuple(int(k) for k in item.get("kids", [])),
        )
        child_refs = [
            Ref(url=item_url(kid), raw={"story_id": story.id})
            for kid in story.comment_ids
        ]
        logger.debug(
            "expanded story %d → %d comment refs", story.id, len(child_refs)
        )
        return Expansion(record=story, child_refs=child_refs)
