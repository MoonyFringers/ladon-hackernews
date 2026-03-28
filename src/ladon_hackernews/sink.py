"""HNSink — consumes a comment ref and returns a CommentRecord."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from ladon.networking.client import HttpClient
from ladon.plugins.errors import LeafUnavailableError
from ladon.plugins.models import Ref

from .records import CommentRecord

logger = logging.getLogger(__name__)


class HNSink:
    """Fetches one HN comment item and returns a ``CommentRecord``.

    Raises ``LeafUnavailableError`` for deleted, dead, or unparseable items
    so the runner counts the failure without aborting the run.  ``story_id``
    is read from ``ref.raw["story_id"]`` injected by ``HNExpander``.
    """

    def consume(self, ref: object, client: HttpClient) -> CommentRecord:
        if not isinstance(ref, Ref):
            raise TypeError(f"expected Ref, got {type(ref).__name__}")
        if not ref.raw or "story_id" not in ref.raw:
            raise LeafUnavailableError(
                f"ref.raw missing story_id for {ref.url} — "
                "was this ref created by HNExpander?"
            )
        story_id = int(ref.raw["story_id"])  # type: ignore[arg-type]

        result = client.get(ref.url)
        if not result.ok or result.value is None:
            raise LeafUnavailableError(
                f"failed to fetch comment {ref.url}: {result.error}"
            )
        item = json.loads(result.value)
        if item is None or item.get("dead") or item.get("deleted"):
            raise LeafUnavailableError(
                f"comment {ref.url} is null, dead, or deleted"
            )

        return CommentRecord(
            id=int(item["id"]),
            story_id=story_id,
            parent_id=int(item.get("parent", 0)),
            by=str(item.get("by", "[deleted]")),
            text=str(item.get("text", "")),
            time=datetime.fromtimestamp(
                int(item.get("time", 0)), tz=timezone.utc
            ),
        )
