"""Domain records for the Hacker News adapter.

``StoryRecord`` is produced by ``HNExpander`` for each story node.
``CommentRecord`` is the leaf record produced by ``HNSink`` — the unit
persisted to storage by ``HNDuckDBRepository.write_leaf``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class StoryRecord:
    """Top-level HN story, produced by ``HNExpander``.

    ``url`` is ``None`` for Ask HN, Show HN, and job posts that have no
    external URL. ``descendants`` is the total comment count reported by
    the HN API (may differ from ``len(comment_ids)`` if comments are
    nested or deleted).

    ``time`` must be timezone-aware; the DuckDB schema stores it as
    ``TIMESTAMPTZ`` and naive datetimes would be rejected or misinterpreted.
    """

    id: int
    title: str
    url: str | None
    by: str
    score: int
    time: datetime
    descendants: int
    comment_ids: tuple[int, ...]

    def __post_init__(self) -> None:
        if self.time.tzinfo is None:
            raise ValueError(
                "StoryRecord.time must be timezone-aware (got naive datetime)"
            )


@dataclass(frozen=True)
class CommentRecord:
    """HN comment, produced by ``HNSink`` (the leaf record).

    ``text`` is raw HTML as returned by the HN API.  It may be an empty
    string for comments that were posted without body text, or for items
    where the ``text`` field was absent.  ``by`` is ``"[deleted]"`` for
    removed accounts.

    ``time`` must be timezone-aware; the DuckDB schema stores it as
    ``TIMESTAMPTZ`` and naive datetimes would be rejected or misinterpreted.
    """

    id: int
    story_id: int
    parent_id: int
    by: str
    text: str
    time: datetime

    def __post_init__(self) -> None:
        if self.time.tzinfo is None:
            raise ValueError(
                "CommentRecord.time must be timezone-aware (got naive datetime)"
            )
