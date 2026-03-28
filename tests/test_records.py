"""Tests for domain records — StoryRecord and CommentRecord."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from ladon_hackernews.records import CommentRecord, StoryRecord

_NOW_UTC = datetime(2026, 3, 28, 12, 0, 0, tzinfo=timezone.utc)
_NOW_NAIVE = datetime(2026, 3, 28, 12, 0, 0)


class TestStoryRecord:
    def test_accepts_aware_datetime(self) -> None:
        record = StoryRecord(
            id=1,
            title="Test",
            url=None,
            by="user",
            score=10,
            time=_NOW_UTC,
            descendants=0,
            comment_ids=(),
        )
        assert record.time.tzinfo is not None

    def test_rejects_naive_datetime(self) -> None:
        with pytest.raises(ValueError, match="timezone-aware"):
            StoryRecord(
                id=1,
                title="Test",
                url=None,
                by="user",
                score=10,
                time=_NOW_NAIVE,
                descendants=0,
                comment_ids=(),
            )


class TestCommentRecord:
    def test_accepts_aware_datetime(self) -> None:
        record = CommentRecord(
            id=1,
            story_id=42,
            parent_id=42,
            by="user",
            text="hello",
            time=_NOW_UTC,
        )
        assert record.time.tzinfo is not None

    def test_rejects_naive_datetime(self) -> None:
        with pytest.raises(ValueError, match="timezone-aware"):
            CommentRecord(
                id=1,
                story_id=42,
                parent_id=42,
                by="user",
                text="hello",
                time=_NOW_NAIVE,
            )
