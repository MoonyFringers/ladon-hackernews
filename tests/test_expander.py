"""Tests for HNExpander."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest
from ladon.networking.errors import HttpClientError
from ladon.networking.types import Err, Ok
from ladon.plugins.errors import (
    ChildListUnavailableError,
    ExpansionNotReadyError,
)
from ladon.plugins.models import Expansion, Ref

from ladon_hackernews.expander import HNExpander
from ladon_hackernews.records import StoryRecord

_STORY_URL = "https://hacker-news.firebaseio.com/v0/item/12345.json"

_STORY_JSON: dict[str, object] = {
    "id": 12345,
    "type": "story",
    "title": "Launch HN: Ladon — structured web crawler",
    "url": "https://example.com",
    "by": "tester",
    "score": 42,
    "time": 1700000000,
    "descendants": 3,
    "kids": [100, 101, 102],
}


def _client(payload: object = _STORY_JSON, *, ok: bool = True) -> MagicMock:
    client = MagicMock()
    if ok:
        client.get.return_value = Ok(json.dumps(payload).encode())
    else:
        client.get.return_value = Err(HttpClientError("err"))
    return client


class TestHNExpander:
    def test_expand_returns_expansion(self) -> None:
        ref = Ref(url=_STORY_URL)
        result = HNExpander().expand(ref, _client())
        assert isinstance(result, Expansion)

    def test_expand_produces_story_record(self) -> None:
        ref = Ref(url=_STORY_URL)
        result = HNExpander().expand(ref, _client())
        assert isinstance(result.record, StoryRecord)
        story = result.record
        assert story.id == 12345
        assert story.title == "Launch HN: Ladon — structured web crawler"
        assert story.url == "https://example.com"
        assert story.score == 42
        assert story.comment_ids == (100, 101, 102)

    def test_expand_child_refs_carry_story_id(self) -> None:
        ref = Ref(url=_STORY_URL)
        result = HNExpander().expand(ref, _client())
        for child in result.child_refs:
            assert isinstance(child, Ref)
            assert child.raw["story_id"] == 12345

    def test_expand_no_kids_returns_empty_child_refs(self) -> None:
        payload: dict[str, object] = {**_STORY_JSON, "kids": []}
        ref = Ref(url=_STORY_URL)
        result = HNExpander().expand(ref, _client(payload))
        assert result.child_refs == []

    def test_expand_none_url_for_ask_hn(self) -> None:
        payload = {k: v for k, v in _STORY_JSON.items() if k != "url"}
        ref = Ref(url=_STORY_URL)
        result = HNExpander().expand(ref, _client(payload))
        assert isinstance(result.record, StoryRecord)
        assert result.record.url is None

    def test_expand_raises_on_http_error(self) -> None:
        ref = Ref(url=_STORY_URL)
        with pytest.raises(ChildListUnavailableError):
            HNExpander().expand(ref, _client(ok=False))

    def test_expand_raises_on_dead_story(self) -> None:
        payload = {**_STORY_JSON, "dead": True}
        ref = Ref(url=_STORY_URL)
        with pytest.raises(ExpansionNotReadyError):
            HNExpander().expand(ref, _client(payload))

    def test_expand_raises_on_deleted_story(self) -> None:
        payload = {**_STORY_JSON, "deleted": True}
        ref = Ref(url=_STORY_URL)
        with pytest.raises(ExpansionNotReadyError):
            HNExpander().expand(ref, _client(payload))

    def test_expand_raises_on_null_item(self) -> None:
        ref = Ref(url=_STORY_URL)
        with pytest.raises(ExpansionNotReadyError):
            HNExpander().expand(ref, _client(None))

    def test_expand_raises_on_non_ref_input(self) -> None:
        with pytest.raises(TypeError, match="expected Ref"):
            HNExpander().expand("not-a-ref", MagicMock())
