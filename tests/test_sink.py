"""Tests for HNSink."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest
from ladon.networking.errors import HttpClientError
from ladon.networking.types import Err, Ok
from ladon.plugins.errors import LeafUnavailableError
from ladon.plugins.models import Ref

from ladon_hackernews.records import CommentRecord
from ladon_hackernews.sink import HNSink

_COMMENT_URL = "https://hacker-news.firebaseio.com/v0/item/100.json"
_STORY_ID = 12345

_COMMENT_JSON: dict[str, object] = {
    "id": 100,
    "type": "comment",
    "by": "commenter",
    "text": "<p>Great framework!</p>",
    "time": 1700000100,
    "parent": _STORY_ID,
}


def _ref(raw: dict[str, object] | None = None) -> Ref:
    return Ref(url=_COMMENT_URL, raw=raw or {"story_id": _STORY_ID})


def _client(payload: object = _COMMENT_JSON, *, ok: bool = True) -> MagicMock:
    client = MagicMock()
    if ok:
        client.get.return_value = Ok(json.dumps(payload).encode())
    else:
        client.get.return_value = Err(HttpClientError("err"))
    return client


class TestHNSink:
    def test_consume_returns_comment_record(self) -> None:
        record = HNSink().consume(_ref(), _client())
        assert isinstance(record, CommentRecord)

    def test_consume_populates_fields(self) -> None:
        record = HNSink().consume(_ref(), _client())
        assert isinstance(record, CommentRecord)
        assert record.id == 100
        assert record.story_id == _STORY_ID
        assert record.parent_id == _STORY_ID
        assert record.by == "commenter"
        assert record.text == "<p>Great framework!</p>"

    def test_consume_story_id_from_ref_raw(self) -> None:
        record = HNSink().consume(_ref({"story_id": 999}), _client())
        assert isinstance(record, CommentRecord)
        assert record.story_id == 999

    def test_consume_missing_text_defaults_to_empty(self) -> None:
        payload = {k: v for k, v in _COMMENT_JSON.items() if k != "text"}
        record = HNSink().consume(_ref(), _client(payload))
        assert isinstance(record, CommentRecord)
        assert record.text == ""

    def test_consume_raises_on_http_error(self) -> None:
        with pytest.raises(LeafUnavailableError):
            HNSink().consume(_ref(), _client(ok=False))

    def test_consume_raises_on_dead_comment(self) -> None:
        payload = {**_COMMENT_JSON, "dead": True}
        with pytest.raises(LeafUnavailableError):
            HNSink().consume(_ref(), _client(payload))

    def test_consume_raises_on_deleted_comment(self) -> None:
        payload = {**_COMMENT_JSON, "deleted": True}
        with pytest.raises(LeafUnavailableError):
            HNSink().consume(_ref(), _client(payload))

    def test_consume_raises_on_null_item(self) -> None:
        with pytest.raises(LeafUnavailableError):
            HNSink().consume(_ref(), _client(None))

    def test_consume_raises_when_ref_raw_is_none(self) -> None:
        ref = Ref(url=_COMMENT_URL, raw=None)  # type: ignore[arg-type]
        with pytest.raises(LeafUnavailableError, match="story_id"):
            HNSink().consume(ref, _client())

    def test_consume_raises_when_ref_raw_missing_story_id(self) -> None:
        ref = Ref(url=_COMMENT_URL, raw={"other_key": 99})
        with pytest.raises(LeafUnavailableError, match="story_id"):
            HNSink().consume(ref, _client())

    def test_consume_raises_on_non_ref_input(self) -> None:
        with pytest.raises(TypeError, match="expected Ref"):
            HNSink().consume("not-a-ref", MagicMock())
