"""Tests for HNSource."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest
from ladon.networking.errors import HttpClientError
from ladon.networking.types import Err, Ok
from ladon.plugins.errors import ChildListUnavailableError
from ladon.plugins.models import Ref

from ladon_hackernews._api import TOP_STORIES_URL
from ladon_hackernews.source import HNSource


def _make_client(
    response: bytes | None = None, *, ok: bool = True
) -> MagicMock:
    client = MagicMock()
    if ok and response is not None:
        client.get.return_value = Ok(response)
    else:
        client.get.return_value = Err(HttpClientError("network error"))
    return client


class TestHNSource:
    def test_discover_returns_refs(self) -> None:
        ids = list(range(1, 101))
        client = _make_client(json.dumps(ids).encode())
        source = HNSource(top=30)

        refs = source.discover(client)

        assert len(refs) == 30
        assert all(isinstance(r, Ref) for r in refs)

    def test_discover_calls_correct_url(self) -> None:
        client = _make_client(json.dumps([1, 2, 3]).encode())
        HNSource(top=3).discover(client)
        client.get.assert_called_once_with(TOP_STORIES_URL)

    def test_discover_ref_urls_contain_item_id(self) -> None:
        client = _make_client(json.dumps([42, 99]).encode())
        refs = HNSource(top=2).discover(client)
        urls = [r.url for r in refs if isinstance(r, Ref)]
        assert any("42" in u for u in urls)
        assert any("99" in u for u in urls)

    def test_discover_respects_top_limit(self) -> None:
        ids = list(range(1, 501))  # 500 IDs from HN
        client = _make_client(json.dumps(ids).encode())
        refs = HNSource(top=10).discover(client)
        assert len(refs) == 10

    def test_discover_raises_on_http_error(self) -> None:
        client = _make_client(ok=False)
        with pytest.raises(ChildListUnavailableError):
            HNSource().discover(client)
