"""Tests for HNPlugin — the CrawlPlugin entry point."""

from __future__ import annotations

from collections.abc import Sequence

from ladon_hackernews.expander import HNExpander
from ladon_hackernews.plugin import HNPlugin
from ladon_hackernews.sink import HNSink
from ladon_hackernews.source import HNSource


class TestHNPlugin:
    def test_name_is_hackernews(self) -> None:
        assert HNPlugin().name == "hackernews"

    def test_source_is_hn_source(self) -> None:
        assert isinstance(HNPlugin().source, HNSource)

    def test_expanders_contains_hn_expander(self) -> None:
        expanders = HNPlugin().expanders
        assert isinstance(expanders, Sequence)
        assert len(expanders) == 1
        assert isinstance(expanders[0], HNExpander)

    def test_sink_is_hn_sink(self) -> None:
        assert isinstance(HNPlugin().sink, HNSink)

    def test_top_forwarded_to_source(self) -> None:
        plugin = HNPlugin(top=10)
        # _top is accessed directly: there is no public read-back for this
        # value, and verifying the forwarding is the point of the test.
        assert plugin.source._top == 10  # type: ignore[attr-defined]

    def test_default_top_is_30(self) -> None:
        assert HNPlugin().source._top == 30  # type: ignore[attr-defined]
