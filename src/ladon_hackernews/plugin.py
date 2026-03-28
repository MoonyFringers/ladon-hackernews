"""HNPlugin — bundles HNSource, HNExpander, and HNSink into a CrawlPlugin.

``HNPlugin`` satisfies the ``CrawlPlugin`` protocol from Ladon by structural
subtyping — no explicit inheritance from a Ladon base class is required.
The properties return the concrete HN types, which are themselves structurally
compatible with the ``Source``, ``Expander``, and ``Sink`` protocols.
"""

from __future__ import annotations

from collections.abc import Sequence

from .expander import HNExpander
from .sink import HNSink
from .source import HNSource


class HNPlugin:
    """``CrawlPlugin`` implementation for Hacker News.

    Satisfies the Ladon ``CrawlPlugin`` protocol structurally — no Ladon
    base class is imported or subclassed. This is the intended pattern for
    third-party adapters.

    Args:
        top: Number of top stories to discover per run (default: 30).
    """

    def __init__(self, top: int = 30) -> None:
        self._source = HNSource(top=top)
        self._expander = HNExpander()
        self._sink = HNSink()

    @property
    def name(self) -> str:
        return "hackernews"

    @property
    def source(self) -> HNSource:
        return self._source

    @property
    def expanders(self) -> Sequence[HNExpander]:
        return [self._expander]

    @property
    def sink(self) -> HNSink:
        return self._sink
