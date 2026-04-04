"""Tests for CLI argument parsing and validation."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from ladon.plugins.errors import ExpansionNotReadyError
from ladon.plugins.models import Ref
from ladon.runner import RunResult

from ladon_hackernews.cli import (  # pyright: ignore[reportPrivateUsage]
    _build_parser,
    _run,
    _story_label,
    _validate_top,
    main,
)
from ladon_hackernews.records import StoryRecord

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_REF_1 = Ref(url="https://news.ycombinator.com/item?id=1")
_REF_2 = Ref(url="https://news.ycombinator.com/item?id=2")

_STORY_RECORD = StoryRecord(
    id=1,
    title="Show HN: a test story",
    url=None,
    by="user",
    score=42,
    time=datetime(2024, 1, 1, tzinfo=timezone.utc),
    descendants=3,
    comment_ids=(10, 11, 12),
)


def _make_result(
    consumed: int = 3,
    persisted: int = 3,
    failed: int = 0,
    errors: tuple[str, ...] = (),
    record: object = _STORY_RECORD,
) -> RunResult:
    return RunResult(
        record=record,
        leaves_consumed=consumed,
        leaves_persisted=persisted,
        leaves_failed=failed,
        errors=errors,
    )


class TestValidateTop:
    def test_valid_minimum(self) -> None:
        assert _validate_top("1") == 1

    def test_valid_default(self) -> None:
        assert _validate_top("30") == 30

    def test_valid_maximum(self) -> None:
        assert _validate_top("500") == 500

    def test_rejects_zero(self) -> None:
        with pytest.raises(
            argparse.ArgumentTypeError, match="integer between 1 and 500"
        ):
            _validate_top("0")

    def test_rejects_negative(self) -> None:
        with pytest.raises(
            argparse.ArgumentTypeError, match="integer between 1 and 500"
        ):
            _validate_top("-1")

    def test_rejects_above_maximum(self) -> None:
        with pytest.raises(
            argparse.ArgumentTypeError, match="integer between 1 and 500"
        ):
            _validate_top("501")

    def test_rejects_non_integer(self) -> None:
        with pytest.raises(argparse.ArgumentTypeError, match="integer"):
            _validate_top("abc")


class TestBuildParser:
    def test_defaults(self) -> None:
        parser = _build_parser()
        args = parser.parse_args([])
        assert args.top == 30
        assert args.out == "hn.db"
        assert args.verbose is False

    def test_verbose_long(self) -> None:
        args = _build_parser().parse_args(["--verbose"])
        assert args.verbose is True

    def test_verbose_short(self) -> None:
        args = _build_parser().parse_args(["-v"])
        assert args.verbose is True

    def test_top_and_out(self) -> None:
        args = _build_parser().parse_args(["--top", "10", "--out", "x.db"])
        assert args.top == 10
        assert args.out == "x.db"


class TestStoryLabel:
    def test_uses_story_title(self) -> None:
        ref = Ref(url="https://news.ycombinator.com/item?id=1")
        record = StoryRecord(
            id=1,
            title="Show HN: something cool",
            url="https://example.com",
            by="user",
            score=42,
            time=datetime(2024, 1, 1, tzinfo=timezone.utc),
            descendants=10,
            comment_ids=(2, 3),
        )
        assert _story_label(ref, record) == '"Show HN: something cool"'

    def test_does_not_truncate_title_at_boundary(self) -> None:
        # Exactly 72 chars: must pass through unchanged.
        ref = Ref(url="https://news.ycombinator.com/item?id=1")
        title_72 = "A" * 72
        record = StoryRecord(
            id=1,
            title=title_72,
            url=None,
            by="user",
            score=1,
            time=datetime(2024, 1, 1, tzinfo=timezone.utc),
            descendants=0,
            comment_ids=(),
        )
        label = _story_label(ref, record)
        assert label == f'"{title_72}"'
        assert "…" not in label

    def test_truncates_title_one_over_boundary(self) -> None:
        # 73 chars: one over the limit, must be truncated to 69 + "…".
        ref = Ref(url="https://news.ycombinator.com/item?id=1")
        record = StoryRecord(
            id=1,
            title="A" * 73,
            url=None,
            by="user",
            score=1,
            time=datetime(2024, 1, 1, tzinfo=timezone.utc),
            descendants=0,
            comment_ids=(),
        )
        label = _story_label(ref, record)
        assert label.endswith('…"')
        assert label == '"' + "A" * 69 + '…"'

    def test_truncates_long_title(self) -> None:
        ref = Ref(url="https://news.ycombinator.com/item?id=1")
        long_title = "A" * 80
        record = StoryRecord(
            id=1,
            title=long_title,
            url=None,
            by="user",
            score=1,
            time=datetime(2024, 1, 1, tzinfo=timezone.utc),
            descendants=0,
            comment_ids=(),
        )
        label = _story_label(ref, record)
        assert label.endswith('…"')
        # 69 visible chars + "…" + two quote chars = 72 total
        assert len(label) == 72

    def test_falls_back_to_url_for_non_story_record(self) -> None:
        ref = Ref(url="https://news.ycombinator.com/item?id=99")
        label = _story_label(ref, object())
        assert label == "https://news.ycombinator.com/item?id=99"


# ---------------------------------------------------------------------------
# _run() — integration of progress output and summary logic
# ---------------------------------------------------------------------------


@patch("ladon_hackernews.cli.run_crawl")
@patch("ladon_hackernews.cli.HttpClient")
@patch("ladon_hackernews.cli.HNDuckDBRepository")
@patch("ladon_hackernews.cli.HNPlugin")
class TestRunOutput:
    """Test progress lines and summary output from _run().

    All network and DB calls are mocked; only the print() side-effects
    (captured via capsys) are asserted.
    """

    def _setup(
        self,
        mock_plugin_cls: MagicMock,
        mock_repo_cls: MagicMock,
        mock_client_cls: MagicMock,
        refs: list[Ref],
    ) -> MagicMock:
        """Wire up mocks and return the repo mock for per-test assertions."""
        mock_plugin = mock_plugin_cls.return_value
        mock_plugin.name = "hackernews"
        mock_plugin.source.discover.return_value = refs
        # Context-manager support for HNDuckDBRepository and HttpClient.
        return mock_repo_cls.return_value.__enter__.return_value

    def test_all_stories_succeed_clean_summary(
        self,
        mock_plugin_cls: MagicMock,
        mock_repo_cls: MagicMock,
        mock_client_cls: MagicMock,
        mock_run_crawl: MagicMock,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        self._setup(
            mock_plugin_cls, mock_repo_cls, mock_client_cls, [_REF_1, _REF_2]
        )
        mock_run_crawl.return_value = _make_result(consumed=3, persisted=3)

        _run(top=2, db_path="out.db")

        out = capsys.readouterr().out
        assert "Crawling 2 stories → out.db" in out
        assert "[1/2]" in out
        assert "[2/2]" in out
        assert '"Show HN: a test story"' in out
        assert "3 comments" in out
        # Clean run: no error buckets in summary.
        assert "Done — 2 stories · 6 comments" in out
        assert "leaf error" not in out
        assert "not ready" not in out
        assert "failed" not in out

    def test_partial_story_shows_status_tag_and_leaf_errors(
        self,
        mock_plugin_cls: MagicMock,
        mock_repo_cls: MagicMock,
        mock_client_cls: MagicMock,
        mock_run_crawl: MagicMock,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        self._setup(mock_plugin_cls, mock_repo_cls, mock_client_cls, [_REF_1])
        mock_run_crawl.return_value = _make_result(
            consumed=3,
            persisted=3,
            failed=2,
            errors=(
                "ref[0] consume failed: gone",
                "ref[1] consume failed: gone",
            ),
        )

        _run(top=1, db_path="out.db")

        out = capsys.readouterr().out
        assert "[partial: 2 errors]" in out
        assert "2 leaf errors" in out

    def test_not_ready_story_prints_url_and_increments_counter(
        self,
        mock_plugin_cls: MagicMock,
        mock_repo_cls: MagicMock,
        mock_client_cls: MagicMock,
        mock_run_crawl: MagicMock,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        self._setup(mock_plugin_cls, mock_repo_cls, mock_client_cls, [_REF_1])
        mock_run_crawl.side_effect = ExpansionNotReadyError("not yet")

        _run(top=1, db_path="out.db")

        out = capsys.readouterr().out
        assert _REF_1.url in out
        assert "not ready" in out
        assert "1 not ready" in out
        assert "leaf error" not in out

    def test_failed_story_prints_url_error_and_increments_counter(
        self,
        mock_plugin_cls: MagicMock,
        mock_repo_cls: MagicMock,
        mock_client_cls: MagicMock,
        mock_run_crawl: MagicMock,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        self._setup(mock_plugin_cls, mock_repo_cls, mock_client_cls, [_REF_1])
        mock_run_crawl.side_effect = RuntimeError("connection reset")

        _run(top=1, db_path="out.db")

        out = capsys.readouterr().out
        assert _REF_1.url in out
        assert "failed: connection reset" in out
        assert "1 failed" in out

    def test_mixed_run_all_summary_buckets(
        self,
        mock_plugin_cls: MagicMock,
        mock_repo_cls: MagicMock,
        mock_client_cls: MagicMock,
        mock_run_crawl: MagicMock,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        refs = [
            _REF_1,
            _REF_2,
            Ref(url="https://news.ycombinator.com/item?id=3"),
        ]
        self._setup(mock_plugin_cls, mock_repo_cls, mock_client_cls, refs)
        mock_run_crawl.side_effect = [
            _make_result(
                consumed=3,
                persisted=3,
                failed=1,
                errors=("ref[0] consume failed: x",),
            ),
            ExpansionNotReadyError("not yet"),
            RuntimeError("boom"),
        ]

        _run(top=3, db_path="out.db")

        out = capsys.readouterr().out
        assert "3 stories" in out
        assert "1 not ready" in out
        assert "1 failed" in out
        assert "1 leaf error" in out

    def test_single_error_uses_singular_form(
        self,
        mock_plugin_cls: MagicMock,
        mock_repo_cls: MagicMock,
        mock_client_cls: MagicMock,
        mock_run_crawl: MagicMock,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        self._setup(mock_plugin_cls, mock_repo_cls, mock_client_cls, [_REF_1])
        mock_run_crawl.return_value = _make_result(
            consumed=1,
            persisted=1,
            failed=1,
            errors=("ref[0] consume failed: x",),
        )

        _run(top=1, db_path="out.db")

        out = capsys.readouterr().out
        assert "[partial: 1 error]" in out
        assert "1 leaf error" in out
        assert "leaf errors" not in out

    def test_single_story_uses_singular_form_in_header_and_summary(
        self,
        mock_plugin_cls: MagicMock,
        mock_repo_cls: MagicMock,
        mock_client_cls: MagicMock,
        mock_run_crawl: MagicMock,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        self._setup(mock_plugin_cls, mock_repo_cls, mock_client_cls, [_REF_1])
        mock_run_crawl.return_value = _make_result(consumed=3, persisted=3)

        _run(top=1, db_path="out.db")

        out = capsys.readouterr().out
        assert "Crawling 1 story →" in out
        assert "Done — 1 story ·" in out
        assert "1 stories" not in out

    def test_single_comment_uses_singular_form_in_progress_line(
        self,
        mock_plugin_cls: MagicMock,
        mock_repo_cls: MagicMock,
        mock_client_cls: MagicMock,
        mock_run_crawl: MagicMock,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        self._setup(mock_plugin_cls, mock_repo_cls, mock_client_cls, [_REF_1])
        mock_run_crawl.return_value = _make_result(consumed=1, persisted=1)

        _run(top=1, db_path="out.db")

        out = capsys.readouterr().out
        assert "— 1 comment" in out
        assert "1 comments" not in out

    def test_zero_stories_prints_clean_summary(
        self,
        mock_plugin_cls: MagicMock,
        mock_repo_cls: MagicMock,
        mock_client_cls: MagicMock,
        mock_run_crawl: MagicMock,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        self._setup(mock_plugin_cls, mock_repo_cls, mock_client_cls, [])

        _run(top=0, db_path="out.db")

        out = capsys.readouterr().out
        assert "Crawling 0 stories → out.db" in out
        assert "Done — 0 stories · 0 comments" in out
        mock_run_crawl.assert_not_called()

    def test_invalid_ref_type_raises_type_error(
        self,
        mock_plugin_cls: MagicMock,
        mock_repo_cls: MagicMock,
        mock_client_cls: MagicMock,
        mock_run_crawl: MagicMock,
    ) -> None:
        # discover() returning a non-Ref is a plugin bug; TypeError must
        # propagate without writing any run record.
        self._setup(mock_plugin_cls, mock_repo_cls, mock_client_cls, ["not-a-ref"])  # type: ignore[list-item]
        mock_repo = mock_repo_cls.return_value.__enter__.return_value

        with pytest.raises(TypeError, match="unexpected type str"):
            _run(top=1, db_path="out.db")

        # No run record should have been written for the invalid ref.
        mock_repo.record_run.assert_not_called()


# ---------------------------------------------------------------------------
# main() — argument dispatch and logging setup
# ---------------------------------------------------------------------------


class TestMain:
    @patch("ladon_hackernews.cli._run")
    def test_default_args_call_run_with_defaults(
        self, mock_run: MagicMock
    ) -> None:
        with patch.object(sys, "argv", ["ladon-hackernews"]):
            main()
        mock_run.assert_called_once_with(top=30, db_path="hn.db")

    @patch("ladon_hackernews.cli._run")
    def test_explicit_args_forwarded_to_run(self, mock_run: MagicMock) -> None:
        with patch.object(
            sys, "argv", ["ladon-hackernews", "--top", "5", "--out", "x.db"]
        ):
            main()
        mock_run.assert_called_once_with(top=5, db_path="x.db")

    @patch("ladon_hackernews.cli._run")
    @patch("logging.basicConfig")
    def test_default_log_level_is_warning(
        self, mock_basicconfig: MagicMock, mock_run: MagicMock
    ) -> None:
        with patch.object(sys, "argv", ["ladon-hackernews"]):
            main()
        mock_basicconfig.assert_called_once()
        assert mock_basicconfig.call_args.kwargs["level"] == logging.WARNING

    @patch("ladon_hackernews.cli._run")
    @patch("logging.basicConfig")
    def test_verbose_flag_sets_debug_level(
        self, mock_basicconfig: MagicMock, mock_run: MagicMock
    ) -> None:
        with patch.object(sys, "argv", ["ladon-hackernews", "--verbose"]):
            main()
        mock_basicconfig.assert_called_once()
        assert mock_basicconfig.call_args.kwargs["level"] == logging.DEBUG
