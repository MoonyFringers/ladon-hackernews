"""Tests for HNDuckDBRepository."""

# pyright: reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false
# pyright: reportPrivateUsage=false
# Rationale: tests access _conn directly (no public read-back API) and duckdb's
# Python wrapper is only partially typed.
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Literal

import pytest
from ladon.persistence import RunRecord

from ladon_hackernews.records import CommentRecord
from ladon_hackernews.repository import HNDuckDBRepository, export_parquet

_NOW = datetime(2026, 3, 28, 12, 0, 0, tzinfo=timezone.utc)


def _make_repo() -> HNDuckDBRepository:
    return HNDuckDBRepository(":memory:")


def _make_comment(id: int = 1, story_id: int = 42) -> CommentRecord:
    return CommentRecord(
        id=id,
        story_id=story_id,
        parent_id=42,
        by="tester",
        text="<p>hello</p>",
        time=_NOW,
    )


def _make_run(
    run_id: str = "run-1",
    plugin_name: str = "hackernews",
    status: Literal[
        "running", "done", "failed", "not_ready", "partial"
    ] = "done",
    finished_at: datetime | None = _NOW,
) -> RunRecord:
    return RunRecord(
        run_id=run_id,
        plugin_name=plugin_name,
        top_ref="https://hacker-news.firebaseio.com/v0/item/42.json",
        started_at=_NOW,
        status=status,
        finished_at=finished_at,
        leaves_consumed=10,
        leaves_persisted=10,
        leaves_failed=0,
        branch_errors=0,
        errors=(),
    )


class TestHNDuckDBRepository:
    def test_write_leaf_inserts_comment(self) -> None:
        repo = _make_repo()
        repo.write_leaf(_make_comment(id=1), "run-1")
        # _conn is intentionally accessed here: write_leaf has no public
        # read-back method, so we verify state at the storage layer directly.
        row = repo._conn.execute("SELECT id FROM hn_comments").fetchone()
        assert row is not None
        assert row[0] == 1

    def test_write_leaf_upserts_on_duplicate_id(self) -> None:
        repo = _make_repo()
        repo.write_leaf(_make_comment(id=1), "run-1")
        repo.write_leaf(_make_comment(id=1), "run-2")
        count = repo._conn.execute(
            "SELECT COUNT(*) FROM hn_comments"
        ).fetchone()
        assert count is not None
        assert count[0] == 1

    def test_write_leaf_rejects_wrong_type(self) -> None:
        repo = _make_repo()
        with pytest.raises(TypeError):
            repo.write_leaf("not a comment", "run-1")

    def test_record_run_inserts_run(self) -> None:
        repo = _make_repo()
        run = _make_run(run_id="run-1")
        repo.record_run(run)
        result = repo.get_last_run("hackernews")
        assert result is not None
        assert result.run_id == "run-1"

    def test_record_run_upserts_on_duplicate_run_id(self) -> None:
        repo = _make_repo()
        repo.record_run(_make_run(status="running", finished_at=None))
        repo.record_run(_make_run(status="done"))
        result = repo.get_last_run("hackernews", status="done")
        assert result is not None
        assert result.status == "done"
        # Only one row: get_last_run for "running" must return nothing.
        assert repo.get_last_run("hackernews", status="running") is None

    def test_get_last_run_returns_most_recent_done(self) -> None:
        repo = _make_repo()
        repo.record_run(_make_run(run_id="run-1", finished_at=_NOW))
        repo.record_run(
            _make_run(run_id="run-2", finished_at=_NOW + timedelta(seconds=60))
        )
        result = repo.get_last_run("hackernews")
        assert result is not None
        assert result.run_id == "run-2"

    def test_get_last_run_filters_by_status(self) -> None:
        repo = _make_repo()
        repo.record_run(_make_run(run_id="run-failed", status="failed"))
        repo.record_run(_make_run(run_id="run-done", status="done"))
        result = repo.get_last_run("hackernews", status="done")
        assert result is not None
        assert result.run_id == "run-done"

    def test_get_last_run_status_none_returns_any(self) -> None:
        repo = _make_repo()
        repo.record_run(_make_run(run_id="run-failed", status="failed"))
        result = repo.get_last_run("hackernews", status=None)
        assert result is not None
        assert result.run_id == "run-failed"

    def test_get_last_run_returns_none_when_empty(self) -> None:
        repo = _make_repo()
        assert repo.get_last_run("hackernews") is None

    def test_get_last_run_returns_none_for_unknown_plugin(self) -> None:
        repo = _make_repo()
        repo.record_run(_make_run())
        assert repo.get_last_run("other-plugin") is None

    def test_context_manager_closes_connection(self) -> None:
        with HNDuckDBRepository(":memory:") as repo:
            repo.record_run(_make_run())
        # No assertion needed — close() must not raise.


class TestExportParquet:
    def test_export_creates_parquet_file(self, tmp_path: Path) -> None:
        db = str(tmp_path / "hn.db")
        out = str(tmp_path / "hn.parquet")
        with HNDuckDBRepository(db) as repo:
            repo.write_leaf(_make_comment(id=1), "run-1")
            repo.write_leaf(_make_comment(id=2), "run-1")
        export_parquet(db, out)
        assert Path(out).exists()
        assert Path(out).stat().st_size > 0

    def test_export_rejects_path_with_single_quote(
        self, tmp_path: Path
    ) -> None:
        db = str(tmp_path / "hn.db")
        with pytest.raises(ValueError, match="unsafe"):
            export_parquet(db, "bad'path.parquet")

    def test_export_rejects_path_with_null_byte(self, tmp_path: Path) -> None:
        db = str(tmp_path / "hn.db")
        with pytest.raises(ValueError, match="unsafe"):
            export_parquet(db, "bad\x00path.parquet")
