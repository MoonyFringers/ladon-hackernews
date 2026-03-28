"""HNDuckDBRepository — DuckDB-backed Repository and RunAudit for Ladon.

Satisfies both ``Repository`` and ``RunAudit`` protocols structurally;
no inheritance from Ladon is required beyond importing ``RunRecord``.

Schema
------
``hn_comments`` — one row per leaf ``CommentRecord``, keyed on ``id``.
``ladon_runs``  — one row per run, keyed on ``run_id``; upserted twice
                  per run (start + finish) per the ADR-006 contract.
"""

# pyright: reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false
# pyright: reportUnknownArgumentType=false
# Rationale: duckdb's Python wrapper is only partially typed; all three
# suppressions above are confined to this file.
from __future__ import annotations

import json
import logging
from typing import Any, Literal, cast

import duckdb
from ladon.persistence import RunRecord

from .records import CommentRecord

logger = logging.getLogger(__name__)

_CREATE_COMMENTS = """
CREATE TABLE IF NOT EXISTS hn_comments (
    id          INTEGER PRIMARY KEY,
    story_id    INTEGER NOT NULL,
    parent_id   INTEGER NOT NULL,
    "by"        TEXT    NOT NULL,
    "text"      TEXT    NOT NULL,
    "time"      TIMESTAMPTZ NOT NULL,
    run_id      TEXT    NOT NULL
)
"""

_CREATE_RUNS = """
CREATE TABLE IF NOT EXISTS ladon_runs (
    run_id           TEXT PRIMARY KEY,
    plugin_name      TEXT    NOT NULL,
    top_ref          TEXT    NOT NULL,
    started_at       TIMESTAMPTZ NOT NULL,
    finished_at      TIMESTAMPTZ,
    status           TEXT    NOT NULL,
    leaves_fetched   INTEGER NOT NULL DEFAULT 0,
    leaves_persisted INTEGER NOT NULL DEFAULT 0,
    leaves_failed    INTEGER NOT NULL DEFAULT 0,
    branch_errors    INTEGER NOT NULL DEFAULT 0,
    errors           TEXT    NOT NULL DEFAULT '[]'
)
"""

_UPSERT_COMMENT = """
INSERT INTO hn_comments (id, story_id, parent_id, "by", "text", "time", run_id)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (id) DO UPDATE SET
    story_id  = excluded.story_id,
    parent_id = excluded.parent_id,
    "by"      = excluded."by",
    "text"    = excluded."text",
    "time"    = excluded."time",
    run_id    = excluded.run_id
"""

_UPSERT_RUN = """
INSERT INTO ladon_runs (
    run_id, plugin_name, top_ref, started_at, status,
    finished_at, leaves_fetched, leaves_persisted,
    leaves_failed, branch_errors, errors
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (run_id) DO UPDATE SET
    status           = excluded.status,
    finished_at      = excluded.finished_at,
    leaves_fetched   = excluded.leaves_fetched,
    leaves_persisted = excluded.leaves_persisted,
    leaves_failed    = excluded.leaves_failed,
    branch_errors    = excluded.branch_errors,
    errors           = excluded.errors
"""

_LAST_RUN_FILTERED = """
SELECT run_id, plugin_name, top_ref, started_at, status,
       finished_at, leaves_fetched, leaves_persisted,
       leaves_failed, branch_errors, errors
FROM ladon_runs
WHERE plugin_name = ? AND status = ?
ORDER BY COALESCE(finished_at, started_at) DESC
LIMIT 1
"""

_LAST_RUN_ANY = """
SELECT run_id, plugin_name, top_ref, started_at, status,
       finished_at, leaves_fetched, leaves_persisted,
       leaves_failed, branch_errors, errors
FROM ladon_runs
WHERE plugin_name = ?
ORDER BY COALESCE(finished_at, started_at) DESC
LIMIT 1
"""


def _row_to_run_record(row: tuple[Any, ...]) -> RunRecord:
    # DuckDB returns TIMESTAMPTZ columns as timezone-aware datetime objects.
    return RunRecord(
        run_id=str(row[0]),
        plugin_name=str(row[1]),
        top_ref=str(row[2]),
        started_at=row[3],
        status=cast(
            Literal["running", "done", "failed", "not_ready", "partial"],
            str(row[4]),
        ),
        finished_at=row[5],  # None for in-progress runs
        leaves_fetched=int(row[6]),
        leaves_persisted=int(row[7]),
        leaves_failed=int(row[8]),
        branch_errors=int(row[9]),
        errors=tuple(json.loads(str(row[10]))),
    )


class HNDuckDBRepository:
    """DuckDB-backed persistence for the Hacker News adapter.

    Implements ``Repository`` (``write_leaf``) and ``RunAudit``
    (``record_run``, ``get_last_run``) structurally — no Ladon base class
    is imported beyond ``RunRecord``.

    Args:
        db_path: Path to the DuckDB database file, or ``":memory:"`` for
                 an in-process database (useful in tests).
    """

    def __init__(self, db_path: str) -> None:
        self._conn: duckdb.DuckDBPyConnection = duckdb.connect(db_path)
        self._conn.execute(_CREATE_COMMENTS)
        self._conn.execute(_CREATE_RUNS)

    def write_leaf(self, record: object, run_id: str) -> None:
        """Upsert one ``CommentRecord`` into ``hn_comments``."""
        if not isinstance(record, CommentRecord):
            raise TypeError(
                f"HNDuckDBRepository.write_leaf expected CommentRecord, "
                f"got {type(record).__name__}"
            )
        self._conn.execute(
            _UPSERT_COMMENT,
            [
                record.id,
                record.story_id,
                record.parent_id,
                record.by,
                record.text,
                record.time,
                run_id,
            ],
        )

    def record_run(self, run: RunRecord) -> None:
        """Upsert a ``RunRecord`` into ``ladon_runs`` (called twice per run)."""
        self._conn.execute(
            _UPSERT_RUN,
            [
                run.run_id,
                run.plugin_name,
                run.top_ref,
                run.started_at,
                run.status,
                run.finished_at,
                run.leaves_fetched,
                run.leaves_persisted,
                run.leaves_failed,
                run.branch_errors,
                json.dumps(list(run.errors)),
            ],
        )

    def get_last_run(
        self, plugin_name: str, status: str | None = "done"
    ) -> RunRecord | None:
        """Return the most recent run for ``plugin_name``, or ``None``.

        Ordered by ``finished_at`` descending (``started_at`` when
        ``finished_at`` is ``None``). Pass ``status=None`` to return the
        most recent run regardless of outcome.
        """
        if status is not None:
            row = self._conn.execute(
                _LAST_RUN_FILTERED, [plugin_name, status]
            ).fetchone()
        else:
            row = self._conn.execute(_LAST_RUN_ANY, [plugin_name]).fetchone()

        if row is None:
            return None
        return _row_to_run_record(row)

    def close(self) -> None:
        """Close the underlying DuckDB connection."""
        self._conn.close()

    def __enter__(self) -> HNDuckDBRepository:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


def export_parquet(db_path: str, output_path: str) -> None:
    """Export ``hn_comments`` to a Parquet file for downstream pipelines.

    Example — LLM training pipeline::

        crawl → hn.db → export_parquet("hn.db", "hn.parquet") → training

    Args:
        db_path: Path to the source DuckDB database file.
        output_path: Destination path for the Parquet file. Must be a
            trusted value — it is interpolated into a SQL statement because
            DuckDB's COPY command does not support parameterized file paths.

    Raises:
        ValueError: If ``output_path`` contains characters that could
            escape the SQL string literal (``'`` or null byte).
    """
    if "'" in output_path or "\x00" in output_path:
        raise ValueError(
            f"output_path contains unsafe characters: {output_path!r}"
        )
    conn = duckdb.connect(db_path, read_only=True)
    try:
        conn.execute(f"COPY hn_comments TO '{output_path}' (FORMAT PARQUET)")
        logger.info("exported hn_comments → %s", output_path)
    finally:
        conn.close()
