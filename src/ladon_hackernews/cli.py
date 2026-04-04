"""Command-line entry point for the Hacker News adapter.

Usage::

    ladon-hackernews --top 30 --out hn.db
    # or:
    python -m ladon_hackernews --top 30 --out hn.db

Each story from the HN top-stories list becomes one run: the orchestration
layer calls ``run_crawl`` once per story, writing comments to DuckDB and
recording the run audit trail via ``HNDuckDBRepository``.
"""

from __future__ import annotations

import argparse
import logging
import uuid
from datetime import datetime, timezone

from ladon.networking.client import HttpClient
from ladon.networking.config import HttpClientConfig
from ladon.persistence import RunRecord
from ladon.plugins.errors import ExpansionNotReadyError
from ladon.plugins.models import Ref
from ladon.runner import RunConfig, run_crawl

from . import __version__
from .plugin import HNPlugin
from .repository import HNDuckDBRepository

logger = logging.getLogger(__name__)


def _validate_top(value: str) -> int:
    """Argparse type validator: enforce 1 ≤ top ≤ 500."""
    try:
        n = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"--top must be an integer between 1 and 500 (got {value!r})"
        ) from None
    if not (1 <= n <= 500):
        raise argparse.ArgumentTypeError(
            f"--top must be an integer between 1 and 500 (got {n})"
        )
    return n


def _run(top: int, db_path: str) -> None:
    plugin = HNPlugin(top=top)
    config = RunConfig()
    client_config = HttpClientConfig(
        user_agent=(
            f"ladon-hackernews/{__version__} "
            "(https://github.com/MoonyFringers/ladon-hackernews)"
        ),
        min_request_interval_seconds=0.1,
    )

    with (
        HNDuckDBRepository(db_path) as repo,
        HttpClient(client_config) as client,
    ):
        stories = plugin.source.discover(client)
        logger.info("starting crawl: %d stories → %s", len(stories), db_path)

        for story_ref in stories:
            if not isinstance(story_ref, Ref):
                raise TypeError(
                    f"source returned unexpected type {type(story_ref).__name__}"
                )
            run_id = str(uuid.uuid4())
            run = RunRecord(
                run_id=run_id,
                plugin_name=plugin.name,
                top_ref=story_ref.url,
                started_at=datetime.now(tz=timezone.utc),
                status="running",
            )

            repo.record_run(run)

            try:
                result = run_crawl(
                    story_ref,
                    plugin,
                    client,
                    config,
                    # Default-argument capture ensures each lambda closes
                    # over its own run_id value, not the loop variable.
                    on_leaf=lambda rec, _, _id=run_id: repo.write_leaf(
                        rec, _id
                    ),
                )
                run.status = (
                    "partial"
                    if result.leaves_failed
                    or result.leaves_consumed > result.leaves_persisted
                    else "done"
                )
                run.leaves_consumed = result.leaves_consumed
                run.leaves_persisted = result.leaves_persisted
                run.leaves_failed = result.leaves_failed
                run.branch_errors = sum(
                    1 for e in result.errors if e.startswith("expander branch")
                )
                run.errors = result.errors
            except ExpansionNotReadyError:
                run.status = "not_ready"
                logger.warning(
                    "story %s not ready — will retry on next run",
                    story_ref.url,
                )
            except Exception as exc:
                run.status = "failed"
                run.errors = (str(exc),)
                logger.error("story %s failed: %s", story_ref.url, exc)
            finally:
                run.finished_at = datetime.now(tz=timezone.utc)
                repo.record_run(run)

        logger.info("crawl complete — db: %s", db_path)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    parser = argparse.ArgumentParser(
        description="Crawl Hacker News top stories into a DuckDB database."
    )
    parser.add_argument(
        "--top",
        type=_validate_top,
        default=30,
        help="Number of top stories to crawl (default: 30, range: 1–500).",
    )
    parser.add_argument(
        "--out",
        default="hn.db",
        metavar="PATH",
        help="Output DuckDB database path (default: hn.db).",
    )
    args = parser.parse_args()
    _run(top=args.top, db_path=args.out)
