"""Command-line entry point for the Hacker News adapter.

Usage::

    ladon-hackernews --top 30 --out hn.db
    ladon-hackernews --top 30 --out hn.db --verbose

Each story from the HN top-stories list becomes one run: the orchestration
layer calls ``run_crawl`` once per story, writing comments to DuckDB and
recording the run audit trail via ``HNDuckDBRepository``.

Output
------
Progress is written to stdout as one line per story::

    Crawling 30 stories → hn.db
    [ 1/30] "Show HN: …"  — 142 comments
    [ 2/30] "Ask HN: …"   — 87 comments  [partial: 3 errors]
       ↳ some comments were deleted or unavailable (run with --verbose for details)
    [ 3/30] https://news.ycombinator.com/item?id=…  — not ready
    Done — 30 stories · 1,234 comments · 2 not ready · 3 leaf errors
    Note: leaf errors are deleted or unavailable HN comments — this is normal. Run with --verbose for details.

Pass ``--verbose`` to also see DEBUG-level messages from the Ladon
framework (leaf-level warnings, HTTP timings, circuit-breaker state).
In default mode, ``ladon.*`` loggers are silenced at ERROR level; the
CLI already surfaces leaf-level problems as per-story status tags and
summary counts.
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
from .records import StoryRecord
from .repository import HNDuckDBRepository

logger = logging.getLogger(__name__)


def _story_label(ref: Ref, record: object) -> str:
    """Return a short display label for a story.

    Uses the story title when the expander produced a ``StoryRecord``
    (the normal case).  Falls back to the raw URL only if ``record`` is
    not a ``StoryRecord`` — which would indicate a plugin bug, not a
    normal not-ready or failed run (those paths never call this function).
    """
    if isinstance(record, StoryRecord):
        title = record.title
        # Titles longer than 72 chars are sliced to 69 and suffixed with
        # "…", giving 70 visible characters.  This keeps the progress line
        # readable on an 80-column terminal: prefix (≤9) + label (≤72) +
        # separator + comment count leaves enough room.
        if len(title) > 72:
            title = title[:69] + "…"
        return f'"{title}"'
    return ref.url


def _run(top: int, db_path: str, verbose: bool = False) -> None:
    """Crawl the top *top* HN stories and write comments to *db_path*.

    Opens a ``HNDuckDBRepository`` and an ``HttpClient``, fetches the story
    list, then calls ``run_crawl`` once per story.  Progress is printed to
    stdout as one line per story; a summary line is printed at the end.

    Each story can end in one of four states:

    - **done** / **partial** — ``run_crawl`` returned; leaf-level errors are
      counted and shown in the summary.
    - **not_ready** — ``run_crawl`` raised ``ExpansionNotReadyError``; the
      story is skipped silently (no leaf-error count).
    - **failed** — ``run_crawl`` raised any other exception; the error
      message is printed inline and counted in the summary.

    A ``TypeError`` raised by an unexpected ref type propagates immediately
    without updating the run record, as it indicates a plugin bug.

    When *verbose* is ``False`` (default), partial stories print an
    explanatory ``↳`` note and the summary prints a ``Note:`` line when
    leaf errors are present.  In verbose mode these hints are suppressed
    because the raw framework log messages already provide full detail.
    """
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
        total = len(stories)
        width = len(str(total))
        print(
            f"Crawling {total} {'story' if total == 1 else 'stories'} → {db_path}"
        )

        total_comments = 0
        total_leaf_errors = 0
        total_not_ready = 0
        total_failed = 0

        for i, story_ref in enumerate(stories, 1):
            if not isinstance(story_ref, Ref):
                raise TypeError(
                    f"source returned unexpected type "
                    f"{type(story_ref).__name__}"
                )
            run_id = str(uuid.uuid4())
            run = RunRecord(
                run_id=run_id,
                plugin_name=plugin.name,
                top_ref=story_ref.url,
                started_at=datetime.now(tz=timezone.utc),
                status="running",
            )

            prefix = f"[{i:{width}d}/{total}]"
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
                run.branch_errors = sum(
                    1 for e in result.errors if e.startswith("expander branch")
                )
                run.status = (
                    "partial"
                    if result.leaves_failed
                    or result.leaves_consumed > result.leaves_persisted
                    or run.branch_errors
                    else "done"
                )
                run.leaves_consumed = result.leaves_consumed
                run.leaves_persisted = result.leaves_persisted
                run.leaves_failed = result.leaves_failed
                run.errors = result.errors

                label = _story_label(story_ref, result.record)
                comments = result.leaves_consumed
                total_comments += comments

                # Leaf-level errors: consume failures + callback failures
                # + branch expansion failures.
                n_errors = (
                    result.leaves_failed
                    + (result.leaves_consumed - result.leaves_persisted)
                    + run.branch_errors
                )
                total_leaf_errors += n_errors
                status_tag = (
                    f"  [partial: {n_errors} error"
                    f"{'s' if n_errors != 1 else ''}]"
                    if run.status == "partial"
                    else ""
                )
                print(
                    f"{prefix} {label}  — {comments} comment"
                    f"{'s' if comments != 1 else ''}{status_tag}"
                )
                if run.status == "partial" and not verbose:
                    print(
                        " " * (width * 2 + 3)
                        + "↳ some comments were deleted or unavailable"
                        " (run with --verbose for details)"
                    )

            except ExpansionNotReadyError:
                run.status = "not_ready"
                total_not_ready += 1
                logger.debug(
                    "story not ready — will retry on next run",
                    extra={"ref": story_ref.url},
                )
                print(f"{prefix} {story_ref.url}  — not ready")

            except Exception as exc:
                run.status = "failed"
                run.errors = (str(exc),)
                total_failed += 1
                logger.debug(
                    "story run failed",
                    extra={"ref": story_ref.url, "error": str(exc)},
                )
                print(f"{prefix} {story_ref.url}  — failed: {exc}")

            finally:
                run.finished_at = datetime.now(tz=timezone.utc)
                repo.record_run(run)

        parts = [
            f"{total} {'story' if total == 1 else 'stories'}",
            f"{total_comments:,} comment{'s' if total_comments != 1 else ''}",
        ]
        if total_not_ready:
            parts.append(f"{total_not_ready} not ready")
        if total_failed:
            parts.append(f"{total_failed} failed")
        if total_leaf_errors:
            parts.append(
                f"{total_leaf_errors} leaf error"
                f"{'s' if total_leaf_errors != 1 else ''}"
            )
        print("\nDone — " + " · ".join(parts))
        if total_leaf_errors and not verbose:
            print(
                "Note: leaf errors are deleted or unavailable HN comments"
                " — this is normal. Run with --verbose for details."
            )


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


def _build_parser() -> argparse.ArgumentParser:
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
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        default=False,
        help=(
            "Enable verbose output: show DEBUG-level messages from the "
            "Ladon framework (leaf warnings, HTTP timings, etc.)."
        ),
    )
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    # In default mode suppress framework-level noise (leaf unavailable,
    # expander branch warnings, HTTP timings).  --verbose exposes everything.
    log_level = logging.DEBUG if args.verbose else logging.WARNING
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    if not args.verbose:
        # Silence ladon.* loggers (leaf unavailable, branch failed, HTTP
        # timings) in normal mode.  The CLI already surfaces these as
        # per-story status tags and summary counts.
        logging.getLogger("ladon").setLevel(logging.ERROR)

    _run(top=args.top, db_path=args.out, verbose=args.verbose)
