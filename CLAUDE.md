# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working
with code in this repository.

## Project description

`ladon-hackernews` is a **Hacker News adapter** for the Ladon crawler
framework. It implements the Ladon SES protocol (Source / Expander / Sink)
to crawl HN stories and persist them to DuckDB.

Repository layout:

```text
src/ladon_hackernews/   ← adapter source
  plugin.py            ← CrawlPlugin composition
  source.py            ← HN top-stories Source
  expander.py          ← story page Expander
  sink.py              ← DuckDB Sink
  cli.py               ← ladon-hackernews CLI entry point
tests/                 ← pytest test suite
```

## Design reference

This adapter implements the conventions defined in the Ladon framework.
Before making structural changes, consult:
- [Ladon SES protocol ADR-004](https://github.com/MoonyFringers/ladon/blob/main/docs/decisions/adr-004-ses-protocol-design.md)
- [Ladon ADR index](https://github.com/MoonyFringers/ladon/blob/main/docs/decisions/index.md)

## Language policy

English only — source code, comments, commit messages, documentation.

## Common commands

```sh
# Install Ladon core (until published to PyPI)
pip install git+https://github.com/MoonyFringers/ladon.git

# Install this package and dev dependencies
pip install -e ".[dev]"

# Install git hooks (run once after cloning)
pre-commit install

# Run tests
pytest tests/ -v
```

## Dev commands

```sh
pytest tests/ -v              # run test suite
pytest tests/ -v --cov        # with coverage
black src/ tests/             # format
ruff check src/ tests/        # lint
isort src/ tests/             # sort imports
pyright                       # type-check (strict)
pre-commit run --all-files    # run all hooks at once
```

## Tests

- All tests must pass before committing.
- New behaviour must be covered by tests.
- Fix source code, not tests, when there is a mismatch.

## Commits and PRs

- Sign commits with `git commit -S` (GPG); wrap subjects to 72 characters
  and bodies to 80 columns
- Follow **Conventional Commits with scope**:
  `feat(crawler): ...`, `fix(db): ...`, `chore(deps): ...`

  Common scopes: `crawler`, `db`, `cli`, `tests`, `docs`, `deps`

- Include `Fixes: #<issue-number>` in the commit footer when resolving an
  issue
- Do not add `Co-Authored-By:` trailers for Claude
- Open a tracking issue before starting implementation work
- Every PR must target upstream `origin` and reference the tracking issue
