# Changelog

All notable changes to `ladon-hackernews` are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added

### Fixed

### Changed

---

## [0.1.0] — 2026-05-18

### Changed

- Bump `ladon-crawl` minimum dependency from `>=0.0.1` to `>=0.1.0` — reflects the actual minimum version that includes proxy support, Retry-After handling, and the stable `HttpClientConfig` API used by this adapter.
- Version alignment with `ladon-mimir`: both adapters now share `0.1.0` as their common baseline. See [versioning rationale](https://github.com/MoonyFringers/ladon/discussions) for details.

---

## [0.0.1] — 2026-04-17

First public release.

### Added

- `HNSource` — fetches the top-N story IDs from the Hacker News Firebase API.
- `HNExpander` — fetches story metadata (title, score, author, comment count, URL) and emits `StoryRef` leaves.
- `HNSink` — fetches full story details and writes `StoryRecord` to the repository.
- `HNPlugin` — `CrawlPlugin`-compatible bundle wiring source, expander, and sink.
- `HNDuckDBRepository` — DuckDB-backed persistence: `hn_stories` (upsert on `story_id`) and `ladon_runs` (run lifecycle tracking). Context manager support; `get_existing_story_ids()` for resume.
- `export_parquet(db_path, parquet_path) -> int` — bulk export of `hn_stories` to Parquet via DuckDB's native `COPY … TO … FORMAT PARQUET`.
- `ladon-hackernews` CLI entrypoint — `--limit`, `--out`, `--dry-run`, `--verbose`.
- Public models: `StoryRecord`, `StoryRef`.

[Unreleased]: https://github.com/MoonyFringers/ladon-hackernews/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/MoonyFringers/ladon-hackernews/compare/v0.0.1...v0.1.0
[0.0.1]: https://github.com/MoonyFringers/ladon-hackernews/releases/tag/v0.0.1
