# Contributing to ladon-hackernews

Thank you for your interest in contributing.

## Development setup

```bash
git clone https://github.com/MoonyFringers/ladon-hackernews
cd ladon-hackernews

# Install Ladon core (not yet on PyPI as ladon-crawl — use git source)
pip install git+https://github.com/MoonyFringers/ladon.git

# Install this package and dev dependencies.
# ladon-crawl is not yet on PyPI, so skip automatic dep resolution and
# install the remaining non-ladon deps explicitly.
pip install -e ".[dev]" --no-deps
# Version floors below must match [project.dependencies] in pyproject.toml.
pip install "duckdb>=1.0.0" "pytz>=2023.3"

# Enable pre-commit hooks
pre-commit install

# Run tests
pytest tests/ -v
```

## Standards

This project follows the same conventions as [Ladon core](https://github.com/MoonyFringers/ladon):

- **Formatting:** Black (`line-length = 80`)
- **Linting:** Ruff
- **Imports:** isort
- **Types:** pyright strict — all public functions must be fully annotated
- **Tests:** pytest — new behaviour must be covered
- **Commits:** [Conventional Commits](https://www.conventionalcommits.org/) (`feat:`, `fix:`, `docs:`, `chore:`, etc.)

Pre-commit enforces all of the above automatically on every commit.

## Opening a pull request

1. Fork the repository and create a branch from `main`.
2. Make your changes with tests.
3. Run `pytest tests/ -v` and confirm all tests pass.
4. Open a pull request — CI will run the full test suite.

## Reporting issues

Open an issue at https://github.com/MoonyFringers/ladon-hackernews/issues.
For issues with the Ladon framework itself, open them at
https://github.com/MoonyFringers/ladon/issues.
