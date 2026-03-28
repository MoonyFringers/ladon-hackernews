"""Tests for CLI argument validation."""

from __future__ import annotations

import argparse

import pytest

from ladon_hackernews.cli import (
    _validate_top,  # pyright: ignore[reportPrivateUsage]
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
