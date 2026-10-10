"""The installed CLI must describe its own supported development workflow."""

from __future__ import annotations

from importlib import resources
from pathlib import Path
import shlex

import pytest

from kazeflow import cli


def test_instructions_are_bundled_and_do_not_load_an_entry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    def unexpected_entry_load(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("instructions must not load an application entry")

    monkeypatch.setattr(cli, "_validate_entry_syntax", unexpected_entry_load)
    monkeypatch.setattr(cli, "_load_entry_for_output", unexpected_entry_load)

    assert cli.main(["instructions"]) == cli.EXIT_SUCCESS
    captured = capsys.readouterr()
    bundled = (
        resources.files("kazeflow")
        .joinpath("development_instructions.md")
        .read_text(encoding="utf-8")
    )
    assert captured.out == bundled
    assert captured.err == ""
    assert bundled.startswith("# kazeflow Development Instructions\n")
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("extra", [["flow.py"], ["--format", "json"]])
def test_instructions_reject_unsupported_arguments(
    extra: list[str], capsys: pytest.CaptureFixture[str]
) -> None:
    assert cli.main(["instructions", *extra]) == cli.EXIT_USAGE
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "unrecognized arguments" in captured.err


def test_instructions_help_and_advertised_commands_match_parser(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert cli.main(["--help"]) == cli.EXIT_SUCCESS
    help_text = capsys.readouterr().out
    assert "instructions" in help_text
    assert "print bundled development instructions" in help_text

    guide = (
        resources.files("kazeflow")
        .joinpath("development_instructions.md")
        .read_text(encoding="utf-8")
    )
    parser = cli._parser()
    for line in guide.splitlines():
        if line.startswith("kazeflow "):
            parsed = parser.parse_args(shlex.split(line)[1:])
            assert parsed.command in {"assets", "partitions", "plan"}
    assert cli.main(["plan", "--help"]) == cli.EXIT_SUCCESS
    plan_help = capsys.readouterr().out
    for option in (
        "--target",
        "--partition-key",
        "--partition-range",
        "--empty-partitions",
    ):
        assert option in guide
        assert option in plan_help


def test_missing_bundled_resource_is_an_infrastructure_error(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def unavailable(_package: str) -> None:
        raise FileNotFoundError("development_instructions.md")

    monkeypatch.setattr(cli.resources, "files", unavailable)
    assert cli.main(["instructions"]) == cli.EXIT_INFRASTRUCTURE
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "bundled instructions unavailable" in captured.err
