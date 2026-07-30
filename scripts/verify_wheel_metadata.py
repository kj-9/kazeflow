#!/usr/bin/env python3
"""Assert the base wheel only declares the optional Rich TUI dependency."""

from __future__ import annotations

import argparse
from email.parser import BytesParser
from pathlib import Path
import re
import zipfile


_REQUIREMENT_NAME = re.compile(r"\s*([A-Za-z0-9_.-]+)")
_TUI_MARKER = re.compile(
    r"^\s*\(*\s*extra\s*==\s*(['\"])tui\1\s*\)*\s*$",
    re.IGNORECASE,
)


def _resolve_wheel(path: Path) -> Path:
    if path.is_file():
        return path
    if not path.is_dir():
        raise FileNotFoundError(path)

    wheels = sorted(path.glob("kazeflow-*.whl"))
    if len(wheels) != 1:
        raise AssertionError(
            f"expected exactly one kazeflow wheel in {path}, found {wheels}"
        )
    return wheels[0]


def _metadata_from_wheel(wheel: Path):
    with zipfile.ZipFile(wheel) as archive:
        metadata_paths = [
            path for path in archive.namelist() if path.endswith(".dist-info/METADATA")
        ]
        if len(metadata_paths) != 1:
            raise AssertionError(
                f"expected one METADATA file in {wheel}, found {metadata_paths}"
            )
        return BytesParser().parsebytes(archive.read(metadata_paths[0]))


def _assert_metadata(metadata: object) -> None:
    get_all = getattr(metadata, "get_all")
    extras = get_all("Provides-Extra", [])
    if extras != ["tui"]:
        raise AssertionError(f"expected only the tui extra, found {extras}")

    requirements = get_all("Requires-Dist", [])
    requirement_names: list[str] = []
    for requirement in requirements:
        name_match = _REQUIREMENT_NAME.match(requirement)
        if name_match is None:
            raise AssertionError(f"could not parse requirement: {requirement!r}")
        name = name_match.group(1).lower()
        requirement_names.append(name)
        if name == "netext":
            raise AssertionError("netext must not be present in wheel metadata")
        marker = requirement.partition(";")[2]
        if not _TUI_MARKER.fullmatch(marker):
            raise AssertionError(
                "base wheel dependencies must be gated solely by the tui extra: "
                f"{requirement}"
            )

    if requirement_names != ["rich"]:
        raise AssertionError(
            "expected Rich to be the only optional runtime dependency, found "
            f"{requirement_names}"
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("wheel", type=Path)
    args = parser.parse_args()

    _assert_metadata(_metadata_from_wheel(_resolve_wheel(args.wheel)))


if __name__ == "__main__":
    main()
