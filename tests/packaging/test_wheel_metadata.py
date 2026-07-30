from email.parser import Parser
import importlib.util
from pathlib import Path
from typing import Any, Callable

import pytest


_MODULE_PATH = Path(__file__).resolve().parents[2] / "scripts/verify_wheel_metadata.py"
_SPEC = importlib.util.spec_from_file_location("verify_wheel_metadata", _MODULE_PATH)
assert _SPEC is not None
assert _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)
_assert_metadata: Callable[[Any], None] = getattr(_MODULE, "_assert_metadata")


def _metadata(requirement: str):
    return Parser().parsestr(
        "\n".join(
            [
                "Metadata-Version: 2.1",
                "Name: kazeflow",
                "Provides-Extra: tui",
                f"Requires-Dist: {requirement}",
                "",
            ]
        )
    )


def test_metadata_accepts_rich_gated_only_by_tui_extra() -> None:
    _assert_metadata(_metadata('rich >= 14.1.0 ; ( extra == "tui" )'))


@pytest.mark.parametrize(
    "requirement",
    [
        "rich >= 14.1.0",
        'rich >= 14.1.0 ; extra == "tui" or python_version >= "3.10"',
        'rich >= 14.1.0 ; extra == "tui" and python_version >= "3.10"',
    ],
)
def test_metadata_rejects_nonexclusive_tui_markers(requirement: str) -> None:
    with pytest.raises(AssertionError, match="gated solely"):
        _assert_metadata(_metadata(requirement))
