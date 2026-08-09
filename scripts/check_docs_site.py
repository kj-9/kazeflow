#!/usr/bin/env python3
"""Validate the small static documentation site without third-party tooling."""

from __future__ import annotations

from html.parser import HTMLParser
from pathlib import Path
import sys


REQUIRED_PAGES = {
    "index.html",
    "getting-started.html",
    "cli.html",
    "partitions.html",
    "results.html",
}
REQUIRED_NAVIGATION = REQUIRED_PAGES
REQUIRED_INDEX_TEXT = (
    "kazeflow",
    "Run your first flow",
    "Trust boundary.",
)
REQUIRED_TRANSCRIPT_TEXT = {
    "getting-started.html": ("Planned run:", "Proceed? [y/N] y", "Run result:"),
    "cli.html": ("flowchart LR", 'task_1["publish (target)"]'),
    "results.html": ("Stored runs:", "Portable record:"),
}
REQUIRED_REFERENCE_TEXT = {
    "getting-started.html": (
        "FlowPlan",
        "RunResult",
        "Python module loading can execute top-level code",
    ),
    "cli.html": (
        "Entry forms and asset discovery",
        "Exit statuses",
        "--max-concurrency",
        "--store PATH",
    ),
    "partitions.html": (
        "DatePartitionDef",
        "--partition-key",
        "Falsey Python keys",
    ),
    "results.html": (
        "Direct Python storage API",
        "SQLiteRunStore",
        "Raw partition-key values",
    ),
}
FORBIDDEN_REPOSITORY_GUIDES = (
    "docs/cli.md",
    "docs/reviewable-flows.md",
    "docs/sqlite-run-store.md",
)


class _LinkCollector(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []
        self.has_title = False
        self.has_main = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "title":
            self.has_title = True
        if tag == "main":
            self.has_main = True
        if tag not in {"a", "link"}:
            return
        for name, value in attrs:
            if name == "href" and value is not None:
                self.links.append(value)


def _is_external_or_fragment(link: str) -> bool:
    return link.startswith(("#", "https://", "http://", "mailto:"))


def _validate_page(site_root: Path, page: Path) -> list[str]:
    source = page.read_text(encoding="utf-8")
    parser = _LinkCollector()
    parser.feed(source)
    errors: list[str] = []

    if not parser.has_title:
        errors.append(f"{page}: missing <title>")
    if not parser.has_main:
        errors.append(f"{page}: missing <main>")

    local_pages = {
        link.split("#", 1)[0]
        for link in parser.links
        if link.split("#", 1)[0] in REQUIRED_PAGES
    }
    missing_navigation = REQUIRED_NAVIGATION - local_pages
    errors.extend(
        f"{page}: missing hosted navigation link: {name}"
        for name in sorted(missing_navigation)
    )

    errors.extend(
        f"{page}: links to removed repository user guide: {guide}"
        for guide in FORBIDDEN_REPOSITORY_GUIDES
        if guide in source
    )

    for link in parser.links:
        if _is_external_or_fragment(link):
            continue
        destination = (page.parent / link.split("#", 1)[0]).resolve()
        if site_root not in destination.parents and destination != site_root:
            errors.append(f"{page}: local link escapes site: {link}")
        elif not destination.is_file():
            errors.append(f"{page}: missing local link target: {link}")
    return errors


def main() -> int:
    site_root = Path(sys.argv[1] if len(sys.argv) == 2 else "docs/site").resolve()
    if not site_root.is_dir():
        print(
            f"documentation site directory does not exist: {site_root}", file=sys.stderr
        )
        return 1

    actual_pages = {path.name for path in site_root.glob("*.html")}
    missing_pages = REQUIRED_PAGES - actual_pages
    errors = [f"missing required page: {name}" for name in sorted(missing_pages)]
    for page_name in sorted(REQUIRED_PAGES & actual_pages):
        errors.extend(_validate_page(site_root, site_root / page_name))

    index = site_root / "index.html"
    if index.is_file():
        index_source = index.read_text(encoding="utf-8")
        errors.extend(
            f"{index}: missing required landing text: {text!r}"
            for text in REQUIRED_INDEX_TEXT
            if text not in index_source
        )

    for page_name, expected_text in REQUIRED_TRANSCRIPT_TEXT.items():
        page = site_root / page_name
        if not page.is_file():
            continue
        source = page.read_text(encoding="utf-8")
        errors.extend(
            f"{page}: missing required transcript text: {text!r}"
            for text in expected_text
            if text not in source
        )

    for page_name, expected_text in REQUIRED_REFERENCE_TEXT.items():
        page = site_root / page_name
        if not page.is_file():
            continue
        source = page.read_text(encoding="utf-8")
        errors.extend(
            f"{page}: missing required reference text: {text!r}"
            for text in expected_text
            if text not in source
        )

    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print(f"validated {len(REQUIRED_PAGES)} static documentation pages in {site_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
