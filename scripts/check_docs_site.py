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
REQUIRED_INDEX_TEXT = (
    "kazeflow",
    "Run your first flow",
    "Trust boundary.",
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

    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print(f"validated {len(REQUIRED_PAGES)} static documentation pages in {site_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
