"""Check the generated documentation surface used by GitHub Pages."""

from __future__ import annotations

import argparse
from pathlib import Path


REQUIRED_PAGES = {
    "index.html": ("Make a growing script understandable before you run it", "Search"),
    "getting-started.html": ("Proceed?", "RunResult"),
    "cli.html": ("CLI reference", "kazeflow plan"),
    "partitions.html": ("Partitions", "--partition-key"),
    "results.html": ("Results and history", "list, show, or compare"),
    "cli/plan.html": ("--max-concurrency", "mermaid"),
    "cli/run.html": ("--yes", "--store PATH"),
    "api/core.html": ("kazeflow.Flow", "kazeflow.asset"),
    "api/sqlite.html": ("SQLiteRunStore", "StoredRunRecord"),
}

REQUIRED_SEARCH_TERMS = (
    "FlowPlan",
    "SQLiteRunStore",
    "kazeflow run",
    "--partition-key",
    "Trust boundary",
)


def check_site(site_dir: Path) -> list[str]:
    issues: list[str] = []

    for relative_path, required_text in REQUIRED_PAGES.items():
        page = site_dir / relative_path
        if not page.is_file():
            issues.append(f"missing generated page: {relative_path}")
            continue

        html = page.read_text(encoding="utf-8")
        for text in required_text:
            if text not in html:
                issues.append(f"{relative_path}: missing text {text!r}")

    search_index = site_dir / "search" / "search_index.json"
    if not search_index.is_file():
        issues.append("missing generated search index")
    else:
        search_text = search_index.read_text(encoding="utf-8")
        for term in REQUIRED_SEARCH_TERMS:
            if term not in search_text:
                issues.append(f"search index: missing term {term!r}")

    return issues


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("site_dir", type=Path)
    args = parser.parse_args()

    issues = check_site(args.site_dir)
    if issues:
        for issue in issues:
            print(f"docs check: {issue}")
        return 1

    print(f"docs check: verified {len(REQUIRED_PAGES)} pages and search index")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
