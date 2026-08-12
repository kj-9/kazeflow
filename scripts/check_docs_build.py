"""Check the generated documentation surface used by GitHub Pages."""

from __future__ import annotations

import argparse
from pathlib import Path


REQUIRED_PAGES = {
    "index.html": ("Make a growing script understandable before you run it", "Search"),
    "getting-started.html": (
        "Python 3.10 through 3.13",
        "kazeflow --help",
        "&quot;kazeflow[tui]&quot;",
        "Proceed?",
        "RunResult",
        "not automatically redacted",
    ),
    "cli.html": ("CLI reference", "kazeflow plan"),
    "partitions.html": (
        "configuration error",
        "DatePartitionDef",
        "--partition-range",
        "--empty-partitions",
        "canonical ISO",
        "generic redaction",
        "--partition-key",
    ),
    "results.html": (
        "Results and history",
        "list, show, or compare",
        "other sensitive application values",
    ),
    "cli/partitions.html": (
        "Inspect partition definitions",
        "--format text|json",
        "strict ISO",
        "dynamic catalog",
    ),
    "cli/plan.html": ("--max-concurrency", "--partition-range", "mermaid"),
    "cli/run.html": ("--yes", "--empty-partitions", "--store PATH"),
    "api/core.html": ("kazeflow.Flow", "kazeflow.asset", "kazeflow.PartitionDef"),
    "api/sqlite.html": ("SQLiteRunStore", "StoredRunRecord"),
    "concepts/failure-semantics.html": (
        "asyncio.CancelledError",
        "no synthetic terminal result",
    ),
    "concepts/trust-boundary.html": (
        "Portable-record boundary",
        "not sanitization, redaction, or a confidentiality guarantee",
    ),
}

REQUIRED_SEARCH_TERMS = (
    "FlowPlan",
    "SQLiteRunStore",
    "kazeflow run",
    "--partition-key",
    "--partition-range",
    "--empty-partitions",
    "kazeflow partitions",
    "Trust boundary",
    "Portable-record boundary",
    "External asyncio cancellation",
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
