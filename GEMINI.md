# Gemini Development Rules

This file outlines the development rules for this project when using Gemini.

## 1. Code Style and Quality

*   **Linter and Formatter**: `ruff` is used as the linter and formatter. All code must be formatted and linted with `ruff` before committing.
*   **Type Checking**: `ty` is used as the type checker. All code must pass `ty`'s strict type checking before committing.
*   **Testing**: `pytest` is used for unit testing. All new code should be accompanied by unit tests.

## 2. Workflow

After any code changes, Gemini will automatically perform the following steps:

use `make check` to run all of below:

1.  **Format the code**: Run `ruff format .`
2.  **Lint the code**: Run `ruff check . --fix`
3.  **Type check the code**: Run `uv run ty check`
4.  **Run tests**: Run `uv run pytest`
