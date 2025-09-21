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

and `make test` for:
4.  **Run tests**: Run `uv run pytest`

## 3. Test-Driven Development (TDD)

When fixing a bug or adding a new feature, the following TDD workflow should be followed:

1.  **Add a Failing Test**: Before writing any implementation code, add a new test case that reproduces the bug or validates the new feature.
2.  **Confirm Failure**: Run the test suite to ensure that the new test fails as expected and that the existing tests still pass.
3.  **Implement the Change**: Write the necessary code to fix the bug or implement the feature.
4.  **Confirm Success**: Run the entire test suite again to confirm that the new test case now passes and that all existing tests still pass.
5.  **Code Quality Checks**: Run `make check` to ensure the code is well-formatted, linted, and passes type checks.
6.  **Clean up**: The test case can be left as a regression test.