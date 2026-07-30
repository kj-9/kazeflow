.PHONY: format lint typecheck test check build package-check

UV_RUN=uv run --frozen
UV_RUN_TUI=$(UV_RUN) --extra tui


format:
	$(UV_RUN) ruff format .

lint:
	$(UV_RUN) ruff check . --fix

typecheck:
	$(UV_RUN_TUI) ty check

test:
	$(UV_RUN_TUI) pytest

ci-check:
	$(UV_RUN) ruff format . --check
	$(UV_RUN) ruff check .
	$(UV_RUN_TUI) ty check


check:
	$(MAKE) -j format lint typecheck


build:
	uv build


package-check: build
	python3 scripts/verify_wheel_metadata.py dist
	python3 scripts/smoke_wheel_install.py --wheel dist --mode core
	python3 scripts/smoke_wheel_install.py --wheel dist --mode tui
