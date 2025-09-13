.PHONY: format lint typecheck test check

UV_RUN=uv run --frozen


format:
	$(UV_RUN) ruff format .

lint:
	$(UV_RUN) ruff check . --fix

typecheck:
	$(UV_RUN) ty check

test:
	$(UV_RUN) pytest

check:
	$(MAKE) -j format lint typecheck test

build:
	uv build --frozen
