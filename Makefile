.PHONY: install test bench fuzz repl clean fmt lint

install:
	pip install -e ".[dev]"

test:
	pytest tests/ -v

bench:
	python benchmarks/bench_queries.py

fuzz:
	pytest tests/test_fuzzing.py -v

repl:
	python -m forge

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	rm -rf .pytest_cache .mypy_cache dist build *.egg-info src/*.egg-info

fmt:
	ruff format .

lint:
	ruff check .
