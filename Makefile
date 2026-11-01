.PHONY: help test test-core test-modules test-integration deps verify run smoke clean

PY ?= python3.11

help:
	@echo "Targets:"
	@echo "  test           Run all tests"
	@echo "  test-core      Unit tests for core infrastructure"
	@echo "  test-modules   Unit tests for business modules"
	@echo "  test-integration  End-to-end backtest tests"
	@echo "  deps           Validate import layering + cycles"
	@echo "  verify         Bootstrap verification (all 3 modes)"
	@echo "  smoke          Run a 500-bar demo backtest"
	@echo "  gen-bars       Generate sample bar data"
	@echo "  clean          Remove caches and snapshots"

test: test-core test-modules test-integration

test-core:
	$(PY) tests/unit/test_core.py

test-modules:
	$(PY) tests/unit/test_modules.py

test-integration:
	$(PY) tests/integration/test_backtest.py

deps:
	$(PY) tools/check_deps.py

verify:
	$(PY) tools/verify_bootstrap.py

smoke:
	$(PY) src/bootstrap.py --mode backtest --config config/default.yaml

gen-bars:
	$(PY) tools/gen_bars.py --bars 1000 --output data/bars.json

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name '*.pyc' -delete
	rm -rf snapshots_backtest snapshots_paper snapshots_live
	rm -f traces/*.jsonl

