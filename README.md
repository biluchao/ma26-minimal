# MA26-ATR Quantitative Trading System — Minimal Runtime

最小可运行集（42 files）· 规格书 v4.1.0

## Quick Start

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install pyyaml
python src/bootstrap.py --mode backtest --config config/default.yaml
```

## Layout

```
src/core/        7 files — infrastructure pillars (Result / EventBus / Trace / Config / Lifecycle / State / Manifest)
src/m0x_*/       9 modules — Strategy / Cache / Risk / State / Equity / Execution / Performance / Snapshot / Tracer
src/patches/     hotfix patches
src/bootstrap.py orchestrator
config/           YAML configs (default / paper / live)
tests/           unit + integration
tools/            verification scripts
```


