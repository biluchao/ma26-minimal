#!/usr/bin/env python3
"""tools/verify_bootstrap.py — Verify the system can bootstrap in all three modes."""

import sys, subprocess, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from src.bootstrap import SystemOrchestrator, generate_demo_bars


def check_mode(mode, config_path, expect_fatal_on_missing_key=False):
    print(f"\n{'='*60}")
    print(f"Verifying mode: {mode}  config: {config_path}")
    print('='*60)
    orch = SystemOrchestrator(config_path, mode)
    r = orch.bootstrap()
    if r.is_err():
        print(f"  ✗ bootstrap failed: {r.error.format_chain()}")
        return False
    print(f"  ✓ bootstrap ok (modules: {len(orch.modules)})")

    # dependency check
    from src.core.module_manifest import DependencyResolver, ModuleManifest
    manifests = []
    for n, m in orch.modules.items():
        manifests.append(ModuleManifest(name=n, version=getattr(m, 'MODULE_VERSION', '0.0.0'),
                                      dependencies=[{'name': d} for d in getattr(m, 'DEPENDENCIES', [])]))
    ok, errs = DependencyResolver(manifests).resolve()
    print(f"  {'✓' if ok else '✗'} dependency resolution: {len(errs)} errors")

    # run a short backtest
    bars = generate_demo_bars(120)
    try:
        orch.run(bars)
        print(f"  ✓ run() completed ({len(bars)} bars)")
    except Exception as e:
        print(f"  ✗ run() raised: {e}")
        return False
    return ok


def main():
    base = os.path.join(os.path.dirname(__file__), '..', 'config')
    results = []
    results.append(('backtest', check_mode('backtest', os.path.join(base, 'default.yaml'))))
    results.append(('paper', check_mode('paper', os.path.join(base, 'paper.yaml'))))
    results.append(('live', check_mode('live', os.path.join(base, 'live.yaml'))))

    print(f"\n{'='*60}")
    print("Summary")
    print('='*60)
    for mode, ok in results:
        print(f"  {'✓' if ok else '✗'} {mode}")
    all_ok = all(r[1] for r in results)
    print(f"\nOverall: {'ALL PASS' if all_ok else 'SOME FAILED'}")
    sys.exit(0 if all_ok else 1)


if __name__ == '__main__':
    main()

