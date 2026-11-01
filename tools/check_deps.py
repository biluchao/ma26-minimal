#!/usr/bin/env python3
"""tools/check_deps.py — Scan src/ for internal import graph and validate layering + cycles."""

import ast
import sys
from pathlib import Path
from collections import defaultdict, deque

ROOT = Path(__file__).resolve().parent.parent / 'src'

LAYER_ORDER = [
    'core', 'm01_strategy', 'm03_cache', 'm04_risk', 'm05_state', 'm06_equity',
    'm07_execution', 'm11_performance', 'm12_snapshot', 'm13_tracer', 'patches',
]


def layer_of(modname: str) -> int:
    for i, p in enumerate(LAYER_ORDER):
        if modname.startswith(p):
            return i
    return -1


def scan():
    edges = []
    modules = set()
    for py in ROOT.rglob('*.py'):
        if any(s in py.parts for s in ('__pycache__',)):
            continue
        rel = py.relative_to(ROOT)
        mod = str(rel).replace('/', '.').replace('\\', '.').replace('.py', '')
        if mod == '__init__':
            continue
        modules.add(mod)
        try:
            tree = ast.parse(py.read_text())
        except Exception:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                target = node.module
                if target.startswith('core.') or any(target.startswith(f'm{i:02d}_') for i in range(1, 26)):
                    edges.append((mod, target, node.lineno, py))
            elif isinstance(node, ast.Import):
                for a in node.names:
                    if a.name.startswith('core.') or any(a.name.startswith(f'm{i:02d}_') for i in range(1, 26)):
                        edges.append((mod, a.name, node.lineno, py))
    return modules, edges


def validate(modules, edges):
    errors = []
    warnings = []
    graph = defaultdict(list)

    for src, tgt, lineno, py in edges:
        # resolve relative prefix
        base = src.rsplit('.', 1)[0]
        if tgt.startswith('.'):
            tgt = (base + tgt).replace('..', '.').strip('.')
        parts = tgt.split('.')
        tgt_mod = tgt if tgt in modules else parts[0]

        if tgt_mod not in modules:
            continue
        graph[src].append(tgt_mod)

        ls, lt = layer_of(src), layer_of(tgt_mod)
        if ls == -1 or lt == -1:
            continue
        if ls > lt + 1:
            errors.append(f"LAYER VIOLATION: {src} (L{ls}) → {tgt_mod} (L{lt}) @ {py}:{lineno}")
        if ls - lt > 2:
            warnings.append(f"LAYER SKIP: {src} → {tgt_mod} @ {py}:{lineno}")
        if ls == 0 and lt > 0 and not tgt_mod.startswith('core'):
            errors.append(f"CORE DEPENDS ON BUSINESS: {src} → {tgt_mod} @ {py}:{lineno}")

    # cycle detection
    visited, stack = set(), set()
    cycles = []
    def dfs(n, path):
        visited.add(n); stack.add(n)
        for nb in graph.get(n, []):
            if nb in stack:
                i = path.index(nb)
                cycles.append(path[i:] + [nb])
            elif nb not in visited:
                dfs(nb, path + [nb])
        stack.discard(n)
    for m in list(graph):
        if m not in visited:
            dfs(m, [m])
    for c in cycles:
        errors.append(f"CIRCULAR DEPENDENCY: {' → '.join(c)}")

    return errors, warnings


def main():
    modules, edges = scan()
    errors, warnings = validate(modules, edges)
    print(f"Scanned {len(modules)} modules, {len(edges)} internal edges\n")
    if errors:
        print("ERRORS:")
        for e in errors:
            print(f"  ✗ {e}")
    if warnings:
        print("\nWARNINGS:")
        for w in warnings:
            print(f"  ⚠ {w}")
    print(f"\nResult: {'PASS' if not errors else 'FAIL'} "
          f"({len(errors)} errors, {len(warnings)} warnings)")
    sys.exit(0 if not errors else 1)


if __name__ == '__main__':
    main()

