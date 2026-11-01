#!/usr/bin/env python3
"""tools/check_patches.py — List registered patches and verify their format."""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from src import patches


def main():
    print("Registered patches:\n")
    all_ok = True
    for code, name, desc in patches.list_patches():
        mod = getattr(patches, f"PATCH_{code.replace('-', '_')}_{name}", None)
        if mod is None:
            print(f"  ✗ {code} {name} — module not importable")
            all_ok = False
            continue
        has_desc = bool(getattr(mod, 'DESCRIPTION', ''))
        is_applied = bool(getattr(mod, 'APPLIED', False))
        print(f"  {'✓' if has_desc and is_applied else '✗'} {code} {name}")
        print(f"      desc: {getattr(mod, 'DESCRIPTION', '(missing)')}")
        print(f"      applied: {is_applied}")
        if not (has_desc and is_applied):
            all_ok = False
    sys.exit(0 if all_ok else 1)


if __name__ == '__main__':
    main()

