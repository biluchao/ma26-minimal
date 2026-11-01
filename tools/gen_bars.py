#!/usr/bin/env python3
"""tools/gen_bars.py — Generate a deterministic OHLCV bar file for backtest.

Usage:
    python tools/gen_bars.py --bars 1000 --output data/bars.json
"""

import argparse, json, hashlib, math, os


def generate(n: int, start_price: float = 30000.0, seed: str = "ma26") -> list:
    bars = []
    price = start_price
    h = hashlib.md5(seed.encode()).hexdigest()
    for i in range(n):
        token = int(hashlib.md5(f"{seed}:{i}".encode()).hexdigest(), 16)
        # regime: trend changes every ~n/4 bars
        regime = math.sin(i / (n / 4) * math.pi)
        noise = ((token % 10000) - 5000) / 5000.0
        close = max(100.0, price * (1 + 0.002 * regime + 0.001 * noise))
        spread = abs(noise) * 0.3 + 0.0005
        high = close * (1 + spread)
        low = close * (1 - spread)
        bars.append({
            'bar_index': i,
            'timestamp': i * 3600,
            'open': round(price, 2),
            'high': round(high, 2),
            'low': round(low, 2),
            'close': round(close, 2),
            'volume': round(1000 + (token % 5000), 1),
        })
        price = close
    return bars


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--bars', type=int, default=500)
    ap.add_argument('--start', type=float, default=30000.0)
    ap.add_argument('--seed', default='ma26')
    ap.add_argument('--output', default='data/bars.json')
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.output) or '.', exist_ok=True)
    bars = generate(args.bars, args.start, args.seed)
    with open(args.output, 'w') as f:
        json.dump(bars, f)
    print(f"Wrote {len(bars)} bars → {args.output}")
    print(f"  first: {bars[0]}")
    print(f"  last:  {bars[-1]}")


if __name__ == '__main__':
    main()

