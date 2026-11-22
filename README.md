# BTCUSDT Real-Time L2 Order Book & Slippage Modeler

Real-time Level 2 (`L2`) order book and slippage modeler for `BTCUSDT` using Binance's `@depth@100ms` WebSocket stream synchronized with a REST depth snapshot and sequence-ID gap recovery (`U`/`u`).

## Overview
- **Top-of-Book & Depth-Weighted Spread (`L10`):** Tracks best bid/ask spreads alongside volume-weighted bid and ask prices across the top 10 depth levels in USD and basis points (`bps`).
- **Order Book Imbalance (`OBI`):** Measures top-10 bid vs. ask volume imbalance on `[-1, +1]`.
- **Simulated Market-Order Slippage:** Walks the order book for `$100K`, `$500K`, and `$2M` notional buy and sell orders, reporting slippage in `bps` relative to best ask / best bid.
- **Snapshot Logging:** Saves periodic order book snapshots to CSV for empirical analysis.

## Repository Contents
- `Orderbook_modeler.py` — Main real-time order book and slippage script.
- `Orderbook_modeler.txt` — Plain-text copy of the Python script.
- `requirements.txt` — Third-party Python dependencies (`websockets`, `requests`).
- `Task1.1.pdf` — 1-page report with appendix of the snapshot covering theoretical market-making edge, adverse selection, and top-tier exchange fee structures.
