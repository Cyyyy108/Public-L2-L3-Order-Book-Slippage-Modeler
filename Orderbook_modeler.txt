import asyncio
import csv
import json
import os
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
import requests
import websockets

SYMBOL = "BTCUSDT"
WS_URL = f"wss://stream.binance.com:9443/ws/{SYMBOL.lower()}@depth@100ms"
REST_SNAPSHOT_URL = f"https://api.binance.com/api/v3/depth?symbol={SYMBOL}&limit=5000"
NOTIONAL_TIERS = [100_000.0, 500_000.0, 2_000_000.0]
TOP_K_LEVELS = 10
SORT_DEPTH = 2000
CSV_FILENAME = "orderbook_metrics.csv"

CSV_HEADERS = [
    "timestamp_utc",
    "timestamp_local",
    "symbol",
    "best_bid",
    "best_ask",
    "mid_price",
    "top_spread_usd",
    "top_spread_bps",
    f"dw_spread_l{TOP_K_LEVELS}_usd",
    f"dw_spread_l{TOP_K_LEVELS}_bps",
    f"obi_l{TOP_K_LEVELS}",
    "buy_100k_slip_bps",
    "sell_100k_slip_bps",
    "buy_500k_slip_bps",
    "sell_500k_slip_bps",
    "buy_2m_slip_bps",
    "sell_2m_slip_bps",
]


class OrderBook:
    def __init__(self) -> None:
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        self.last_update_id: int = 0
        self.synced: bool = False

    def load_snapshot(self, snapshot: dict) -> None:
        self.bids = {
            float(p): float(q) for p, q in snapshot.get("bids", []) if float(q) > 0.0
        }
        self.asks = {
            float(p): float(q) for p, q in snapshot.get("asks", []) if float(q) > 0.0
        }
        self.last_update_id = int(snapshot["lastUpdateId"])
        self.synced = True

    def _apply_side(self, side_dict: Dict[float, float], updates: List[List[str]]) -> None:
        for price_str, qty_str in updates:
            price = float(price_str)
            qty = float(qty_str)
            if qty == 0.0:
                side_dict.pop(price, None)
            else:
                side_dict[price] = qty

    def apply_diff(self, event: dict) -> bool:
        u = int(event["u"])
        U = int(event["U"])

        if not self.synced:
            return False

        if u <= self.last_update_id:
            return True

        if U > self.last_update_id + 1:
            self.synced = False
            return False

        self._apply_side(self.bids, event.get("b", []))
        self._apply_side(self.asks, event.get("a", []))
        self.last_update_id = u
        return True

    def sorted_levels(
        self, depth: int = SORT_DEPTH
    ) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
        sorted_bids = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[:depth]
        sorted_asks = sorted(self.asks.items(), key=lambda x: x[0])[:depth]
        return sorted_bids, sorted_asks


def compute_depth_weighted_spread(
    bids: List[Tuple[float, float]],
    asks: List[Tuple[float, float]],
    k: int = TOP_K_LEVELS,
) -> Tuple[float, float, float]:
    top_bids = bids[:k]
    top_asks = asks[:k]
    if not top_bids or not top_asks:
        return 0.0, 0.0, 0.0

    bid_vol = sum(q for _, q in top_bids)
    ask_vol = sum(q for _, q in top_asks)
    if bid_vol <= 0.0 or ask_vol <= 0.0:
        return 0.0, 0.0, 0.0

    pw_bid = sum(p * q for p, q in top_bids) / bid_vol
    pw_ask = sum(p * q for p, q in top_asks) / ask_vol
    dw_spread = pw_ask - pw_bid
    mid_price = 0.5 * (bids[0][0] + asks[0][0])
    dw_spread_bps = (dw_spread / mid_price) * 10_000.0 if mid_price > 0 else 0.0
    return dw_spread, dw_spread_bps, mid_price


def compute_obi(
    bids: List[Tuple[float, float]],
    asks: List[Tuple[float, float]],
    k: int = TOP_K_LEVELS,
) -> float:
    v_bid = sum(q for _, q in bids[:k])
    v_ask = sum(q for _, q in asks[:k])
    total = v_bid + v_ask
    return (v_bid - v_ask) / total if total > 0.0 else 0.0


def simulate_market_order_slippage(
    levels: List[Tuple[float, float]],
    notional_usd: float,
    reference_price: float,
    side: str,
) -> Tuple[float, Optional[float], bool]:
    remaining_notional = notional_usd
    total_qty = 0.0
    total_cost = 0.0

    for price, qty in levels:
        level_notional = price * qty
        if remaining_notional <= level_notional:
            fill_qty = remaining_notional / price
            total_qty += fill_qty
            total_cost += remaining_notional
            remaining_notional = 0.0
            break
        else:
            total_qty += qty
            total_cost += level_notional
            remaining_notional -= level_notional

    filled_completely = remaining_notional <= 1e-6
    if not filled_completely or total_qty <= 0.0 or reference_price <= 0.0:
        return 0.0, None, False

    vwap = total_cost / total_qty
    if side == "buy":
        slippage_bps = ((vwap - reference_price) / reference_price) * 10_000.0
    else:
        slippage_bps = ((reference_price - vwap) / reference_price) * 10_000.0

    return vwap, slippage_bps, True


def fetch_snapshot() -> dict:
    resp = requests.get(REST_SNAPSHOT_URL, timeout=10)
    resp.raise_for_status()
    return resp.json()


class CsvLogger:
    def __init__(self, filepath: str) -> None:
        self.filepath = filepath
        file_exists = os.path.exists(self.filepath) and os.path.getsize(self.filepath) > 0
        self._file = open(self.filepath, mode="a", newline="", encoding="utf-8")
        self._writer = csv.writer(self._file)
        if not file_exists:
            self._writer.writerow(CSV_HEADERS)
            self._file.flush()

    def write_row(self, row: list) -> None:
        self._writer.writerow(row)
        self._file.flush()

    def close(self) -> None:
        if not self._file.closed:
            self._file.close()


def fmt_slip(val: Optional[float]) -> str:
    return f"{val:6.2f} bps" if val is not None else "THIN BOOK "


def csv_slip(val: Optional[float]) -> str:
    return f"{val:.6f}" if val is not None else ""


def record_metrics(book: OrderBook, logger: CsvLogger) -> None:
    bids, asks = book.sorted_levels(depth=SORT_DEPTH)
    if not bids or not asks:
        return

    best_bid = bids[0][0]
    best_ask = asks[0][0]
    mid = 0.5 * (best_bid + best_ask)
    top_spread_usd = best_ask - best_bid
    top_spread_bps = (top_spread_usd / mid) * 10_000.0 if mid > 0 else 0.0

    dw_spread, dw_spread_bps, _ = compute_depth_weighted_spread(bids, asks, k=TOP_K_LEVELS)
    obi = compute_obi(bids, asks, k=TOP_K_LEVELS)

    _, buy_100k, _ = simulate_market_order_slippage(asks, 100_000.0, best_ask, "buy")
    _, sell_100k, _ = simulate_market_order_slippage(bids, 100_000.0, best_bid, "sell")
    _, buy_500k, _ = simulate_market_order_slippage(asks, 500_000.0, best_ask, "buy")
    _, sell_500k, _ = simulate_market_order_slippage(bids, 500_000.0, best_bid, "sell")
    _, buy_2m, _ = simulate_market_order_slippage(asks, 2_000_000.0, best_ask, "buy")
    _, sell_2m, _ = simulate_market_order_slippage(bids, 2_000_000.0, best_bid, "sell")

    now_local_dt = datetime.now().astimezone()
    now_utc_dt = datetime.now(timezone.utc)
    ts_local = now_local_dt.strftime("%Y-%m-%d %H:%M:%S")
    ts_utc = now_utc_dt.strftime("%Y-%m-%d %H:%M:%S")

    print(
        f"[{now_local_dt.strftime('%H:%M:%S')}] Mid: ${mid:,.2f} | "
        f"Top Spread: {top_spread_bps:.2f} bps | "
        f"DW-Spread (L{TOP_K_LEVELS}): ${dw_spread:.2f} ({dw_spread_bps:.2f} bps) | "
        f"OBI (L{TOP_K_LEVELS}): {obi:+.4f}"
    )
    print(f"   $0.10M -> Buy Slippage vs Best Ask: {fmt_slip(buy_100k)} | Sell Slippage vs Best Bid: {fmt_slip(sell_100k)}")
    print(f"   $0.50M -> Buy Slippage vs Best Ask: {fmt_slip(buy_500k)} | Sell Slippage vs Best Bid: {fmt_slip(sell_500k)}")
    print(f"   $2.00M -> Buy Slippage vs Best Ask: {fmt_slip(buy_2m)} | Sell Slippage vs Best Bid: {fmt_slip(sell_2m)}")

    logger.write_row([
        ts_utc,
        ts_local,
        SYMBOL,
        f"{best_bid:.2f}",
        f"{best_ask:.2f}",
        f"{mid:.2f}",
        f"{top_spread_usd:.2f}",
        f"{top_spread_bps:.6f}",
        f"{dw_spread:.2f}",
        f"{dw_spread_bps:.6f}",
        f"{obi:.6f}",
        csv_slip(buy_100k),
        csv_slip(sell_100k),
        csv_slip(buy_500k),
        csv_slip(sell_500k),
        csv_slip(buy_2m),
        csv_slip(sell_2m),
    ])


async def stream_order_book(logger: CsvLogger) -> None:
    book = OrderBook()
    buffer: List[dict] = []
    last_print = 0.0

    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
                buffer.clear()
                book.synced = False
                snapshot_loaded = False

                async for raw_msg in ws:
                    event = json.loads(raw_msg)
                    if not snapshot_loaded:
                        buffer.append(event)
                        if len(buffer) >= 3:
                            snapshot = fetch_snapshot()
                            book.load_snapshot(snapshot)
                            for ev in buffer:
                                if int(ev["u"]) > book.last_update_id:
                                    book.apply_diff(ev)
                            buffer.clear()
                            snapshot_loaded = True
                    else:
                        ok = book.apply_diff(event)
                        if not ok:
                            snapshot = fetch_snapshot()
                            book.load_snapshot(snapshot)

                    now = time.monotonic()
                    if book.synced and (now - last_print >= 1.0):
                        record_metrics(book, logger)
                        last_print = now

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"Connection interrupted ({exc}); reconnecting in 2s...")
            await asyncio.sleep(2.0)


def main() -> None:
    logger = CsvLogger(CSV_FILENAME)
    print(f"Logging live metrics to: {os.path.abspath(CSV_FILENAME)} (Press Ctrl+C to stop)")
    try:
        asyncio.run(stream_order_book(logger))
    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        logger.close()


if __name__ == "__main__":
    main()