import os
import time

import requests

from src import config

# Binance intervals are case-sensitive: "1m" is one MINUTE, "1M" is one MONTH.
CANDLE_INTERVAL = os.getenv("CANDLE_INTERVAL", "1m")
CANDLE_LIMIT = int(os.getenv("CANDLE_LIMIT", "100"))  # Keep batch memory bounded
MAX_SYMBOLS = int(os.getenv("MAX_SYMBOLS", "0"))    # Set to 0 to fetch all active pairs
QUOTE_ASSET_FILTER = os.getenv("QUOTE_ASSET_FILTER", "USDT")  # Focus on liquid USDT pairs

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}


def get_json(session, url, retries=5, **kwargs):
    """GET a URL and return its JSON, backing off on rate limits, server errors, and network timeouts."""
    for attempt in range(retries):
        try:
            response = session.get(url, headers=HEADERS, timeout=10, **kwargs)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            wait = 2 ** attempt
            print(f"Network error ({exc}) from {url}; retrying in {wait}s ({attempt + 1}/{retries})", flush=True)
            time.sleep(wait)
            continue

        if response.status_code in (418, 429) or response.status_code >= 500:
            try:
                wait = int(response.headers.get("Retry-After", 2 ** attempt))
            except ValueError:
                wait = 2 ** attempt
            print(f"HTTP {response.status_code} from {url}; retrying in {wait}s ({attempt + 1}/{retries})", flush=True)
            time.sleep(wait)
            continue

        response.raise_for_status()
        return response.json()

    raise RuntimeError(f"Exhausted {retries} retries for {url}")


def extract():
    """Extracts Data from Binance API endpoints"""
    market_data = {}

    with requests.Session() as session:
        try:
            urls = {
                "ticker": config.price_url,
                "trading_pairs": config.trading_pairs_url,
            }

            for name, url in urls.items():
                market_data[name] = get_json(session, url)

            # Filter to actively trading symbols only (avoids 1,000+ halted 'BREAK' symbols)
            exchange_symbols = market_data.get("trading_pairs", {}).get("symbols", [])
            active_symbols = {
                s["symbol"]
                for s in exchange_symbols
                if s.get("status") == "TRADING"
                and (not QUOTE_ASSET_FILTER or s.get("quoteAsset") == QUOTE_ASSET_FILTER)
            }

            if active_symbols:
                symbols = [t["symbol"] for t in market_data["ticker"] if t["symbol"] in active_symbols]
            else:
                symbols = [t["symbol"] for t in market_data["ticker"]]

            if MAX_SYMBOLS > 0:
                symbols = symbols[:MAX_SYMBOLS]

            print(f"Extracting order books and candlesticks for {len(symbols)} active symbols...", flush=True)

            for idx, symbol in enumerate(symbols, start=1):
                params = {"symbol": symbol}
                ohlc_params = {"symbol": symbol, "interval": CANDLE_INTERVAL, "limit": CANDLE_LIMIT}

                order_data = get_json(session, config.order_book_url, params=params)
                ohlc_data = get_json(session, config.ohlc_url, params=ohlc_params)

                market_data[symbol] = {
                    "order_book": order_data,
                    "candlestick": ohlc_data,
                }

                if idx % 25 == 0 or idx == len(symbols):
                    print(f"Extracted {idx}/{len(symbols)} symbols", flush=True)

                time.sleep(0.1)

        except Exception as e:
            print(f"Extraction Error: {e}", flush=True)
            raise
        else:
            print("Data Extraction successful", flush=True)
            return market_data
