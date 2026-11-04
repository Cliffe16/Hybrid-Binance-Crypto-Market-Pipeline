import os

import uvicorn
from cassandra.cluster import Cluster
from fastapi import FastAPI, HTTPException, Query

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "127.0.0.1")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
KEYSPACE = "binance"

app = FastAPI(
    title="Binance Pipeline API",
    description="API Data from Binance",
)

# Connect to Cassandra
cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
session = cluster.connect(KEYSPACE)
print("Successfully connected to Cassandra")

# symbol and limit are bound as parameters, never interpolated.
GET_TICKER = session.prepare("SELECT * FROM tickers WHERE symbol=?")
GET_CANDLES = session.prepare(
    "SELECT * FROM candlestick WHERE symbol=? ORDER BY window_start DESC LIMIT ?"
)
GET_PAIRS = session.prepare("SELECT * FROM trading_pairs LIMIT ?")
GET_ORDER_BOOKS = session.prepare(
    "SELECT * FROM order_books WHERE symbol=? ORDER BY lastupdateid DESC LIMIT ?"
)


@app.get("/")
def home():
    return {"status": "API is live", "docs_url": "/docs"}


@app.get("/tickers/{symbol}")
def get_ticker(symbol: str):
    """Fetch the absolute latest price for a specific symbol"""
    row = session.execute(GET_TICKER, (symbol.upper(),)).one()

    if row is None:
        raise HTTPException(status_code=404, detail="Symbol not found in database")

    return {
        "symbol": row.symbol,
        "last_price": getattr(row, "last_price", None),
        # Not written by the Spark job today, so these are None unless populated elsewhere
        "price_change": getattr(row, "price_change", None),
        "price_change_percent": getattr(row, "price_change_percent", None),
        "volume": getattr(row, "volume", None),
    }


@app.get("/candlesticks/{symbol}")
def get_candlesticks(symbol: str, limit: int = Query(10, ge=1, le=1000)):
    """Fetch the latest 1-minute candlestick aggregations"""
    rows = session.execute(GET_CANDLES, (symbol.upper(), limit))

    results = [
        {
            "timestamp": row.window_start.strftime("%Y-%m-%d %H:%M:%S"),
            "average_price": row.avg_minute_price,
            "volume": row.total_volume_min,
            "volatility_spread": row.max_spread,
        }
        for row in rows
    ]

    return {"symbol": symbol.upper(), "data_points": len(results), "data": results}


@app.get("/trading-pairs")
def get_pairs(limit: int = Query(100, ge=1, le=1000)):
    """Fetch active trading pairs and their base assets"""
    rows = session.execute(GET_PAIRS, (limit,))

    results = [
        {
            "symbol": row.symbol,
            "base_asset": getattr(row, "base_asset", None),
            "quote_asset": getattr(row, "quote_asset", None),
            "status": getattr(row, "status", None),
        }
        for row in rows
    ]

    return {"total_returned": len(results), "data": results}


@app.get("/order-books/{symbol}")
def get_order_books(symbol: str, limit: int = Query(5, ge=1, le=1000)):
    """Fetch recent best bid/ask snapshots (top of book)"""
    rows = session.execute(GET_ORDER_BOOKS, (symbol.upper(), limit))

    results = [
        {
            "update_id": row.lastupdateid,
            "bid_price": row.bid_price,
            "bid_qty": row.bid_qty,
            "ask_price": row.ask_price,
            "ask_qty": row.ask_qty,
        }
        for row in rows
    ]

    return {"symbol": symbol.upper(), "depth_points": len(results), "data": results}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
