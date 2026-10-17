from fastapi import FastAPI, HTTPException
from cassandra.cluster import Cluster
import uvicorn

# Initialize the FastApi engine
app = FastAPI(
    title="Binance Pipeline API",
    description="API Data from Binance"
    )

# Connect to Cassandra
try:
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect('binance')
    print("Successfully connected to Cassandra")
except Exception as e:
    print(f"Database Connection Error: {e}")

@app.get("/")
def home():
    return {"status": "API is live", "docs_url": "http://localhost:8080/docs"}

@app.get("/tickers/{symbol}")
def get_ticker(symbol: str):
    """Fetch the absolute latest price for a specific symbol"""
    query = f"SELECT * FROM tickers WHERE symbol='{symbol.upper()}'"
    rows = session.execute(query)

    if not rows:
        raise HTTPException(status_code=404, detail="Symbol not found in database")

    row = rows[0]
    return {
        "symbol": row.symbol,
        "last_price": getattr(row, 'last_price', None),
        "price_change": getattr(row, 'price_change', None),
        "price_change_percent": getattr(row, 'price_change_percent', None),
        "volume": getattr(row, 'volume', None)
        }

@app.get("/candlesticks/{symbol}")
def get_candlesticks(symbol: str, limit: int=10):
    """Fetch the latest 1-minute candlestick aggregations"""
    query = f"SELECT * FROM candlestick WHERE symbol='{symbol.upper()}' ORDER BY window_start DESC LIMIT {limit}"
    try:
        rows = session.execute(query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    results = [{
        "timestamp": row.window_start.strftime("%Y-%m-%d %H:%M:%S"),
        "average_price": row.avg_minute_price,
        "volume": row.total_volume_min,
        "volatility_spread": row.max_spread
        } for row in rows]

    return {"symbol": symbol.upper(), "data_points": len(results), "data": results}

@app.get("/trading-pairs")
def get_pairs(limit: int=100):
    """Fetch active trading pairs and their base assets"""
    query = f"SELECT * FROM trading_pairs LIMIT {limit}"
    rows = session.execute(query)

    results = [{
        "symbol": row.symbol,
        "base_asset": getattr(row, 'base_asset', None),
        "quote_asset": getattr(row, 'quote_asset', None),
        "status": getattr(row, 'status', None)
        } for row in rows]

    return {"total_returned": len(results), "data": results}

@app.get("/order-books/{symbol}"}
def get_order_books(symbol: str, limit: int=5):
    """Fetch recent order book depth(Bids/Asks)"""
    query = f"SELECT * FROM order_books WHERE symbol='{symbol.upper()}' ORDER BY lastupdateid DESC LIMIT {limit}"
    try:
        rows = session.execute(query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    results = []
    for row in rows:
        # This maps the columns dynamically(json or flattened price)
        results.append({
            "update_id": getattr(row, 'lastupdateid', None),
            "bid_price": getattr(row, 'bid_price', getattr(row, 'bids', None)),
            "bid_qty": getattr(row, 'bid_qty', None),
            "ask_price": getattr(row, 'ask_price', getattr(row, 'asks', None)),
            "ask_qty": getattr(row, 'ask_qty', None)
            }]

        return {"symbol": symbol.upper(), "depth_points": len(results), "data": results}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
