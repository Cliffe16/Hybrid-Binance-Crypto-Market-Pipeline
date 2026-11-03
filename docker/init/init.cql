CREATE KEYSPACE IF NOT EXISTS binance 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE binance;

CREATE TABLE IF NOT EXISTS candlestick (
    symbol text,
    window_start timestamp,
    avg_minute_price double,
    total_volume_min double,
    price_stddev double,
    max_spread float,
    PRIMARY KEY (symbol, window_start)
);

CREATE TABLE IF NOT EXISTS order_books (
    symbol text,
    lastupdateid bigint,
    bid_price text,
    bid_qty text,
    ask_price text,
    ask_qty text,
    PRIMARY KEY (symbol, lastupdateid)
);

CREATE TABLE IF NOT EXISTS tickers (
    symbol text PRIMARY KEY,
    price_change text,
    price_change_percent text,
    last_price text,
    volume text
);

CREATE TABLE IF NOT EXISTS trading_pairs (
    symbol text PRIMARY KEY,
    base_asset text,
    quote_asset text,
    status text
);
