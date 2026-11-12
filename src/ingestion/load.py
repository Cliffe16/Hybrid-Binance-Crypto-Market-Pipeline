import json

import pandas as pd
from sqlalchemy import create_engine, exc, or_, text
from sqlalchemy.dialects.postgresql import insert

from src import config
from src.extraction import extract


def make_insert_method(key_cols, update=True):
    """Build a pandas `to_sql` method that upserts on `key_cols` instead of blindly appending.

    update=True  : existing rows are overwritten, but only when a value actually changed
                   (so re-running the load with identical data produces no CDC events).
    update=False : existing rows are left untouched.
    """

    def _method(table, conn, keys, data_iter):
        rows = [dict(zip(keys, row)) for row in data_iter]
        if not rows:
            return
        stmt = insert(table.table).values(rows)
        value_cols = [c for c in keys if c not in key_cols]
        if update and value_cols:
            stmt = stmt.on_conflict_do_update(
                index_elements=key_cols,
                set_={c: stmt.excluded[c] for c in value_cols},
                where=or_(*[table.table.c[c].is_distinct_from(stmt.excluded[c]) for c in value_cols]),
            )
        else:
            stmt = stmt.on_conflict_do_nothing(index_elements=key_cols)
        conn.execute(stmt)

    return _method


def ensure_table(engine, name, df, key_cols):
    """Create the table if needed and make sure it has a primary key on `key_cols`.

    Tables created by the old append-only loader have no key and may hold duplicates. For those,
    duplicates are removed once and the key is added. REPLICA IDENTITY FULL is set temporarily
    because Debezium's publication would otherwise reject the DELETE on a table without a key.
    """
    missing = [c for c in key_cols if c not in df.columns]
    if missing:
        raise ValueError(f"{name}: key columns {missing} not found in the extracted data")

    df.head(0).to_sql(name, con=engine, if_exists="append", index=False)  # create if absent

    quoted_keys = ", ".join(f'"{c}"' for c in key_cols)
    same_key = " AND ".join(f'a."{c}" = b."{c}"' for c in key_cols)
    with engine.begin() as conn:
        has_pk = conn.execute(
            text("SELECT 1 FROM pg_index WHERE indrelid = CAST(:t AS regclass) AND indisprimary"),
            {"t": name},
        ).first()
        if has_pk:
            return
        conn.execute(text(f'ALTER TABLE "{name}" REPLICA IDENTITY FULL'))
        conn.execute(text(f'DELETE FROM "{name}" a USING "{name}" b WHERE a.ctid < b.ctid AND {same_key}'))
        conn.execute(text(f'ALTER TABLE "{name}" ADD PRIMARY KEY ({quoted_keys})'))
        conn.execute(text(f'ALTER TABLE "{name}" REPLICA IDENTITY DEFAULT'))


def load():
    """Load the raw data into Postgres (idempotent: re-running does not create duplicates)"""
    market_data = extract.extract()
    try:
        # Convert the dictionaries to  dataframes
        ticker_df = pd.DataFrame(market_data.get("ticker", []))

        # Get the nested data
        trading_pairs_raw = market_data.get("trading_pairs", {})

        # Extract the symbol data to void a Value Error
        trading_pairs_df = pd.DataFrame(trading_pairs_raw.get("symbols", []))

        # Define the complex lists and dicts still in the data and convert them into strings for safe loading
        complex_columns = ['filters', 'permissions', 'permissionSets', 'allowedSelfTradePreventionModes', 'orderTypes']

        for col in complex_columns:
            if col in trading_pairs_df.columns:
                trading_pairs_df[col] = trading_pairs_df[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x) #convert the columns to strings if lisst or dict

        # For the othe dictionaries, iterate through the symbols (top-level key)
        # Create empty lists to store the data after each iteration
        ob_list = []
        ohlc_list = []

        for symbol, data in market_data.items():
            if symbol in ["ticker", "trading_pairs"]:
                continue #skip, already converted

            # Extract order book data
            order_book = data.get("order_book")
            if order_book:
                order_book["symbol"] = symbol #tag the dats with the corresponding symbol
                ob_list.append(order_book) #store the data

            # Extract candlestick data
            ohlc = data.get("candlestick")

            # Define candlestick columns (payload only returns values)
            candlestick_columns = [
                "open_time", "open", "high", "low", "close", "volume",
                "close_time", "quote_asset_volume", "number_of_trades",
                "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
            ]

            if ohlc:
                temp_df = pd.DataFrame(ohlc, columns=candlestick_columns) #convert to df to add defined columns; ohlc is a list of lists
                temp_df["symbol"] = symbol
                ohlc_list.append(temp_df)

        # Combine the nested dfs from the for loop
        order_book_df = pd.DataFrame(ob_list)

        if ohlc_list:
            candlestick_df = pd.concat(ohlc_list, ignore_index=True)
        else:
            candlestick_df = pd.DataFrame()

        # Define sqlalchemy engine
        engine = create_engine(config.POSTGRES_URL)

        # (table, dataframe, key columns, overwrite changed rows?, rows per insert)
        loads = [
            ("tickers", ticker_df, ["symbol"], True, 500),
            ("trading_pairs", trading_pairs_df, ["symbol"], True, 200),
            ("order_books", order_book_df, ["symbol", "lastUpdateId"], False, 25),
            ("candlestick_data", candlestick_df, ["symbol", "open_time"], True, 500),
        ]

        for name, df, key_cols, update, chunk in loads:
            if df.empty:
                print(f"{name}\n No rows to load")
                continue
            df = df.drop_duplicates(subset=key_cols, keep="last")
            ensure_table(engine, name, df, key_cols)
            df.to_sql(
                name, con=engine, if_exists="append", index=False,
                chunksize=chunk, method=make_insert_method(key_cols, update),
            )
            print(f"{name}\n Rows: {len(df)} processed")

    except exc.IntegrityError as e:
        print(f"Data Constraint Error: {e}")
        raise
    except exc.OperationalError as e:
        print(f"Database connection Error: {e}")
        raise
    except exc.DataError as e:
        print(f"Value Processing Error: {e}")
        raise
    except Exception as e:
        print(f"Ingestion Failure: {e}")
        raise
    else:
        print("Data Loaded Successfully")
