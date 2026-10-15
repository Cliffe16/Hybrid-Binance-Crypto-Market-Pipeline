from sqlalchemy import create_engine, exc
import psycopg2
from src import config
from src.extraction import extract
import pandas as pd
import json

def load():
    """Load the raw data into Postgres"""
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
        ticker_df.to_sql("tickers", con=engine, if_exists="append", index=False, chunksize=500)
        print(f"Tickers\n Rows: {len(ticker_df)} loaded")

        trading_pairs_df.to_sql("trading_pairs", con=engine, if_exists="append", index=False, chunksize=500)
        print(f"Trading Pairs\n Rows: {len(trading_pairs_df)} loaded")
        
        order_book_df.to_sql("order_books", con=engine, if_exists="append", index=False, chunksize=500)
        print(f"Order Books\n Rows: {len(order_book_df)} loaded")

        candlestick_df.to_sql("candlestick_data", con=engine, if_exists="append", index=False, chunksize=500)
        print(f"OHLC Data\n Rows: {len(candlestick_df)} loaded")

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
        
