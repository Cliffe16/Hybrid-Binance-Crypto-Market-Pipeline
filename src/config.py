import os
from dotenv import load_dotenv
from urllib.parse import quote_plus

load_dotenv()

# Define the api endpoints
base_url = os.getenv("BASE_URL")

price_url = base_url + os.getenv("PRICE")
trading_pairs_url = base_url + os.getenv("TRADING_PAIRS")
order_book_url = base_url + os.getenv("ORDER_BOOK")
ohlc_url = base_url + os.getenv("OHLC")

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASS = os.getenv("POSTGRES_PASS")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")

POSTGRES_CONNECTION = {
    "host": POSTGRES_HOST,
    "user": POSTGRES_USER,
    "password": POSTGRES_PASS,
    "database": POSTGRES_DB,
    "port": POSTGRES_PORT
    }

# Encode credentials 
encoded_user = quote_plus(POSTGRES_USER) if POSTGRES_USER else ""
encoded_pass = quote_plus(POSTGRES_PASS) if POSTGRES_PASS else ""

POSTGRES_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASS}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

