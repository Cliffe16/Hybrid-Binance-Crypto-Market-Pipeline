import os
import time
import pyspark.sql.functions as F
import requests
from pyspark.sql.avro.functions import from_avro
from src.utils.kafka_utils import read_kafka_topic
from src.utils.spark_utils import spark_session

SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = os.getenv("CASSANDRA_PORT", "9042")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/checkpoints")
KEYSPACE = "binance"
TOPIC_PREFIX = "binance_server.public"

spark = spark_session()

# Optimize memory footprint for the Java runtime
spark.conf.set("spark.sql.shuffle.partitions", "4")  # Drop from 200 to 4
spark.conf.set("spark.sql.streaming.minBatchesToRetain", "2")  # Don't hold old batch metadata in RAM


def fetch_schema(topic_name, retries=20):
    """Fetch the latest Avro value schema from the Confluent Schema Registry."""
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{topic_name}-value/versions/latest"

    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            return response.json()["schema"]
        except (requests.RequestException, KeyError, ValueError) as exc:
            print(f"Schema Registry not ready ({exc}). Retrying in 5 seconds... (Attempt {attempt}/{retries})")
            time.sleep(5)

    raise RuntimeError(f"Could not fetch schema for {topic_name} after {retries} attempts.")


def decode(raw_df, schema):
    """Strip the 5-byte Schema Registry header, decode Avro and unwrap Debezium's `after` block."""
    payload = F.expr("substring(value, 6, length(value) - 5)")
    return (
        raw_df
        .withColumn("avro_data", from_avro(payload, schema))
        .select("avro_data.after.*")
        .filter(F.col("symbol").isNotNull() & (F.trim(F.col("symbol")) != ""))
    )


def top_of_book(column, index):
    """Best level of a bids/asks array stored as a Postgres-style string: index 0 = price, 1 = quantity."""
    return F.get_json_object(F.translate(F.col(column), "{}", "[]"), f"$[0][{index}]")


def write_to_cassandra(df, table):
    return (
        df.writeStream.outputMode("append")
        .format("org.apache.spark.sql.cassandra")
        .option("keyspace", KEYSPACE)
        .option("table", table)
        .option("spark.cassandra.connection.host", CASSANDRA_HOST)
        .option("spark.cassandra.connection.port", CASSANDRA_PORT)
        .option("spark.cassandra.connection.timeoutMS", "60000")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/{table}")
        .trigger(processingTime="30 seconds")
        .start()
    )


def upsert_to_cassandra(df, table):
    """Emit every window that changed in each trigger (update mode) and upsert it by primary key.

    Uses foreachBatch with a plain batch write, so the same (symbol, window_start) row is simply
    overwritten as its window is updated, and a replayed batch is harmless.
    """

    def _write(batch_df, batch_id):
        (
            batch_df.write.format("org.apache.spark.sql.cassandra")
            .mode("append")
            .option("keyspace", KEYSPACE)
            .option("table", table)
            .option("spark.cassandra.connection.host", CASSANDRA_HOST)
            .option("spark.cassandra.connection.port", CASSANDRA_PORT)
            .option("spark.cassandra.connection.timeoutMS", "60000")
            .save()
        )

    return (
        df.writeStream.outputMode("update")
        .foreachBatch(_write)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/{table}")
        .trigger(processingTime="30 seconds")
        .start()
    )


# Fetch schemas
candlestick_schema = fetch_schema(f"{TOPIC_PREFIX}.candlestick_data")
orders_schema = fetch_schema(f"{TOPIC_PREFIX}.order_books")
tickers_schema = fetch_schema(f"{TOPIC_PREFIX}.tickers")
pairs_schema = fetch_schema(f"{TOPIC_PREFIX}.trading_pairs")

# Ingest the Kafka streams
raw_candlestick = read_kafka_topic(f"{TOPIC_PREFIX}.candlestick_data")
raw_orders = read_kafka_topic(f"{TOPIC_PREFIX}.order_books")
raw_tickers = read_kafka_topic(f"{TOPIC_PREFIX}.tickers")
raw_pairs = read_kafka_topic(f"{TOPIC_PREFIX}.trading_pairs")

# Candlesticks: 1-minute windowed aggregates
candlestick_df = (
    decode(raw_candlestick, candlestick_schema)
    .withColumn("price", F.col("close").cast("float"))
    .withColumn("price_high", F.col("high").cast("float"))
    .withColumn("price_low", F.col("low").cast("float"))
    .withColumn("trade_volume", F.col("volume").cast("float"))
    .withColumn("timestamp", F.to_timestamp(F.col("open_time") / 1000))
    .withWatermark("timestamp", "2 minutes")
    .groupBy(F.window(F.col("timestamp"), "1 minute"), F.col("symbol"))
    .agg(
        F.avg("price").alias("avg_minute_price"),
        F.sum("trade_volume").alias("total_volume_min"),
        F.coalesce(F.stddev("price"), F.lit(0.0)).alias("price_stddev"),
        (F.max("price_high") - F.min("price_low")).alias("max_spread"),
    )
    .withColumn("window_start", F.col("window.start"))
    .drop("window")
)

# Price and quantity of the top level
clean_orders = decode(raw_orders, orders_schema).select(
    F.col("symbol"),
    F.col("lastUpdateId").alias("lastupdateid"),
    top_of_book("bids", 0).alias("bid_price"),
    top_of_book("bids", 1).alias("bid_qty"),
    top_of_book("asks", 0).alias("ask_price"),
    top_of_book("asks", 1).alias("ask_qty"),
)

clean_tickers = decode(raw_tickers, tickers_schema).select(
    F.col("symbol"),
    F.col("price").alias("last_price"),
)

clean_pairs = decode(raw_pairs, pairs_schema).select(
    F.col("symbol"),
    F.col("baseAsset").alias("base_asset"),
    F.col("quoteAsset").alias("quote_asset"),
    F.col("status"),
)

# Execute the streams
upsert_to_cassandra(candlestick_df, "candlestick")
write_to_cassandra(clean_orders, "order_books")
write_to_cassandra(clean_tickers, "tickers")
write_to_cassandra(clean_pairs, "trading_pairs")

spark.streams.awaitAnyTermination()
