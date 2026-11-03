import requests
import time
from src.utils.spark_utils import spark_session
from pyspark.sql.functions import *
from pyspark.sql.avro.functions import from_avro
from src.utils.kafka_utils import read_kafka_topic

spark = spark_session()

# Optimize memory footprint for java runtime
spark.conf.set("spark.sql.shuffle.partitions", "4") # Drop from 200 to 4
spark.conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
spark.conf.set("spark.sql.streaming.minBatchesToRetain", "2") # Don't hold old batch metadata in RAM

# Increased retries to 100 to prevent cold-start crashes while Debezium initializes
def fetch_schema(topic_name, retries=100):
    """Fetches the latest Avro schema from the Confluent Schema Registry."""
    url = f"http://schema-registry:8081/subjects/{topic_name}-value/versions/latest"

    for _try in range(retries):
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()["schema"]
        except:
            print(f"Schema Registry not ready. Retrying in 5 seconds... (Attempt {_try+1}/{retries})")
            time.sleep(5)

    raise Exception(f"Fatal: Could not connect to Schema Registry for {topic_name} after {retries} attempts.")

candlestick_schema = fetch_schema("binance_server.public.candlestick_data")
orders_schema = fetch_schema("binance_server.public.order_books")
tickers_schema = fetch_schema("binance_server.public.tickers")
pairs_schema = fetch_schema("binance_server.public.trading_pairs")

# Ingest the kafka streams
raw_candlestick = read_kafka_topic("binance_server.public.candlestick_data")
raw_orders = read_kafka_topic("binance_server.public.order_books")
raw_tickers = read_kafka_topic("binance_server.public.tickers")
raw_pairs = read_kafka_topic("binance_server.public.trading_pairs")

# Slice off the 5-byte Confluent Schema Registry header
clean_avro_bytes = expr("substring(value, 6, length(value)-5)")

# Removed watermark and windowing. Mapped raw data directly to Cassandra schema columns.
enriched_candlestick = raw_candlestick \
    .withColumn("avro_data", from_avro(clean_avro_bytes, candlestick_schema)) \
    .select("avro_data.after.*") \
    .filter(col("symbol").isNotNull() & (trim(col("symbol")) != "")) \
    .withColumn("price", col("close").cast("float")) \
    .withColumn("price_high", col("high").cast("float")) \
    .withColumn("price_low", col("low").cast("float")) \
    .withColumn("trade_volume", col("volume").cast("float")) \
    .withColumn("window_start", to_timestamp(col("open_time") / 1000)) \
    .withColumn("avg_minute_price", col("price")) \
    .withColumn("total_volume_min", col("trade_volume")) \
    .withColumn("price_stddev", lit(0.0)) \
    .withColumn("max_spread", col("price_high") - col("price_low"))

candlestick_df = enriched_candlestick.select(
    "symbol", 
    "window_start", 
    "avg_minute_price", 
    "total_volume_min", 
    "price_stddev", 
    "max_spread"
)

clean_orders = raw_orders \
    .withColumn("avro_data", from_avro(clean_avro_bytes, orders_schema)) \
    .select("avro_data.after.*") \
    .filter(col("symbol").isNotNull() & (trim(col("symbol")) != "")) \
    .withColumnRenamed("lastUpdateId", "lastupdateid") \
    .select(
        col("symbol"),
        col("lastUpdateId").alias("lastupdateid"),
        get_json_object(translate(col("bids"), "{}", "[]"),  "$[0][1]").alias("bid_price"),
        get_json_object(translate(col("bids"), "{}", "[]"),  "$[0][1]").alias("bid_qty"),
        get_json_object(translate(col("asks"), "{}", "[]"),  "$[0][1]").alias("ask_price"),
        get_json_object(translate(col("asks"), "{}", "[]"),  "$[0][1]").alias("ask_qty")
    )

clean_tickers = raw_tickers \
    .withColumn("avro_data", from_avro(clean_avro_bytes, tickers_schema)) \
    .select("avro_data.after.*") \
    .filter(col("symbol").isNotNull() & (trim(col("symbol")) != "")) \
    .select(
        col("symbol"),
        col("priceChange").alias("price_change"),
        col("priceChangePercent").alias("price_change_percent"),
        col("price").alias("last_price"),
        col("volume")
    )

clean_pairs = raw_pairs \
    .withColumn("avro_data", from_avro(clean_avro_bytes, pairs_schema)) \
    .select("avro_data.after.*") \
    .filter(col("symbol").isNotNull() & (trim(col("symbol")) != "")) \
    .select(
        col("symbol"),
        col("baseAsset").alias("base_asset"),
        col("quoteAsset").alias("quote_asset"),
        col("status")
    )


# Execute the streams 
query_candles = candlestick_df.writeStream.outputMode("append") \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "binance") \
    .option("table", "candlestick") \
    .option("spark.cassandra.connection.host", "cassandra") \
    .option("spark.cassandra.connection.port", "9042") \
    .option("checkpointLocation", "/tmp/checkpoints/candlestick") \
    .option("spark.cassandra.connection.timeoutMS", "60000") \
    .option("spark.cassandra.output.ttl", "172800") \
    .trigger(processingTime="30 seconds") \
    .start()

query_orders = clean_orders.writeStream.outputMode("append") \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "binance") \
    .option("table", "order_books") \
    .option("spark.cassandra.connection.host", "cassandra") \
    .option("spark.cassandra.connection.port", "9042") \
    .option("checkpointLocation", "/tmp/checkpoints/order_books") \
    .option("spark.cassandra.connection.timeoutMS", "60000") \
	.option("spark.cassandra.output.ttl", "172800") \
    .trigger(processingTime="30 seconds") \
    .start()


query_tickers = clean_tickers.writeStream.outputMode("append") \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "binance") \
    .option("table", "tickers") \
    .option("spark.cassandra.connection.host", "cassandra") \
    .option("spark.cassandra.connection.port", "9042") \
    .option("checkpointLocation", "/tmp/checkpoints/tickers") \
    .option("spark.cassandra.connection.timeoutMS", "60000") \
    .option("spark.cassandra.output.ttl", "172800") \
    .trigger(processingTime="30 seconds") \
    .start()

query_pairs = clean_pairs.writeStream.outputMode("append") \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "binance") \
    .option("table", "trading_pairs") \
    .option("spark.cassandra.connection.host", "cassandra") \
    .option("spark.cassandra.connection.port", "9042") \
    .option("checkpointLocation", "/tmp/checkpoints/trading_pairs") \
    .option("spark.cassandra.connection.timeoutMS", "60000") \
    .trigger(processingTime="30 seconds") \
    .start()

spark.streams.awaitAnyTermination()
