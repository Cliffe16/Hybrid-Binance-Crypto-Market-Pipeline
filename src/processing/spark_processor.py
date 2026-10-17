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

def fetch_schema(topic_name, retries=20):
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

# Enrich and transform the data
enriched_candlestick = raw_candlestick \
    .withColumn("avro_data", from_avro(clean_avro_bytes, candlestick_schema)) \
    .select("avro_data.after.*") \
    .filter(col("symbol").isNotNull() & (trim(col("symbol")) != "")) \
    .withColumn("price", col("close").cast("float")) \
    .withColumn("price_high", col("high").cast("float")) \
    .withColumn("price_low", col("low").cast("float")) \
    .withColumn("trade_volume", col("volume").cast("float")) \
    .withColumn("timestamp", to_timestamp(col("open_time") / 1000)) \
    .withWatermark("timestamp", "2 minutes") \
    .groupBy(window(col("timestamp"), "1 minute"), col("symbol")) \
    .agg(
        avg("price").alias("avg_minute_price"),
        sum("trade_volume").alias("total_volume_min"),
        coalesce(stddev("price"), lit(0.0)).alias("price_stddev"),
        (max("price_high") - min("price_low")).alias("max_spread")
    ) 

candlestick_df = enriched_candlestick \
    .withColumn("window_start", col("window.start")) \
    .drop("window")

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
        col("price").alias("last_price")
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
