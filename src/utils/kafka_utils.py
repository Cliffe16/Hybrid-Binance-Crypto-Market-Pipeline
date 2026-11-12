from src.utils.spark_utils import spark_session
import os

spark = spark_session()

# Default to 20,000 to improve throughput while preventing OOM errors
MAX_OFFSETS = int(os.getenv("KAFKA_MAX_OFFSETS", "2000"))
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

def read_kafka_topic(topic_name):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", MAX_OFFSETS) \
        .load()
