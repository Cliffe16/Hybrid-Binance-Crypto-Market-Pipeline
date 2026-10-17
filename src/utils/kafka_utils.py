from src.utils.spark_utils import spark_session

spark = spark_session()

def read_kafka_topic(topic_name):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 1000) \
        .load()
