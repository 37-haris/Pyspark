"""
Spark Structured Streaming job:
reads the Kafka topic `random_names`, parses the JSON, and writes each row
into the Cassandra table spark_streaming.random_names.
"""
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import FloatType, StringType, StructField, StructType

logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(funcName)s:%(levelname)s:%(message)s")

KAFKA_SERVERS = "kafka1:19092,kafka2:19093,kafka3:19094"


def create_spark_session() -> SparkSession:
    """Spark session configured to talk to Cassandra.
    (The Kafka and Cassandra connector packages are passed with --packages in spark-submit.)"""
    spark = (SparkSession.builder
             .appName("SparkStructuredStreaming")
             .config("spark.cassandra.connection.host", "cassandra")
             .config("spark.cassandra.connection.port", "9042")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    logging.info("Spark session created successfully")
    return spark


def create_initial_dataframe(spark: SparkSession):
    """Streaming DataFrame reading the Kafka topic from the beginning."""
    df = (spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", KAFKA_SERVERS)
          .option("subscribe", "random_names")
          .option("startingOffsets", "earliest")
          .load())
    logging.info("Initial dataframe created successfully")
    return df


def create_final_dataframe(df):
    """Kafka gives binary key/value columns: cast value to string and parse the JSON."""
    schema = StructType([
        StructField("full_name", StringType(), False),
        StructField("gender",    StringType(), False),
        StructField("location",  StringType(), False),
        StructField("city",      StringType(), False),
        StructField("country",   StringType(), False),
        StructField("postcode",  StringType(), False),
        StructField("latitude",  FloatType(),  False),
        StructField("longitude", FloatType(),  False),
        StructField("email",     StringType(), False),
    ])
    return (df.selectExpr("CAST(value AS STRING) AS value")
              .select(from_json(col("value"), schema).alias("data"))
              .select("data.*"))


def start_streaming(df):
    """Writes the stream into Cassandra (append mode)."""
    logging.info("Streaming is being started...")
    query = (df.writeStream
               .format("org.apache.spark.sql.cassandra")
               .outputMode("append")
               .option("checkpointLocation", "/tmp/checkpoints/random_names")
               .options(table="random_names", keyspace="spark_streaming")
               .start())
    query.awaitTermination()


if __name__ == "__main__":
    spark = create_spark_session()
    start_streaming(create_final_dataframe(create_initial_dataframe(spark)))
