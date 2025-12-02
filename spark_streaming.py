"""
Spark Structured Streaming job that reads JSON messages from Kafka
and produces real-time alerts and windowed aggregations.

Usage:
  1. Ensure Kafka is running and topic `terai_weather_stream` exists.
  2. Activate your Python venv with pyspark installed.
  3. Run this script: `python spark_streaming.py`

Notes:
  - Requires the Spark Kafka connector. The script configures
    `spark.jars.packages` to download `spark-sql-kafka-0-10` at runtime.
  - Ensure `SPARK_HOME` and `HADOOP_HOME` (with winutils.exe on Windows)
    are configured when running on Windows.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, window, avg, max as spark_max, count, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


# Configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "terai_weather_stream"
# Use absolute paths to avoid confusion about current working directory
OUTPUT_DIR = os.path.abspath(os.path.join(".", "data", "stream", "output"))
CHECKPOINT_DIR = os.path.abspath(os.path.join(".", "data", "stream", "checkpoint"))

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(CHECKPOINT_DIR, exist_ok=True)


def create_spark_session():
    builder = SparkSession.builder.appName("KafkaSparkStructuredStreaming") \
        .master("local[1]") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.default.parallelism", "1") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")

    # On Windows, binding to 127.0.0.1 avoids some networking issues
    builder = builder.config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def define_schema():
    return StructType([
        StructField("timestamp", StringType(), True),
        StructField("location_id", StringType(), True),
        StructField("elevation_m", DoubleType(), True),
        StructField("rainfall_mm_per_hr", DoubleType(), True),
        StructField("temperature_c", DoubleType(), True),
        StructField("river_level_m", DoubleType(), True)
    ])


def main():
    spark = create_spark_session()
    schema = define_schema()

    # Read from Kafka and keep the raw message string
    raw = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # Keep the original message as string so we can display it "as-is"
    raw = raw.withColumn("raw_value", col("value").cast("string"))

    # Parse JSON value from the raw string
    parsed = raw.select("raw_value", from_json(col("raw_value"), schema).alias("data")) \
        .select("raw_value", "data.*") \
        .withColumn("processing_time", current_timestamp())

    # Simple alert detection
    alerts = parsed.filter((col("rainfall_mm_per_hr") > 5.0) | (col("river_level_m") > 4.5)) \
        .select(
            col("timestamp"),
            col("location_id"),
            col("rainfall_mm_per_hr"),
            col("river_level_m"),
            lit("FLOOD_ALERT").alias("alert_type"),
            col("processing_time")
        )

    # Windowed aggregations (5-minute tumbling windows)
    windowed = parsed.withWatermark("processing_time", "10 minutes") \
        .groupBy(window(col("processing_time"), "5 minutes"), col("location_id")) \
        .agg(
            avg("rainfall_mm_per_hr").alias("avg_rainfall"),
            spark_max("rainfall_mm_per_hr").alias("max_rainfall"),
            avg("river_level_m").alias("avg_river_level"),
            spark_max("river_level_m").alias("max_river_level"),
            count("*").alias("record_count")
        )

    # 1) Console output for monitoring: display the original message as-is
    console_query = raw.select("raw_value").writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()

    # 1.5) Save raw messages exactly as produced (JSON lines)
    # Use the text format so each record is written as a single line containing
    # the original JSON payload. This preserves the message "as-is" from the producer.
    raw_output_path = os.path.join(OUTPUT_DIR, "raw_messages")
    raw_checkpoint = os.path.join(CHECKPOINT_DIR, "raw_messages")
    os.makedirs(raw_output_path, exist_ok=True)
    raw_messages_query = raw.select(col("raw_value").alias("value")).writeStream \
        .outputMode("append") \
        .format("text") \
        .option("path", raw_output_path) \
        .option("checkpointLocation", raw_checkpoint) \
        .trigger(processingTime="5 seconds") \
        .start()

    # 2) Alerts to JSON files
    alerts_output_path = os.path.join(OUTPUT_DIR, "alerts")
    alerts_checkpoint = os.path.join(CHECKPOINT_DIR, "alerts")
    os.makedirs(alerts_output_path, exist_ok=True)
    alerts_query = alerts.writeStream \
        .outputMode("append") \
        .format("json") \
        .option("path", alerts_output_path) \
        .option("checkpointLocation", alerts_checkpoint) \
        .trigger(processingTime="5 seconds") \
        .start()

    # 3) Windowed stats to memory (queryName) for simple inspection
    stats_query = windowed.writeStream \
        .outputMode("complete") \
        .format("memory") \
        .queryName("realtime_windowed_stats") \
        .start()

    print("Streaming started. Console + alerts (json) + raw messages + memory table (realtime_windowed_stats)")
    print(f"Raw messages output path: {raw_output_path}")
    print(f"Alerts output path: {alerts_output_path}")
    print(f"Checkpoint root: {CHECKPOINT_DIR}")

    try:
        console_query.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping streams...")
        console_query.stop()
        alerts_query.stop()
        stats_query.stop()
        raw_messages_query.stop()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
