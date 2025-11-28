"""
Speed Layer: Real-time Processing with Spark Structured Streaming
Processes streaming data from Kafka and provides real-time views
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import sys

# Suppress Hadoop warnings on Windows
if sys.platform.startswith('win'):
    import warnings
    warnings.filterwarnings('ignore')

# Configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "terai_weather_stream"
CHECKPOINT_DIR = "./data/speed/checkpoints"
OUTPUT_DIR = "./data/speed/output"

# Create output directories
os.makedirs(CHECKPOINT_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

def create_spark_session():
    """Initialize Spark session with Kafka support"""
    spark = SparkSession.builder \
        .appName("WeatherFloodSpeedLayer") \
        .master("local[1]") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.default.parallelism", "1") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.hadoop.fs.defaultFS", "file:///") \
        .config("spark.hadoop.hadoop.tmp.dir", "./.hadoop_tmp") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def define_schema():
    """Define schema for incoming weather data"""
    return StructType([
        StructField("timestamp", StringType(), True),
        StructField("location_id", StringType(), True),
        StructField("elevation_m", DoubleType(), True),
        StructField("rainfall_mm_per_hr", DoubleType(), True),
        StructField("temperature_c", DoubleType(), True),
        StructField("river_level_m", DoubleType(), True)
    ])

def process_speed_layer():
    """Main speed layer processing logic"""
    
    print("=" * 60)
    print(" SPEED LAYER STARTING - Real-time Processing")
    print("=" * 60)
    
    spark = create_spark_session()
    schema = define_schema()
    
    # Read from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parse JSON data
    weather_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # Add processing timestamp
    weather_df = weather_df.withColumn("processing_time", current_timestamp())
    
    # REAL-TIME ANALYTICS
    
    # 1. Flood Risk Detection (High rainfall + High river level)
    flood_alerts = weather_df \
        .filter((col("rainfall_mm_per_hr") > 5.0) | (col("river_level_m") > 4.5)) \
        .select(
            col("timestamp"),
            col("location_id"),
            col("rainfall_mm_per_hr"),
            col("river_level_m"),
            lit("FLOOD_ALERT").alias("alert_type")
        )
    
    # 2. Real-time Aggregations (5-minute windows)
    windowed_stats = weather_df \
        .withWatermark("processing_time", "10 minutes") \
        .groupBy(
            window(col("processing_time"), "5 minutes"),
            col("location_id")
        ) \
        .agg(
            avg("rainfall_mm_per_hr").alias("avg_rainfall"),
            max("rainfall_mm_per_hr").alias("max_rainfall"),
            avg("river_level_m").alias("avg_river_level"),
            max("river_level_m").alias("max_river_level"),
            count("*").alias("record_count")
        )
    
    # Output Stream 1: Console output for monitoring
    console_query = weather_df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()
    
    # Output Stream 2: Flood alerts to file
    alerts_query = flood_alerts \
        .writeStream \
        .outputMode("append") \
        .format("json") \
        .option("path", f"{OUTPUT_DIR}/flood_alerts") \
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/alerts") \
        .start()
    
    # Output Stream 3: Windowed statistics
    stats_query = windowed_stats \
        .writeStream \
        .outputMode("complete") \
        .format("memory") \
        .queryName("realtime_stats") \
        .start()
    
    print("\n[OK] Speed Layer Active - Processing Real-time Data")
    print(f" Monitoring: Console, Alerts: {OUTPUT_DIR}/flood_alerts")
    print("Press Ctrl+C to stop...\n")
    
    # Keep streams running
    try:
        console_query.awaitTermination()
    except KeyboardInterrupt:
        print("\n Stopping Speed Layer...")
        console_query.stop()
        alerts_query.stop()
        stats_query.stop()
        spark.stop()

if __name__ == "__main__":
    process_speed_layer()