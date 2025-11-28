"""
Batch Layer: Historical Data Processing with PySpark
Processes accumulated data for comprehensive analytics and model training
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import os
from datetime import datetime
import sys

# Suppress Hadoop warnings on Windows
if sys.platform.startswith('win'):
    import warnings
    warnings.filterwarnings('ignore')

# Configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "terai_weather_stream"
BATCH_OUTPUT_DIR = "./data/batch/output"
BATCH_CHECKPOINT = "./data/batch/checkpoints"

# Create directories
os.makedirs(BATCH_OUTPUT_DIR, exist_ok=True)
os.makedirs(BATCH_CHECKPOINT, exist_ok=True)

def create_spark_session():
    """Initialize Spark session for batch processing"""
    import subprocess
    
    spark = SparkSession.builder \
        .appName("WeatherFloodBatchLayer") \
        .master("local[1]") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.adaptive.enabled", "false") \
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
    """Define schema for weather data"""
    return StructType([
        StructField("timestamp", StringType(), True),
        StructField("location_id", StringType(), True),
        StructField("elevation_m", DoubleType(), True),
        StructField("rainfall_mm_per_hr", DoubleType(), True),
        StructField("temperature_c", DoubleType(), True),
        StructField("river_level_m", DoubleType(), True)
    ])

def read_kafka_batch(spark, schema):
    """Read all available data from Kafka"""
    df = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()
    
    # Parse JSON
    weather_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select("data.*", "kafka_timestamp")
    
    return weather_df

def compute_batch_analytics(df):
    """Comprehensive batch analytics"""
    
    print("\n" + "=" * 60)
    print(" BATCH LAYER ANALYTICS")
    print("=" * 60)
    
    # 1. Overall Statistics by Location
    print("\n1 Location-wise Statistics:")
    location_stats = df.groupBy("location_id").agg(
        count("*").alias("total_records"),
        avg("rainfall_mm_per_hr").alias("avg_rainfall"),
        max("rainfall_mm_per_hr").alias("max_rainfall"),
        min("rainfall_mm_per_hr").alias("min_rainfall"),
        avg("temperature_c").alias("avg_temperature"),
        avg("river_level_m").alias("avg_river_level"),
        max("river_level_m").alias("max_river_level")
    ).orderBy("location_id")
    
    location_stats.show(truncate=False)
    
    # 2. Flood Risk Classification
    print("\n2 Flood Risk Classification:")
    flood_risk_df = df.withColumn(
        "flood_risk_level",
        when((col("rainfall_mm_per_hr") > 7.0) & (col("river_level_m") > 5.0), "CRITICAL")
        .when((col("rainfall_mm_per_hr") > 5.0) | (col("river_level_m") > 4.0), "HIGH")
        .when((col("rainfall_mm_per_hr") > 3.0) | (col("river_level_m") > 3.5), "MODERATE")
        .otherwise("LOW")
    )
    
    risk_distribution = flood_risk_df.groupBy("flood_risk_level").count() \
        .orderBy(desc("count"))
    risk_distribution.show()
    
    # 3. Temporal Patterns (if timestamp parsing works)
    print("\n3 Temporal Analysis:")
    try:
        temporal_df = df.withColumn("hour", hour(col("timestamp"))) \
                       .withColumn("date", to_date(col("timestamp")))
        
        hourly_patterns = temporal_df.groupBy("hour").agg(
            avg("rainfall_mm_per_hr").alias("avg_rainfall"),
            avg("river_level_m").alias("avg_river_level")
        ).orderBy("hour")
        
        hourly_patterns.show(24, truncate=False)
    except Exception as e:
        print(f"[WARNING] Temporal analysis skipped: {e}")
    
    # 4. Correlation Analysis
    print("\n4 Correlation Analysis:")
    correlations = df.stat.corr("rainfall_mm_per_hr", "river_level_m")
    print(f"Rainfall vs River Level Correlation: {correlations:.4f}")
    
    # 5. Extreme Events Detection
    print("\n5 Extreme Events (Top 10 Highest Risk):")
    extreme_events = df.orderBy(
        desc("rainfall_mm_per_hr"), 
        desc("river_level_m")
    ).limit(10).select(
        "timestamp", "location_id", "rainfall_mm_per_hr", "river_level_m"
    )
    extreme_events.show(truncate=False)
    
    return {
        "location_stats": location_stats,
        "flood_risk": flood_risk_df,
        "risk_distribution": risk_distribution
    }

def save_batch_views(analytics_results):
    """Save batch views for serving layer"""
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save location statistics
    analytics_results["location_stats"].write \
        .mode("overwrite") \
        .parquet(f"{BATCH_OUTPUT_DIR}/location_stats")
    
    # Save flood risk data
    analytics_results["flood_risk"].write \
        .mode("overwrite") \
        .partitionBy("flood_risk_level") \
        .parquet(f"{BATCH_OUTPUT_DIR}/flood_risk_data")
    
    # Save risk distribution
    analytics_results["risk_distribution"].write \
        .mode("overwrite") \
        .json(f"{BATCH_OUTPUT_DIR}/risk_distribution_{timestamp}.json")
    
    print(f"\n[OK] Batch views saved to: {BATCH_OUTPUT_DIR}")

def run_batch_processing():
    """Main batch processing pipeline"""
    
    print("=" * 60)
    print(" BATCH LAYER STARTING - Historical Processing")
    print("=" * 60)
    
    spark = create_spark_session()
    schema = define_schema()
    
    # Read all historical data
    print("\n Reading historical data from Kafka...")
    weather_df = read_kafka_batch(spark, schema)
    
    total_records = weather_df.count()
    print(f"[OK] Loaded {total_records} records")
    
    if total_records == 0:
        print("[WARNING] No data found in Kafka. Run the producer first!")
        spark.stop()
        return
    
    # Cache for multiple operations
    weather_df.cache()
    
    # Compute analytics
    analytics_results = compute_batch_analytics(weather_df)
    
    # Save results
    save_batch_views(analytics_results)
    
    print("\n" + "=" * 60)
    print("[OK] BATCH LAYER COMPLETED")
    print("=" * 60)
    
    spark.stop()

if __name__ == "__main__":
    run_batch_processing()