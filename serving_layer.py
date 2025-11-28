"""
Serving Layer: Combines batch and speed layer views for queries
Provides unified interface for querying weather and flood data
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os
import time

# Configuration
BATCH_OUTPUT_DIR = "./data/batch/output"
SPEED_OUTPUT_DIR = "./data/speed/output"

def create_spark_session():
    """Initialize Spark session"""
    spark = SparkSession.builder \
        .appName("WeatherFloodServingLayer") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

class ServingLayer:
    """Serving layer that combines batch and speed views"""
    
    def __init__(self):
        self.spark = create_spark_session()
        self.batch_views = {}
        self.load_batch_views()
    
    def load_batch_views(self):
        """Load pre-computed batch views"""
        print(" Loading Batch Views...")
        
        try:
            # Load location statistics
            if os.path.exists(f"{BATCH_OUTPUT_DIR}/location_stats"):
                self.batch_views['location_stats'] = self.spark.read.parquet(
                    f"{BATCH_OUTPUT_DIR}/location_stats"
                )
                print("[OK] Loaded location statistics")
            
            # Load flood risk data
            if os.path.exists(f"{BATCH_OUTPUT_DIR}/flood_risk_data"):
                self.batch_views['flood_risk'] = self.spark.read.parquet(
                    f"{BATCH_OUTPUT_DIR}/flood_risk_data"
                )
                print("[OK] Loaded flood risk data")
            
            if not self.batch_views:
                print("[WARNING] No batch views found. Run batch layer first!")
        
        except Exception as e:
            print(f"[WARNING] Error loading batch views: {e}")
    
    def get_realtime_alerts(self):
        """Get recent flood alerts from speed layer"""
        print("\n Recent Flood Alerts (Speed Layer):")
        
        alert_path = f"{SPEED_OUTPUT_DIR}/flood_alerts"
        
        if os.path.exists(alert_path):
            try:
                alerts_df = self.spark.read.json(alert_path)
                recent_alerts = alerts_df.orderBy(desc("timestamp")).limit(10)
                
                if recent_alerts.count() > 0:
                    recent_alerts.show(truncate=False)
                    return recent_alerts
                else:
                    print("No alerts found")
            except Exception as e:
                print(f"Error reading alerts: {e}")
        else:
            print("No alert data available yet")
        
        return None
    
    def get_location_summary(self, location_id=None):
        """Get comprehensive location summary (batch + speed)"""
        print("\n Location Summary:")
        
        if 'location_stats' not in self.batch_views:
            print("[WARNING] Batch statistics not available")
            return None
        
        stats_df = self.batch_views['location_stats']
        
        if location_id:
            result = stats_df.filter(col("location_id") == location_id)
        else:
            result = stats_df
        
        result.show(truncate=False)
        return result
    
    def get_flood_risk_summary(self):
        """Get flood risk distribution"""
        print("\n[WARNING] Flood Risk Distribution:")
        
        if 'flood_risk' not in self.batch_views:
            print("[WARNING] Flood risk data not available")
            return None
        
        risk_summary = self.batch_views['flood_risk'].groupBy("flood_risk_level").count() \
            .orderBy(desc("count"))
        
        risk_summary.show()
        return risk_summary
    
    def get_high_risk_locations(self):
        """Identify locations with highest flood risk"""
        print("\n High Risk Locations:")
        
        if 'flood_risk' not in self.batch_views:
            print("[WARNING] Flood risk data not available")
            return None
        
        high_risk = self.batch_views['flood_risk'] \
            .filter(col("flood_risk_level").isin(["HIGH", "CRITICAL"])) \
            .groupBy("location_id", "flood_risk_level").count() \
            .orderBy(desc("count"))
        
        high_risk.show()
        return high_risk
    
    def query_custom(self, query_type, **kwargs):
        """Custom query interface"""
        queries = {
            "alerts": self.get_realtime_alerts,
            "location": lambda: self.get_location_summary(kwargs.get('location_id')),
            "risk_summary": self.get_flood_risk_summary,
            "high_risk": self.get_high_risk_locations
        }
        
        if query_type in queries:
            return queries[query_type]()
        else:
            print(f"Unknown query type: {query_type}")
            return None
    
    def interactive_menu(self):
        """Interactive query menu"""
        while True:
            print("\n" + "=" * 60)
            print(" WEATHER & FLOOD RISK SERVING LAYER")
            print("=" * 60)
            print("1. View Recent Flood Alerts (Speed Layer)")
            print("2. View Location Statistics (Batch Layer)")
            print("3. View Flood Risk Distribution")
            print("4. View High Risk Locations")
            print("5. Refresh Batch Views")
            print("0. Exit")
            print("=" * 60)
            
            choice = input("\nEnter your choice: ").strip()
            
            if choice == "1":
                self.get_realtime_alerts()
            elif choice == "2":
                location = input("Enter location ID (or press Enter for all): ").strip()
                self.get_location_summary(location if location else None)
            elif choice == "3":
                self.get_flood_risk_summary()
            elif choice == "4":
                self.get_high_risk_locations()
            elif choice == "5":
                self.load_batch_views()
            elif choice == "0":
                print("Exiting...")
                break
            else:
                print("Invalid choice!")
            
            time.sleep(1)
    
    def stop(self):
        """Stop Spark session"""
        self.spark.stop()

def main():
    """Main serving layer interface"""
    print("=" * 60)
    print(" SERVING LAYER STARTING")
    print("=" * 60)
    
    serving = ServingLayer()
    
    try:
        serving.interactive_menu()
    except KeyboardInterrupt:
        print("\n\n Serving layer stopped by user")
    finally:
        serving.stop()

if __name__ == "__main__":
    main()