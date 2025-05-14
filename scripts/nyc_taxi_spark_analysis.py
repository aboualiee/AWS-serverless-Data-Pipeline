from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import math

# Initialize Spark session
spark = SparkSession.builder \
    .appName("NYC Taxi Comprehensive Analysis") \
    .getOrCreate()

# Define UDF functions for distance calculations
def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate haversine distance between two points"""
    # Convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(math.radians, [lon1, lat1, lon2, lat2])
    
    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    r = 6371  # Radius of earth in kilometers
    return c * r

def manhattan_distance(lat1, lon1, lat2, lon2):
    """Calculate Manhattan distance (approximation in urban grid)"""
    # Constants for NYC (approximate conversion from lat/lon to km)
    lat_km = 111  # 1 degree latitude = ~111 km
    lon_km = 85   # 1 degree longitude = ~85 km at NYC's latitude
    
    # Calculate distance
    lat_dist = abs(lat2 - lat1) * lat_km
    lon_dist = abs(lon2 - lon1) * lon_km
    
    return lat_dist + lon_dist

# Register UDFs
spark.udf.register("haversine_distance_udf", haversine_distance)
spark.udf.register("manhattan_distance_udf", manhattan_distance)

# Read the processed data that already has distances calculated
print("Reading data from S3...")
bucket_name = "nyc-taxi-analytics-project"
taxi_data = spark.read.csv(f"s3://{bucket_name}/processed-data/train.csv", 
                           header=True, inferSchema=True)

# Print schema and count
print(f"Data schema: {taxi_data.schema}")
print(f"Total records: {taxi_data.count()}")

# Perform additional analyses that go beyond the Glue job

# 1. Find busiest hours of the day
print("Analyzing busiest hours...")
busiest_hours = taxi_data.withColumn("pickup_hour", hour(col("pickup_datetime"))) \
    .groupBy("pickup_hour") \
    .count() \
    .orderBy(desc("count"))

busiest_hours.show(24)

# 2. Analyze trip durations vs distance to identify anomalies
print("Analyzing trip durations vs distance...")
duration_vs_distance = taxi_data.select(
    "trip_duration", 
    "haversine_distance",
    "manhattan_distance",
    expr("trip_duration / haversine_distance").alias("seconds_per_km")
) \
.filter(col("haversine_distance") > 0)  # Avoid division by zero

# Find outliers - very slow trips (could indicate traffic or inefficient routes)
slow_trips = duration_vs_distance.filter(col("seconds_per_km") > 300)  # More than 5 min per km
slow_trips.cache()  # Cache for multiple uses

print(f"Number of unusually slow trips: {slow_trips.count()}")
if slow_trips.count() > 0:
    slow_trips.sort(desc("seconds_per_km")).show(10)

# 3. Find routes with high discrepancy between Manhattan and haversine distance
print("Analyzing Manhattan vs haversine distance ratio...")
distance_ratio = taxi_data.withColumn(
    "manhattan_haversine_ratio", 
    col("manhattan_distance") / col("haversine_distance")
) \
.filter(col("haversine_distance") > 1.0)  # Filter out very short trips

# High ratio means the Manhattan distance is much longer than direct distance
high_ratio_routes = distance_ratio.filter(col("manhattan_haversine_ratio") > 2.0)
high_ratio_routes.cache()

print(f"Routes with high Manhattan/haversine ratio: {high_ratio_routes.count()}")
if high_ratio_routes.count() > 0:
    high_ratio_routes.sort(desc("manhattan_haversine_ratio")).show(10)

# 4. Passenger count analysis
print("Analyzing by passenger count...")
by_passenger_count = taxi_data.groupBy("passenger_count") \
    .agg(
        count("*").alias("trip_count"),
        avg("trip_duration").alias("avg_duration"),
        avg("haversine_distance").alias("avg_distance")
    ) \
    .orderBy("passenger_count")

by_passenger_count.show()

# 5. Calculate vendor performance metrics
print("Analyzing vendor performance...")
vendor_performance = taxi_data.groupBy("vendor_id") \
    .agg(
        count("*").alias("trip_count"),
        avg("trip_duration").alias("avg_duration"),
        avg("haversine_distance").alias("avg_distance"),
        expr("avg(haversine_distance / trip_duration * 3600)").alias("avg_speed_kmh")
    )

vendor_performance.show()

# Save results to S3
print("Saving results to S3...")
output_path = f"s3://{bucket_name}/emr-results/"

# Save busiest hours
busiest_hours.write.mode("overwrite").parquet(f"{output_path}busiest_hours/")

# Save slow trips analysis
slow_trips.write.mode("overwrite").parquet(f"{output_path}slow_trips/")

# Save high ratio routes
high_ratio_routes.write.mode("overwrite").parquet(f"{output_path}high_ratio_routes/")

# Save passenger count analysis
by_passenger_count.write.mode("overwrite").parquet(f"{output_path}by_passenger_count/")

# Save vendor performance
vendor_performance.write.mode("overwrite").parquet(f"{output_path}vendor_performance/")

print("Analysis complete! All results saved to S3.")