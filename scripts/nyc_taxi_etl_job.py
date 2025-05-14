import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Print message to confirm job is running
print("Starting NYCTaxiETLJob")

# Read the data from the processed S3 location with calculated distances
bucket_name = "nyc-taxi-analytics-project"
input_path = f"s3://{bucket_name}/processed-data/train.csv"
print(f"Reading data from {input_path}")

try:
    # Read the processed data with distances calculated by Lambda
    taxi_data = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
    
    print(f"Successfully read data. Row count: {taxi_data.count()}")
    print(f"Schema: {taxi_data.schema}")
    
    # Filter by vendor (example: vendor_id = 1) 
    # Note: Adjust this filter based on your data
    filtered_data = taxi_data.filter(col("vendor_id").cast("integer") == 1)
    print(f"Filtered data count: {filtered_data.count()}")
    
    # Calculate aggregations using available columns
    print("Calculating aggregations...")
    
    # Group by pickup date (extract date from pickup_datetime)
    aggregations = filtered_data.withColumn(
        "pickup_date", 
        to_date(col("pickup_datetime"))
    ).groupBy("pickup_date").agg(
        count("*").alias("total_trips"),
        avg("trip_duration").alias("average_trip_duration_seconds"),  # Using trip_duration instead of fare_amount
        sum("haversine_distance").alias("total_distance_km")
    )
    
    # Calculate metrics by hour of day
    hourly_metrics = filtered_data.withColumn(
        "pickup_hour", 
        hour(col("pickup_datetime"))
    ).groupBy("pickup_hour").agg(
        count("*").alias("trip_count"),
        avg("trip_duration").alias("avg_duration_seconds"),
        avg("haversine_distance").alias("avg_distance_km"),
        avg("manhattan_distance").alias("avg_manhattan_distance_km")
    ).orderBy("pickup_hour")
    
    # Calculate top pickup and dropoff locations
    top_pickups = filtered_data.groupBy("pickup_longitude", "pickup_latitude") \
        .count() \
        .orderBy(desc("count")) \
        .limit(100) \
        .withColumnRenamed("count", "pickup_count")
    
    top_dropoffs = filtered_data.groupBy("dropoff_longitude", "dropoff_latitude") \
        .count() \
        .orderBy(desc("count")) \
        .limit(100) \
        .withColumnRenamed("count", "dropoff_count")
    
    print(f"Aggregation results created successfully")
    
    # Write results back to S3
    output_base_path = f"s3://{bucket_name}/aggregations/"
    
    # Write daily aggregations
    daily_output_path = output_base_path + "daily_stats/"
    print(f"Writing daily aggregations to {daily_output_path}")
    aggregations.write.mode("overwrite").parquet(daily_output_path)
    
    # Write hourly metrics
    hourly_output_path = output_base_path + "hourly_stats/"
    print(f"Writing hourly metrics to {hourly_output_path}")
    hourly_metrics.write.mode("overwrite").parquet(hourly_output_path)
    
    # Write top locations
    top_pickups_path = output_base_path + "top_pickup_locations/"
    print(f"Writing top pickup locations to {top_pickups_path}")
    top_pickups.write.mode("overwrite").parquet(top_pickups_path)
    
    top_dropoffs_path = output_base_path + "top_dropoff_locations/"
    print(f"Writing top dropoff locations to {top_dropoffs_path}")
    top_dropoffs.write.mode("overwrite").parquet(top_dropoffs_path)
    
    print("ETL job completed successfully!")
    
except Exception as e:
    print(f"Error in ETL job: {str(e)}")
    import traceback
    print(traceback.format_exc())
    raise e

# End the job
job.commit()