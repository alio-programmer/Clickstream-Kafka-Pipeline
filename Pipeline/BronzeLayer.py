#Here I will be upserting data coming from kafka to a datalake 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, unix_timestamp
from pyspark.sql.types import StructField, StructType, StringType, TimestampType, LongType
import shutil
import os

HUDI_RECORD_KEY = "event_id" 
HUDI_PRECOMBINE_FIELD = "timestamp_ms" # Used to determine the latest record

CLICKSTREAM_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("action", StringType(), True),
    # Parse timestamp as String, then convert later
    StructField("timestamp", StringType(), True), 
    StructField("page_url", StringType(), True),
    StructField("session_id", StringType(), True)
])

# Kafka Broker and Topic
KAFKA_BROKERS = "kafka1:29092"  # Use internal Docker network
TOPIC = "clickstream_events"
BRONZE_PATH = "/data/bronze/clickstream"
HUDI_TABLE_NAME = "clickstream_bronze" 

HUDI_COMMON_OPTIONS = {
    # Hudi Table Name and Path
    'hoodie.table.name': HUDI_TABLE_NAME,
    'path': BRONZE_PATH,
    
    # Primary Key Fields
    'hoodie.datasource.write.recordkey.field': HUDI_RECORD_KEY,
    'hoodie.datasource.write.precombine.field': HUDI_PRECOMBINE_FIELD,

    # Operation and Storage Type
    'hoodie.datasource.write.operation': 'upsert', # The operation (insert, upsert, bulk_insert)
    'hoodie.datasource.write.table.type': 'COPY_ON_WRITE', # Or MERGE_ON_READ for faster writes
    
    # Deduplication and File Management
    'hoodie.cleaner.commits.retain': 2,
    'hoodie.keep.min.commits': 3,
    'hoodie.keep.max.commits': 4,
}

SPARK_VERSION = "3.5.0"

def clear_checkpoint(checkpoint_path):
    """Clear corrupted checkpoint to resolve offset serialization issues"""
    try:
        if os.path.exists(checkpoint_path):
            shutil.rmtree(checkpoint_path)
            print(f"Cleared corrupted checkpoint at {checkpoint_path}")
            return True
    except Exception as e:
        print(f"Warning: Could not clear checkpoint: {e}")
    return False

def write_hudi_batch(microBatchDF, batchId):
# 1. Parse the JSON and keep the original Kafka metadata columns
    # microBatchDF contains: (value, timestamp, offset, etc.)
    parsed_df = microBatchDF.select(
        from_json(col("json_data"), CLICKSTREAM_SCHEMA).alias("data"),
        col("kafka_ingestion_timestamp"),
        col("kafka_offset")
    )
    
    # 2. Extract the JSON fields (from 'data') and retain the Kafka metadata
    # The output columns are now: (event_id, ip_address, ..., session_id, kafka_ingestion_timestamp, kafka_offset)
    final_df_base = parsed_df.select(
        col("data.*"), 
        col("kafka_ingestion_timestamp"), 
        col("kafka_offset")
    )

    # 3. Add the required Hudi pre-combine field (timestamp in milliseconds)
    # The 'timestamp' column used here is the one extracted from the JSON payload (data.*)
    final_df = final_df_base.withColumn(
        HUDI_PRECOMBINE_FIELD, 
        (unix_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ssXXX") * 1000).cast(LongType())
    )

    # 4. Write/Upsert to Hudi using the defined options
    try:
        final_df.write.format("hudi") \
            .options(**HUDI_COMMON_OPTIONS) \
            .mode("append") \
            .save()
        print(f"Batch {batchId} successfully upserted to Hudi at {BRONZE_PATH}")
    except Exception as e:
        print(f"!!! Error writing Hudi batch {batchId}: {e}")
        raise

def start_spark_session():
    try:
        spark = SparkSession.builder \
            .appName("Clickstream Bronze Layer") \
            .master("local[1]") \
            .config("spark.sql.shuffle.partitions", "1") \
            .config("spark.sql.streaming.checkpointLocation.required", "false") \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.driver.memory", "512m") \
            .config("spark.network.timeout", "300s") \
            .config("spark.executor.heartbeatInterval", "60s") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
        
        
        spark.sparkContext.setLogLevel("WARN")
    except Exception as e:
        print(f"Error creating Spark Session: {e}")
        raise

    print("Spark Session initialized for Bronze Layer.")
    return spark

def start_bronze_layer_ingestion(spark):
    try:
        try:
            raw_stream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
            .option("subscribe", TOPIC) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()
        except Exception as e:
            print(f"Error reading from Kafka: {e}")
            raise

        # Explicitly cast offset to string to avoid serialization issues
        df_bronze = raw_stream.select(
            col("value").cast(StringType()).alias("json_data"),
            col("timestamp").alias("kafka_ingestion_timestamp"),
            col("offset").cast(LongType()).alias("kafka_offset")
        )

        checkpoint_location = f"{BRONZE_PATH}/_checkpoint"
        
        # Clear corrupted checkpoint if it exists
        checkpoint_location = f"{BRONZE_PATH}/_checkpoint"
        clear_checkpoint(checkpoint_location)

        try:
            query = df_bronze.writeStream \
                .foreachBatch(write_hudi_batch) \
                .outputMode("update") \
                .option("checkpointLocation", checkpoint_location) \
                .trigger(processingTime="10 seconds") \
                .start()
        except Exception as e:
            print(f"Error starting Hudi streaming query: {e}")
            raise
    except Exception as e:
        print(f"Error in Bronze Layer ingestion setup: {e}")
        raise        

    print(f"Bronze Layer (Hudi) writing to: {BRONZE_PATH}")
    query.awaitTermination()

if __name__ == "__main__":
    try:
        spark = start_spark_session()
    except KeyboardInterrupt:
        print("Spark Session initialization interrupted by user.")
        exit(0)
    except Exception as e:
        print(f"Error initializing Spark Session: {e}")
        exit(1)

    try:
        start_bronze_layer_ingestion(spark)
    except KeyboardInterrupt:
        print("Bronze Layer ingestion interrupted by user.")
    except Exception as e:
        print(f"Error in Bronze Layer ingestion: {e}")
        exit(1)