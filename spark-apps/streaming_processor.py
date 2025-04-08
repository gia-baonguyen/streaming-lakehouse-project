# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, window, count, approx_count_distinct, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import os

# --- Configuration (Using service names in Docker network) ---
KAFKA_BROKERS = "kafk-1:9092,kafk-2:9092,kafk-3:9092" # Internal listeners
ORDERS_TOPIC = "orders_topic"
CLICKS_TOPIC = "clicks_topic"
ERRORS_TOPIC = "errors_topic"

MINIO_ENDPOINT = "http://minio:9000"
# Credentials will be picked from Spark Session config, providing defaults here if needed
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")
MINIO_BUCKET = "warehouse" # Bucket created in MinIO entrypoint

NESSIE_URI = "http://nessie:19120/api/v1"
NESSIE_REF = "main"
ICEBERG_CATALOG_NAME = "nessie_minio_catalog" # Catalog name used in Spark Conf
ICEBERG_WAREHOUSE_PATH = f"s3a://{MINIO_BUCKET}/iceberg_nessie_warehouse" # Path for data files on MinIO

DB_NAME = "ecommerce_db"
ORDERS_TABLE = f"{ICEBERG_CATALOG_NAME}.{DB_NAME}.orders_processed"
CLICKS_TABLE = f"{ICEBERG_CATALOG_NAME}.{DB_NAME}.clicks_processed"
ERRORS_TABLE = f"{ICEBERG_CATALOG_NAME}.{DB_NAME}.errors_processed"
ORDERS_METRICS_TABLE = f"{ICEBERG_CATALOG_NAME}.{DB_NAME}.orders_revenue_per_minute"
ACTIVE_USERS_TABLE = f"{ICEBERG_CATALOG_NAME}.{DB_NAME}.active_users_per_window"
ERRORS_METRICS_TABLE = f"{ICEBERG_CATALOG_NAME}.{DB_NAME}.errors_per_minute"
ALERTS_TABLE = f"{ICEBERG_CATALOG_NAME}.{DB_NAME}.alerts"

# Checkpoint locations inside Spark container (temporary)
CHECKPOINT_BASE_DIR = "/tmp/spark_checkpoints_nessie"
CHECKPOINT_ORDERS = f"{CHECKPOINT_BASE_DIR}/orders"
CHECKPOINT_CLICKS = f"{CHECKPOINT_BASE_DIR}/clicks"
CHECKPOINT_ERRORS = f"{CHECKPOINT_BASE_DIR}/errors"
CHECKPOINT_ORDERS_METRICS = f"{CHECKPOINT_BASE_DIR}/orders_metrics"
CHECKPOINT_ACTIVE_USERS = f"{CHECKPOINT_BASE_DIR}/active_users"
CHECKPOINT_ERRORS_METRICS = f"{CHECKPOINT_BASE_DIR}/errors_metrics"
CHECKPOINT_ALERTS = f"{CHECKPOINT_BASE_DIR}/alerts"

print("Initializing Spark Session...")
# --- Initialize Spark Session ---
# Note: Ensure packages are provided during spark-submit
spark = SparkSession.builder \
    .appName("RealtimeEcommerceMonitoring") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions") \
    .config(f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
    .config(f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.warehouse", ICEBERG_WAREHOUSE_PATH) \
    .config(f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.uri", NESSIE_URI) \
    .config(f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.ref", NESSIE_REF) \
    .config(f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.authentication.type", "NONE") \
    .config(f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config(f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.s3.endpoint", MINIO_ENDPOINT) \
    .config(f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.s3.path-style-access", "true") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_BASE_DIR) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN") # Reduce verbosity
print("Spark Session Created. Ensuring Namespace exists...")
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG_NAME}.{DB_NAME}") # Use NAMESPACE for Iceberg v1.0+
print(f"Namespace {DB_NAME} ensured in catalog {ICEBERG_CATALOG_NAME}")

# --- EXPLICITLY CREATE ICEBERG TABLES (window_* columns renamed) ---
print("Ensuring Iceberg tables exist...")

# orders_processed table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ORDERS_TABLE} (
    order_id STRING,
    user_id STRING,
    product_id STRING,
    quantity INT,
    price DOUBLE,
    order_total DOUBLE,
    status STRING,
    order_ts TIMESTAMP
)
USING iceberg
TBLPROPERTIES ('format-version'='2')
""")
print(f"Table {ORDERS_TABLE} ensured.")

# clicks_processed table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CLICKS_TABLE} (
    click_id STRING,
    user_id STRING,
    url STRING,
    device_type STRING,
    click_ts TIMESTAMP
)
USING iceberg
TBLPROPERTIES ('format-version'='2')
""")
print(f"Table {CLICKS_TABLE} ensured.")

# errors_processed table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ERRORS_TABLE} (
    error_id STRING,
    service_name STRING,
    error_message STRING,
    severity STRING,
    error_ts TIMESTAMP
)
USING iceberg
TBLPROPERTIES ('format-version'='2')
""")
print(f"Table {ERRORS_TABLE} ensured.")

# orders_revenue_per_minute table (columns renamed, no partitioning DDL)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ORDERS_METRICS_TABLE} (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    orders_count BIGINT,
    total_revenue DOUBLE
)
USING iceberg
TBLPROPERTIES ('format-version'='2')
""")
print(f"Table {ORDERS_METRICS_TABLE} ensured.")

# active_users_per_window table (columns renamed, no partitioning DDL)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ACTIVE_USERS_TABLE} (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    active_users_approx BIGINT
)
USING iceberg
TBLPROPERTIES ('format-version'='2')
""")
print(f"Table {ACTIVE_USERS_TABLE} ensured.")

# errors_per_minute table (columns renamed, no partitioning DDL)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ERRORS_METRICS_TABLE} (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    service_name STRING,
    severity STRING,
    error_count BIGINT
)
USING iceberg
TBLPROPERTIES ('format-version'='2')
""")
print(f"Table {ERRORS_METRICS_TABLE} ensured.")

# alerts table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ALERTS_TABLE} (
    alert_id STRING,
    alert_type STRING,
    description STRING,
    alert_timestamp TIMESTAMP,
    metric_value DOUBLE
)
USING iceberg
TBLPROPERTIES ('format-version'='2')
""")
print(f"Table {ALERTS_TABLE} ensured.")
print("All Iceberg tables ensured.")
# --- END OF TABLE CREATION ---


# --- Define Schemas ---
orders_schema = StructType([
    StructField("order_id", StringType()), StructField("user_id", StringType()),
    StructField("product_id", StringType()), StructField("quantity", IntegerType()),
    StructField("price", DoubleType()), StructField("order_time", StringType()), # Kafka sends as string
    StructField("status", StringType())
])

clicks_schema = StructType([
    StructField("click_id", StringType()), StructField("user_id", StringType()),
    StructField("url", StringType()), StructField("device_type", StringType()),
    StructField("click_time", StringType()) # Kafka sends as string
])

errors_schema = StructType([
    StructField("error_id", StringType()), StructField("service_name", StringType()),
    StructField("error_message", StringType()), StructField("severity", StringType()),
    StructField("error_time", StringType()) # Kafka sends as string
])

# --- Function to read Kafka stream ---
def read_kafka_stream(topic, schema):
    return spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load() \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

# --- Job 1: Orders Processing & Metrics ---
print("Starting Orders Processing Job...")
orders_raw_stream = read_kafka_stream(ORDERS_TOPIC, orders_schema)

orders_processed_stream = orders_raw_stream \
    .withColumn("order_ts", expr("CAST(order_time AS TIMESTAMP)")) \
    .withColumn("order_total", col("quantity") * col("price")) \
    .select("order_id", "user_id", "product_id", "quantity", "price", "order_total", "status", "order_ts")

# Write detailed data
query_orders_sink = orders_processed_stream \
    .writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("path", ORDERS_TABLE) \
    .option("checkpointLocation", CHECKPOINT_ORDERS) \
    .start()

# Calculate order/revenue metrics per minute (select columns renamed)
orders_metrics_stream = orders_processed_stream \
    .withWatermark("order_ts", "2 minutes") \
    .groupBy(window(col("order_ts"), "1 minute", "30 seconds").alias("time_window")) \
    .agg(
        count("order_id").alias("orders_count"),
        expr("sum(order_total)").alias("total_revenue")
    ) \
    .select(
        col("time_window.start").alias("window_start"), # Renamed
        col("time_window.end").alias("window_end"),     # Renamed
        "orders_count",
        "total_revenue"
    )

# !!! ADDED printSchema FOR DEBUG !!!
print("Schema before writing ORDERS_METRICS_TABLE:")
orders_metrics_stream.printSchema()

query_orders_metrics_sink = orders_metrics_stream \
    .writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("path", ORDERS_METRICS_TABLE) \
    .option("checkpointLocation", CHECKPOINT_ORDERS_METRICS) \
    .start()
print("Orders Job Started.")

# --- Job 2: Clicks Processing & Active Users ---
print("Starting Clicks Processing Job...")
clicks_raw_stream = read_kafka_stream(CLICKS_TOPIC, clicks_schema)

clicks_processed_stream = clicks_raw_stream \
    .withColumn("click_ts", expr("CAST(click_time AS TIMESTAMP)")) \
    .select("click_id", "user_id", "url", "device_type", "click_ts")

# Write detailed data
query_clicks_sink = clicks_processed_stream \
    .writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("path", CLICKS_TABLE) \
    .option("checkpointLocation", CHECKPOINT_CLICKS) \
    .start()

# Calculate active users in a 5-minute window (select columns renamed)
active_users_stream = clicks_processed_stream \
    .withWatermark("click_ts", "6 minutes") \
    .groupBy(window(col("click_ts"), "5 minutes", "1 minute").alias("time_window")) \
    .agg(approx_count_distinct("user_id").alias("active_users_approx")) \
    .select(
        col("time_window.start").alias("window_start"), # Renamed
        col("time_window.end").alias("window_end"),     # Renamed
        "active_users_approx"
    )

# !!! ADDED printSchema FOR DEBUG !!!
print("Schema before writing ACTIVE_USERS_TABLE:")
active_users_stream.printSchema()

query_active_users_sink = active_users_stream \
    .writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("path", ACTIVE_USERS_TABLE) \
    .option("checkpointLocation", CHECKPOINT_ACTIVE_USERS) \
    .start()
print("Clicks Job Started.")


# --- Job 3: Errors Processing & Metrics ---
print("Starting Errors Processing Job...")
errors_raw_stream = read_kafka_stream(ERRORS_TOPIC, errors_schema)

errors_processed_stream = errors_raw_stream \
    .withColumn("error_ts", expr("CAST(error_time AS TIMESTAMP)")) \
    .select("error_id", "service_name", "error_message", "severity", "error_ts")

# Write detailed data
query_errors_sink = errors_processed_stream \
    .writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("path", ERRORS_TABLE) \
    .option("checkpointLocation", CHECKPOINT_ERRORS) \
    .start()

# Calculate errors per minute (select columns renamed)
errors_metrics_stream = errors_processed_stream \
    .withWatermark("error_ts", "2 minutes") \
    .groupBy(
        window(col("error_ts"), "1 minute", "30 seconds").alias("time_window"),
        col("service_name"),
        col("severity")) \
    .agg(count("error_id").alias("error_count")) \
    .select(
        col("time_window.start").alias("window_start"), # Renamed
        col("time_window.end").alias("window_end"),     # Renamed
        "service_name",
        "severity",
        "error_count"
    )

# !!! ADDED printSchema FOR DEBUG !!!
print("Schema before writing ERRORS_METRICS_TABLE:")
errors_metrics_stream.printSchema()

query_errors_metrics_sink = errors_metrics_stream \
    .writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("path", ERRORS_METRICS_TABLE) \
    .option("checkpointLocation", CHECKPOINT_ERRORS_METRICS) \
    .start()
print("Errors Job Started.")


# --- Job 4: Simple Anomaly Detection ---
# Write simple alert on FATAL error
alerts_stream = errors_processed_stream \
      .filter(col("severity") == "FATAL") \
      .select(
          expr("uuid()").alias("alert_id"), # Create unique ID
          lit("FATAL_ERROR").alias("alert_type"),
          expr("concat('Fatal error in service: ', service_name, ' - ', error_message)").alias("description"),
          col("error_ts").alias("alert_timestamp"),
          lit(1.0).alias("metric_value") # Symbolic value
      )

# !!! ADDED printSchema FOR DEBUG !!!
print("Schema before writing ALERTS_TABLE:")
alerts_stream.printSchema()

query_alerts_sink = alerts_stream \
    .writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("path", ALERTS_TABLE) \
    .option("checkpointLocation", CHECKPOINT_ALERTS) \
    .start()
print("Alerts Job Started (Simple FATAL error detection).")


# --- Await stream termination ---
print("All streaming jobs started. Awaiting termination...")
spark.streams.awaitAnyTermination()