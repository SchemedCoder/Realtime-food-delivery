from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("FoodDeliveryPipeline") \
    .getOrCreate()

# -----------------------------
# Read Kafka Stream
# -----------------------------
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "food_orders") \
    .load()

# -----------------------------
# Convert binary → string
# -----------------------------
json_df = df.selectExpr("CAST(value AS STRING)")

# -----------------------------
# Schema
# -----------------------------
schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("restaurant", StringType(), True),
    StructField("delivery_time", IntegerType(), True),
    StructField("order_amount", IntegerType(), True)
])

# -----------------------------
# Parse JSON
# -----------------------------
parsed_df = json_df \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# -----------------------------
# Handle bad records
# -----------------------------
parsed_df = parsed_df.dropna()

# -----------------------------
# SLA Logic
# -----------------------------
parsed_df = parsed_df.withColumn(
    "is_delayed",
    when(col("delivery_time") > 30, 1).otherwise(0)
)

# -----------------------------
# Aggregation
# -----------------------------
agg_df = parsed_df.groupBy("city").agg(
    count("*").alias("total_orders"),
    avg("delivery_time").alias("avg_delivery_time"),
    sum("is_delayed").alias("delayed_orders"),
    avg("order_amount").alias("avg_order_value")
)

# -----------------------------
# Output
# -----------------------------
query = agg_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
