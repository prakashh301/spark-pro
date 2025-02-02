"""
PySpark Data Skew Fixes - Hands-on Examples

This script demonstrates five techniques to handle data skew in PySpark,
including salting, broadcast joins, repartitioning, skew hints, and window functions.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, concat_ws, broadcast, count
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder.appName("Data Skew Fixes").getOrCreate()

# Sample Data Creation
large_data = [("North", i) for i in range(1000000)] + [("South", i) for i in range(1000)]
small_data = [("North", "High"), ("South", "Low")]
large_df = spark.createDataFrame(large_data, ["region", "value"])
small_df = spark.createDataFrame(small_data, ["region", "category"])

# 1. Salting for Skewed Joins
large_df = large_df.withColumn("salt", (monotonically_increasing_id() % 3))
large_df = large_df.withColumn("region_salted", concat_ws("_", large_df.region, large_df.salt))
small_df = small_df.crossJoin(spark.range(3).withColumnRenamed("id", "salt"))
small_df = small_df.withColumn("region_salted", concat_ws("_", small_df.region, small_df.salt))
joined_salted_df = large_df.join(small_df, "region_salted", "inner")

# 2. Broadcast Joins for Small Tables
joined_broadcast_df = large_df.join(broadcast(small_df), "region", "inner")

# 3. Repartition to Distribute Data Evenly
repartitioned_df = large_df.repartition(10, "region")

# 4. Optimize Skewed Joins with skewHint() (Spark 3+)
skew_hint_df = large_df.hint("skew", "region").join(small_df, "region", "inner")

# 5. Avoid groupBy on Skewed Columns (Use Window Functions)
window_spec = Window.partitionBy("region")
skew_fixed_df = large_df.withColumn("transaction_count", count("value").over(window_spec))

# Show Results
print("Salting Join Result:")
joined_salted_df.show(5)
print("Broadcast Join Result:")
joined_broadcast_df.show(5)
print("Repartitioning Result:")
repartitioned_df.show(5)
print("Skew Hint Result:")
skew_hint_df.show(5)
print("Window Function Result:")
skew_fixed_df.show(5)

# Stop Spark Session
spark.stop()
