Basic Spark Example

from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("Basic Spark Example") \
    .getOrCreate()

# Create a DataFrame
data = [("John", 28), ("Anna", 23), ("Peter", 35)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Show the DataFrame
df.show()

# Print the schema
df.printSchema()

# Select and show specific columns
df.select("Name").show()

# Filter and show rows
df.filter(df.Age > 25).show()

# Stop the Spark session
spark.stop()
