from pyspark.sql import SparkSession
from pyspark.sql.functions import window, explode, split, current_timestamp

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

# Create a DataFrame representing the stream of input lines from a socket connection
lines = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Add a timestamp column to the DataFrame
lines = lines.withColumn("timestamp", current_timestamp())

# Split the lines into words
words = lines.select(
    explode(
        split(lines.value, " ")
    ).alias("word"),
    lines.timestamp
)

# Group the data by window and word, then compute the count of each group
windowCounts = words.groupBy(
    window(words.timestamp, "10 seconds", "5 seconds"),
    words.word
).count()

# Start running the query that prints the running counts to the console
query = windowCounts.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
