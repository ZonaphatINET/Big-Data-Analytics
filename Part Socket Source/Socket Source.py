from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

# Create Spark Session
spark = SparkSession.builder \
    .appName("SocketSourceExample") \
    .getOrCreate()

# Read data from the socket
lines = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Split the data into words and count them
words = lines.select(
    explode(
        split(lines.value, " ")
    ).alias("word")
)

wordCounts = words.groupBy("word").count()  # Group and count each word

# Write the results to the console
query = wordCounts.writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

# Await termination of the query
query.awaitTermination()