from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

# Create Spark session
spark = (SparkSession
    .builder
    .appName("StructuredStreaming")
    .getOrCreate())

# Configure the socket source
host, port = "localhost", 5555
lines = (spark
    .readStream
    .format("socket")
    .option("host", host)
    .option("port", port)
    .load())

# Split lines into words
words = lines.select(
    explode(
        split(lines.value, " ")
    ).alias("word")
)

# Group by word and count occurrences
wordCounts = words.groupBy("word").count()

# Write the word counts to the console
query = (wordCounts
    .writeStream
    .outputMode("complete")  # Use "complete" mode for word counts
    .format("console")
    .option("truncate", False)
    .option("numRows", 1000)
    .start())

# Await termination of the query
spark.streams.awaitAnyTermination()

# Close Spark session
spark.close()