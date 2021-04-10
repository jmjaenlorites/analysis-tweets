#
# Structured Streaming Word Count Example
#    Original Source: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
#
# To run this example:
#   Terminal 1:  nc -lk 9999
#	Terminal 2:  ./bin/spark-submit structured_streaming_word_count.py
#   Note, type words into Terminal 1
#

import findspark
SPARK_HOME = r"C:\Users\jmjl\Desktop\Master\BigData\spark\spark-3.0.2-bin-hadoop2.7"
findspark.init(spark_home=SPARK_HOME)


# Import the necessary classes and create a local SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split


def quiet_logs( sc ):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
  logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )


spark = SparkSession \
	.builder \
	.appName("StructuredNetworkWordCount") \
	.getOrCreate()

quiet_logs(spark)

 # Create DataFrame representing the stream of input lines from connection to localhost:9999
lines = spark\
	.readStream\
	.format('socket')\
	.option('host', 'localhost')\
	.option('port', 9999)\
	.load()

# Split the lines into words
words = lines.select(
	explode(
    	split(lines.value, ' ')
	).alias('word')
)

# Generate running word count
wordCounts = words.groupBy('word').count().sort('count', ascending=False)


# Start running the query that prints the running counts to the console
query = wordCounts\
	.writeStream\
	.outputMode('update')\
	.format('console')\
	.start()

# Await Spark Streaming termination
query.awaitTermination()
