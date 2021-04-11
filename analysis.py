import findspark

SPARK_HOME = r"C:\Users\jmjl\Desktop\Master\BigData\spark\spark-3.0.2-bin-hadoop2.7"
findspark.init(spark_home=SPARK_HOME)

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from textblob import TextBlob
import json
import pandas as pd
import subprocess

country_codes = pd.read_csv("./countries_codes_and_coordinates.txt")[["Alpha-2code", "Latitude", "Longitude"]]


def preprocessing(lines):
    tweets = lines.withColumnRenamed("value", "text")
    get_country_udf = udf(get_country, StringType())
    tweets = tweets.withColumn("country", get_country_udf("text"))
    tweets = tweets.withColumn('text', F.regexp_replace('text', r' p_[A-Z]{2}$|#|:|RT|@\w+|http\S+', ''))
    return tweets


def get_country(value: str) -> str:
    return value[-2:]


def sentiment(text: str):
    s = TextBlob(text).sentiment
    return (s.polarity, s.subjectivity)


def text_classification(tweets):
    schema = StructType([
        StructField("polarity", FloatType(), False),
        StructField("subjectivity", FloatType(), False)
    ])
    sentiment_udf = udf(sentiment, schema)
    tweets = tweets.withColumn("sentiment", sentiment_udf("text"))

    return tweets


def get_tweets(spark):
    lines = spark.readStream.format("socket") \
        .option("host", "127.0.0.1").option("port", 9999).load()
    tweets = preprocessing(lines)
    tweets = text_classification(tweets)
    tweets.createOrReplaceTempView("tweets")
    sql_query = """
    SELECT  country, 
            count(country) AS tweets_by_country,
            avg(sentiment.polarity) AS avg_polarity, 
            stddev_pop(sentiment.polarity) AS std_polarity,
            avg(sentiment.subjectivity) AS avg_subjectivity, 
            stddev_pop(sentiment.subjectivity) AS std_subjectivity
    FROM tweets
    GROUP BY country
    """
    # tweets = tweets.repartition(1)
    tweets = spark.sql(sql_query)
    return tweets


if __name__ == "__main__":
    keyword = "holland"
    # subprocess.Popen(["python3", "connecting_twitter.py", "-k", keyword])
    print("Starting spark session")
    spark = SparkSession.builder.appName("TwitterSentimentAnalysis").getOrCreate()

    tweets = get_tweets(spark)

    # tweets = spark.readStream.format("socket") \
    #     .option("host", "127.0.0.1").option("port", 9999).load()#.option("sep", "t_end").load()\
    #     # .withColumn("value", explode(split("value", "t_end")))

    # query2 = tweets.writeStream.queryName("all_tweets2")\
    #     .outputMode("update").format("console")\
    #     .trigger(processingTime='20 seconds').start()

    query2 = tweets.writeStream.queryName("all_tweets2") \
        .outputMode("complete").format("console") \
        .trigger(processingTime='5 seconds')\
        .start()
    # print("Starting WriteStream")
    # query = tweets.writeStream.outputMode("complete").format("memory").queryName("alltweets")\
    #     .trigger(processingTime='60 seconds').start()
    print("WriteStream started")
    query2.awaitTermination()
