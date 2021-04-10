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
import time

country_codes = pd.read_csv("./countries_codes_and_coordinates.txt")[["Alpha-2code", "Latitude", "Longitude"]]


def preprocessing(lines):
    words = lines.select(json_tuple(lines.value, "text", "place"))\
        .selectExpr("c0 as text", "c1 as country")
    get_country_udf = udf(get_country, StringType())
    words = words.withColumn("country", get_country_udf("country"))
    words = words.withColumn("word", explode(split(words.text, "t_end")))
    # words = words.na.replace('', None)
    # words = words.na.drop()
    words = words.withColumn('word', F.regexp_replace('word', r'http\S+', ''))
    words = words.withColumn('word', F.regexp_replace('word', '@\w+', ''))
    words = words.withColumn('word', F.regexp_replace('word', '#', ''))
    words = words.withColumn('word', F.regexp_replace('word', 'RT', ''))
    words = words.withColumn('word', F.regexp_replace('word', ':', ''))
    return words


def polarity_detection(text: str) -> float:
    return TextBlob(text).sentiment.polarity


def get_country(value: str) -> str:
    if value is not None:
        value = json.loads(value)
        value = value["country_code"]
    else:
        value = "??"
    return value


def subjectivity_detection(text: str) -> float:
    return TextBlob(text).sentiment.subjectivity


def text_classification(words):
    # polarity detection
    polarity_detection_udf = udf(polarity_detection, StringType())
    words = words.withColumn("polarity", polarity_detection_udf("word"))
    # subjectivity detection
    subjectivity_detection_udf = udf(subjectivity_detection, StringType())
    words = words.withColumn("subjectivity", subjectivity_detection_udf("word"))
    return words


def get_words(spark):
    lines = spark.readStream.format("socket")\
        .option("host", "127.0.0.1").option("port", 9999).load()
    words = preprocessing(lines)
    words = text_classification(words)
    words.createOrReplaceTempView("words")
    sql_query = """
    SELECT  country, 
            count(country) AS tweets_by_country,
            avg(polarity) AS avg_polarity, 
            stddev_pop(polarity) AS std_polarity,
            avg(subjectivity) AS avg_subjectivity, 
            stddev_pop(subjectivity) AS std_subjectivity
    FROM words
    GROUP BY country
    """
    # words = words.repartition(1)
    words = spark.sql(sql_query)
    return words


if __name__ == "__main__":
    print("Starting spark session")
    spark = SparkSession.builder.appName("TwitterSentimentAnalysis").getOrCreate()
    words = get_words(spark)

    # query2 = words.writeStream.queryName("all_tweets2")\
    #     .outputMode("update").format("console")\
    #     .trigger(processingTime='20 seconds').start()

    query2 = words.writeStream.queryName("all_tweets2") \
        .outputMode("update").format("console") \
        .trigger(processingTime='60 seconds').start()
    # print("Starting WriteStream")
    # query = words.writeStream.outputMode("complete").format("memory").queryName("alltweets")\
    #     .trigger(processingTime='60 seconds').start()
    print("WriteStream started")
    query2.awaitTermination()
