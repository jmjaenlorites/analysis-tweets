from pymongo import MongoClient
import findspark
from analysis import *

SPARK_HOME = r"C:\Users\jmjl\Desktop\Master\BigData\spark\spark-3.0.2-bin-hadoop2.7"
findspark.init(spark_home=SPARK_HOME)

import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.types as typ


def start_mongo(spark, keyword):
    client = MongoClient('localhost', 49000)
    db = client["Test"]
    collection = db[keyword]
    allTweets = []
    for msg in collection.find():
        place = msg["place"]["country_code"] if msg["place"] is not None else "??"
        if "extended_tweet" in msg:
            text = msg['extended_tweet']['full_text'].replace("\n", " ") + " p_" + place
        else:
            text = msg["text"].replace("\n", " ") + " p_" + place
        allTweets.append((text, 1))
    schema = StructType([
        StructField('value', typ.StringType(), True),
        StructField('val', typ.IntegerType(), True)
        ])
    lines = spark.createDataFrame(data=allTweets, schema=schema).select("value")
    return lines
