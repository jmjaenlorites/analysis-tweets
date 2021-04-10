import pyspark
from pyspark.sql import SparkSession
from pymongo import MongoClient


sc = pyspark.SparkContext(master="local[6]", appName="Ejemplos")
spark = SparkSession.builder.master("local").getOrCreate()

client = MongoClient('localhost', 49000)
db = client['Isla']
col = 'isla tentaciones'
collection = db[col]

# Extract tweets from MongoDB
allTweets = []
for doc in collection.find():
    allTweets.append(doc)

print("DB: ALL_TWEETS:", len(allTweets))
# Load tweets into Spark for analysis
allTweetsRDD = sc.parallelize(allTweets)

print(allTweetsRDD.take(1))
print("SPARK-count:", allTweetsRDD.count())
print("--------------------------------")

# Filter tweets to get those who have hashtags
tweetsWithTagsRDD = allTweetsRDD.filter(lambda t: len(t['entities']['hashtags']) > 0)
print("HASHTAG-TWEETS:", tweetsWithTagsRDD.count())

# Count the number of occurrences for each hashtag, 
# by first extracting the hashtag and lowercasing it, 
# then do a standard word count with map and reduceByKey
countsRDD = (tweetsWithTagsRDD
            .flatMap(lambda tweet: [hashtag['text'].lower() for hashtag in tweet['entities']['hashtags']])
            .map(lambda tag: (tag, 1))
            .reduceByKey(lambda a, b: a + b)
            )
print("countsRDD has been created")
# Get the most used hashtags (order countsRDD descending by count)
res = countsRDD.takeOrdered(10, lambda ht: -ht[1])
print("TWEETS_WITH_TAGS2:", countsRDD.count())
print("Finished---------------------------")


