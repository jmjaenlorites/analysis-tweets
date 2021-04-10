# Revisa las bases de datos o colecciones existentes

from __future__ import absolute_import, print_function

from tweepy.streaming import StreamListener 
from tweepy import OAuthHandler
from tweepy import Stream
import tweepy
import json
from pymongo import MongoClient

consumer_key = '8c7VlZNhvdFAPNdd0LC244246'
consumer_secret = 'vtPdScrP3Sdj28yCvJIETFREEsdjIqn3xVk56s2SIU2zpLmFCM'
access_token = '900106233272819712-xa6nJKoNvMuF5UWkCO8dVmcL1ZrlkXh'
access_secret = 'noD7jSMfs05CWR8KfNcDPC7STrT5eJOANy6YmE9IWGlQK'

# En terminal:
#   mongod --dbpath="C:\Users\jmjl\Desktop\Master\BigData\Bloque 2 - Spark con Python; pySpark\Analisis_tweets\data\db" --port 49000 --bind_ip 127.0.0.1

client = MongoClient('localhost', 49000)

for db in client.list_databases():
    print(db)
    #client.drop_database(db['name'])
    
"""
db = client['Tweets']
collection = db['Trump']
              
# Extract tweets from MongoDB
allTweets = []
for doc in collection.find():
    allTweets.append(doc)
    print(doc['text'])
"""
