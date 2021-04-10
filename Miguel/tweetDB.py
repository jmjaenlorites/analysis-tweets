from __future__ import absolute_import, print_function

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import tweepy
import json
from pymongo import MongoClient
import sys

consumer_key = '8c7VlZNhvdFAPNdd0LC244246'
consumer_secret = 'vtPdScrP3Sdj28yCvJIETFREEsdjIqn3xVk56s2SIU2zpLmFCM'
access_token = '900106233272819712-xa6nJKoNvMuF5UWkCO8dVmcL1ZrlkXh'
access_secret = 'noD7jSMfs05CWR8KfNcDPC7STrT5eJOANy6YmE9IWGlQK'
kword = ''

class StdOutListener(StreamListener):
    """ A listener handles tweets that are received from the stream.
    This listener inserts the tweets in a mongoDB collection of tweets.
    """
    def on_data(self, data):
        
          try:
    		  
              msg = json.loads( data )
              
              print(msg['text'])
              
              client = MongoClient('localhost', 49000)
              db = client['Isla']
              collection = db[kword]
              collection.insert_one(msg)
              
              return True
          except BaseException as e:
              print("Error on_data: %s" % str(e))
      
    def on_error(self, status):
        print(status)

if __name__ == '__main__':
    
    # kword = sys.argv[1]
    kword = "isla tentaciones"
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    api = tweepy.API(auth)
    print(api.me().name)

    
    stream = Stream(auth, l)
    # print("Filter term: "+kword)
    stream.filter(track=[kword])
