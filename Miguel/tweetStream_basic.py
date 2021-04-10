
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import tweepy
import json
import sys

# consumer_key = '8c7VlZNhvdFAPNdd0LC244246'
# consumer_secret = 'vtPdScrP3Sdj28yCvJIETFREEsdjIqn3xVk56s2SIU2zpLmFCM'
# access_token = '900106233272819712-xa6nJKoNvMuF5UWkCO8dVmcL1ZrlkXh'
# access_secret = 'noD7jSMfs05CWR8KfNcDPC7STrT5eJOANy6YmE9IWGlQK'

api_key = "Hs8q9Y5RnQdW5twdX3Y24adoD"
api_key_secret = "W5kVjmMTyqafEPg8VIVKpkzHDJqmTiT58EHdNkKVYpWcI8vxYQ"
consumer_key = api_key
consumer_secret = api_key_secret
access_token = '1333741454217375746-mqcMEgWznro7BSL6s8Pl0JSJ07xP6l'
access_secret = 'BGMTw0L8Km9aU1Nt2PgbWT6agYBUdQei4dWecEEm2cInP'


class StdOutListener(StreamListener):
    """ A listener handles tweets that are received from the stream.
    This is a basic listener that just prints received tweets to stdout.
    """
    def on_data(self, data):
      try:
          msg = json.loads( data )
          print( msg['text'] )
          return True
      except BaseException as e:
          print("Error on_data: %s" % str(e))
      return True

    def on_error(self, status):
        print(status)

if __name__ == '__main__':
    
    l = StdOutListener()
    
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    api = tweepy.API(auth)
    print(api.me().name)

    stream = Stream(auth, l)
    print("Filter term: "+sys.argv[1])
    stream.filter(track=[sys.argv[1]])
