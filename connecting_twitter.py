import tweepy
from tweepy import Stream
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
import socket
import json
import argparse

consumer_key = "Hs8q9Y5RnQdW5twdX3Y24adoD"
consumer_secret = "W5kVjmMTyqafEPg8VIVKpkzHDJqmTiT58EHdNkKVYpWcI8vxYQ"
access_token = '1333741454217375746-mqcMEgWznro7BSL6s8Pl0JSJ07xP6l'
access_secret = 'BGMTw0L8Km9aU1Nt2PgbWT6agYBUdQei4dWecEEm2cInP'


class TweetsListener(StreamListener):
    # tweet object listens for the tweets
    def __init__(self, csocket):
        self.client_socket = csocket

    def on_data(self, data):
        try:
            msg = json.loads(data)
            # print(msg)
            # if tweet is longer than 140 characters
            if "extended_tweet" in msg:
                msg["text"] = msg['extended_tweet']['full_text']
                if msg['place'] is not None: print(msg['place'])
                # add at the end of each tweet "t_end"
                # self.client_socket.send(str(msg['extended_tweet']['full_text'] + "t_end").encode('utf-8'))
                self.client_socket.send(str(data).encode('utf-8'))
                # self.client_socket.send(json.dumps(msg).encode("utf-8"))
                # print(msg['extended_tweet']['full_text'])
            else:
                # add at the end of each tweet "t_end"
                # self.client_socket.send(str(json.dumps(msg)).encode("utf-8"))
                self.client_socket.send(str(data).encode('utf-8'))
                # self.client_socket.send(str(msg['text'] + "t_end").encode('utf-8'))
                if msg['place'] is not None: print(msg['place'])
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True


def send_data(c_socket, keyword):
    print('Start sending data from Twitter to socket')
    # authentication based on the credentials
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    # start sending data from the Streaming API
    twitter_stream = Stream(auth, TweetsListener(c_socket))
    twitter_stream.filter(track=keyword, languages=["en"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument("-k", '--keyword',  type=str, default="corona")
    args = parser.parse_args()
    # server (local machine) creates listening socket
    s = socket.socket()
    host = "127.0.0.1"
    port = 9999
    s.bind((host, port))
    print("Listening on port: %s" % str(port))
    # server (local machine) listens for connections
    s.listen(4)
    print('socket is listening')
    # return the socket and the address on the other side of the connection (client side)
    c_socket, addr = s.accept()
    print("Received request from: " + str(addr))
    # select here the keyword for the tweet data
    print(args.keyword)
    send_data(c_socket, keyword=[args.keyword])