import tweepy
from tweepy import Stream
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
import socket
import json
import argparse

consumer_key = 
consumer_secret = 
access_token = 
access_secret = 


class TweetsListener(StreamListener):
    # tweet object listens for the tweets
    def __init__(self, csocket):
        self.client_socket = csocket

    def on_data(self, data):
        try:
            msg = json.loads(data)
            place = msg["place"]["country_code"] if msg["place"] is not None else "??"
            if "extended_tweet" in msg:
                text = msg['extended_tweet']['full_text'].replace("\n", " ")+" p_"+place+"\n"
            else:
                text = msg["text"].replace("\n", " ")+" p_"+place+"\n"
            # print(text)
            self.client_socket.send(text.encode("utf-8"))
            # if msg['place'] is not None: print(msg['place'])
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
    try:
        twitter_stream.filter(track=keyword, languages=["en"])
    except:
        print("\n\nSome Error")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument("-k", '--keyword',  type=str, default="WrestleMania")
    args = parser.parse_args()
    # server (local machine) creates listening socket
    s = socket.socket()
    host = "127.0.0.1"
    port = 9999
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
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
