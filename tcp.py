import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json

consumer_key = "1"
consumer_secret = "a"
access_token = "pF"
access_token_secret = "7k"

class streamer(StreamListener):
  def __init__(self, sock):
      self.client_socket = sock
  def on_data(self, data):
      try:
          msg = json.loads( data )
          self.client_socket.send( msg['text'].encode('utf-8') )
          return True
      except BaseException as e:
      return True
  def on_error(self, status):
      return True


def sendData(c_socket):
      auth = OAuthHandler(consumer_key, consumer_secret)
      auth.set_access_token(access_token, access_token_secret)
      twitter_stream = Stream(auth, streamer(c_socket))
      twitter_stream.filter(track=['wearing masks'])

s = socket.socket()
host = 'localhost'
port = 2000
s.bind((host, port))
s.listen(5)
c, addr = s.accept()
sendData(c)
