import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json

consumer_key = "1B0I2AqHTou2TdU5owzrfTxXm"
consumer_secret = "cjVGrn86WRMfpOB0YTJEF5Xi0r8DNi5NOmvNCl56GhdjWnjAFa"
access_token = "1237765910452191232-kYRsxaJM5mvSH6L1ZD0ZIvxKPJbdpF"
access_token_secret = "728oP1Z3JbqDS6JAJozzU9H1WtmM3cy17zx2Q4kYxknmk"

class TweetsListener(StreamListener):
  def __init__(self, csocket):
      self.client_socket = csocket
  def on_data(self, data):
      try:
          msg = json.loads( data )
          self.client_socket.send( msg['text'].encode('utf-8') )
          return True
      except BaseException as e:
          print("Error on_data: %s" % str(e))
      return True
  def on_error(self, status):
      print(status)
      return True


def sendData(c_socket):
      auth = OAuthHandler(consumer_key, consumer_secret)
      auth.set_access_token(access_token, access_token_secret)
      twitter_stream = Stream(auth, TweetsListener(c_socket))
      twitter_stream.filter(track=['wearing masks'])

s = socket.socket()
host = 'localhost'
port = 2000
s.bind((host, port))
s.listen(5)
c, addr = s.accept()
sendData(c)
