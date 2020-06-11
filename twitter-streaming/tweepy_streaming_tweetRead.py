
# coding: utf-8

# In[ ]:

import tweepy
from tweepy import OAuthHandler,Stream


# In[ ]:

from tweepy.streaming import StreamListener
import socket
import json


# In[ ]:

consumer_key = '5jAtQyc9cVlvUCXnic4Obe9H1'
consumer_secret = 'KA16WifhCrcsjgZRwMAfEG7Ifmu003C3yDFxXwgom9Zbun3vaw'
access_token = '1208990024642912256-1Vy7wWC96SGgTuwj3f88e3u0gX5O7b'
access_secret = 'lc1yCpveKd3X6150XpXkYxF38w5fr1vGOoJqRUsDgqF0X'


# In[ ]:

class TweetListener(StreamListener):
    def __init__(self,csocket):
        self.client_socket = csocket
    
    def on_data(self,data):
        
        try:
            msg = json.loads(data)
            print(msg['text'].encode('utf-8'))
            self.client_socket.send(msg['text'].encode('utf-8'))
            return True
        except BaseException as e:
            print("ERROR ", e)
        return True
    
    def on_error(self,status):
        print(status)
        return True


# In[ ]:

def sendData(c_socket):
    auth = OAuthHandler(consumer_key,consumer_secret)
    auth.set_access_token(access_token,access_secret)
    
    twitter_stream = Stream(auth,TweetListener(c_socket))
    twitter_stream.filter(track=['cat'])


# In[ ]:

if __name__ == '__main__':
    s = socket.socket()
    host = '127.0.0.1'
    port = 9993
    s.bind((host,port))
    
    print('listening on port 9993')
    
    s.listen(5)
    c,addr = s.accept()
    
    sendData(c)


# In[ ]:



