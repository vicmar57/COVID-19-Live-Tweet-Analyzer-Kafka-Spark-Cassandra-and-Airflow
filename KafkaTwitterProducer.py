# -*- coding: utf-8 -*-
"""
Created on Wed Apr 15 15:51:51 2020

@author: vicma
"""

from __future__ import print_function
import json
from kafka import KafkaProducer, KafkaClient
import tweepy
import time


try:
    with open('twitter.properties') as file:
        twittCred = json.load(file)
    
    # Twitter Credentials Obtained from http://dev.twitter.com
    consumer_key = twittCred["consumerKey"]
    consumer_secret = twittCred["consumerSecret"]
    access_token = twittCred["accessToken"]
    access_token_secret = twittCred["accessTokenSecret"]

    # Words to track
    WORDS = ['#coronavirus', '#COVID-19', '#COVID19', '#COVID'] #, '#SocialDistancing', '#pandemic']
    raw_tweets_topic = "1_RAW_tweets"
    
    class StreamListener(tweepy.StreamListener):
        # This is a class provided by tweepy to access the Twitter Streaming API.
    
        def on_connect(self):
            # Called initially to connect to the Streaming API
            print("You are now connected to the streaming API.")
    
        def on_error(self, status_code):
            # On error - if an error occurs, display the error / status code
            print("Error received in kafka producer " + repr(status_code))
            return True # Don't kill the stream
    
        def on_data(self, data):
            """ This method is called whenever new data arrives from live stream.
            We asynchronously push this data to kafka queue"""
            try:
                parsed = json.loads(data)
                if "user" in parsed and "location" in parsed["user"]:
                    if parsed["user"]["location"] != None:
                        # time.sleep(10) #seconds
                        producer.send(raw_tweets_topic, data.encode('utf-8'))
                        print()
                        print(parsed["text"]) #["user"]["location"])
                        
            except Exception as e:
                print("error! : " + str(e))
                return False #stop stream
            
            return True # Don't kill the stream
    
        def on_timeout(self):
            return True # Don't kill the stream
    
    # Kafka Configuration
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    
    # Create Auth object
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)
    
    # Set up the listener. The 'wait_on_rate_limit=True' is needed to help with Twitter API rate limiting.
    listener = StreamListener(api=tweepy.API(wait_on_rate_limit=True, wait_on_rate_limit_notify=True, timeout=60, retry_delay=5, retry_count=10, retry_errors=set([401, 404, 500, 503])))
    stream = tweepy.Stream(auth=auth, listener=listener)
    print("Tracking: " + str(WORDS))
    stream.filter(track=WORDS, languages = ['en'])

except:
    import traceback
    import sys
    traceback.print_exc()
    sys.exit()