#!/usr/bin/env python
# -*- coding: utf-8 -*-

import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer, KafkaClient

access_token = "2975180000-vCwya091v2ZwWDtuz8tXznmfGCWnJZNsunOqfGj"
access_token_secret =  "XWy288cYdpKHK0vDRbIcZt3Xhiz1x46VYd8JL0WIqh440"
consumer_key =  "mNs6LjrrXy7AGndXPRSmXJITx"
consumer_secret =  "z6kIQhR0s2TIMTTV0PEqI2wgUJEJMPHCQcPkGAzEIz3kjhA7Qt"


class TweetListener (tweepy.Stream):
    def on_data(self, data):
        try:
            with open("tweets", data.encode('utf-8')) as f:
                print(data)
                fh = open("../Data/tweets_AI_1.json", "a")
                f.write(data)
                f.close()
                return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True
 
    def on_error(self, status):
        print(status)
        return True
 
twitter_stream = TweetListener(
  "Consumer Key here", "Consumer Secret here",
  "Access Token here", "Access Token Secret here"
)
twitter_stream.filter(track=['#python'])

kafka = KafkaClient("localhost:9092")
producer = KafkaProducer(kafka)
tweetListener = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, tweetListener)
stream.filter(track=["AI", "Artificial intelligence", "ArtificialIntelligence"])

