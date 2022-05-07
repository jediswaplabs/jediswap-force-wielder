import os, json, tweepy
from dotenv import load_dotenv

# Instantiate Twitter API
load_dotenv('./.env')
c_k, c_s, a_t, a_s = (
    os.environ['TW_CONSUMER_KEY'],
    os.environ['TW_CONSUMER_SECRET'],
    os.environ['TW_CONSUMER_KEY'],
    os.environ['TW_CONSUMER_KEY']
    )
auth = tweepy.OAuthHandler(c_k, c_s)
auth.set_access_token(a_t, a_s)
api = tweepy.API(auth)
