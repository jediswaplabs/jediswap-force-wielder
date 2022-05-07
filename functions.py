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



# TWEETS contains all tweets containing {keyword} from last 7 days
TWEETS = []
USERS = []
keyword = 'jediswap'
max_tweets = 2000



def set_TWEETS(keyw=keyword, max_amount=max_tweets):
    '''
    Global variable to store tweets json object locally for
    memoization to save on querying the Twitter API wherever possible.
    Stores up to {max_amount} of tweets containing a keyword that have
    been tweeted within the last 7 days (constraint of free version of API).
    '''
    global TWEETS
    TWEETS = get_tweets(keyw, max_amount)


def tweet_to_dict(t):
    '''
    Converts a Twitter API tweet object to a python dictionary
    with the tweet id as key.
    '''
    d = {}

    # create dictionary of tweet metadata
    d['id'] = t.id_str
    d['author'] = t.author
    d['ts'] = t.created_at
    d['entities'] = t.entities
    d['geo'] = t.geo
    d['id'] = t.id_str
    d['is_quote_status'] = t.is_quote_status
    d['lang'] = t.lang
    d['retweet'] = t.retweet
    d['retw_status'] = t.retweeted
    d['retweets'] = t.retweets
    d['retw_count'] = t.retweet_count
    d['retweeted_bool'] = t.retweeted
    d['source'] = t.source
    d['text'] = t.text
    d['truncated'] = t.truncated
    d['user'] = t.user

    return d



def get_tweets(keyw, max_amount):
    '''
    Returns all tweets from within last week containing a keyword
    '''
    search_results = tweepy.Cursor(api.search, q=keyw).items(max_amount)
    out_d = {}

    # create nested dictionary of tweet metadata
    for t in search_results:
        d = tweet_to_dict(t)
        out_d[d['id']] = d

    return out_d



def query_API_for_tweet_id(_id):
    tweet_obj = api.get_status(_id)
    return tweet_to_dict(tweet_obj)


def get_tweet_metadata(tweet_id):
    '''
    Wrapper to save on API queries per minute:
    Checks local json object containing tweets first for a tweet id.
    If it's not in there, queries twitter API. Returns available
    tweet metadata.
    '''
    global TWEETS

    # try downloaded tweets first
    if TWEETS != [] and tweet_id in TWEETS:
        return TWEETS[tweet_id]

    # if not in there, query Twitter API
    else:
        return query_API_for_tweet_id(tweet_id)


def get_retweet_count(tweet_id):
    '''
    Takes tweet id, returns number of retweets.
    '''
    d = get_tweet_metadata(tweet_id)
    return d['retw_count']


def get_follower_count(user):
    '''
    Takes twitter user handle, returns number of followers.
    '''

    n_followers = 0
    return n_followers


# query Twitter API and populate memo variable (TWEETS)
set_TWEETS()
