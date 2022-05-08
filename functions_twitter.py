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
TWEETS = {}
USERS = {}
TWEETS_json_path = './TWEETS_jediswap.json'
USERS_json_path = './USERS_jediswap.json'

keyword = 'jediswap'
max_tweets = 2000



###  batch API querying / file processing

def read_from_json(json_path):
    '''
    Reads json, returns dict with contents of json file
    '''
    with open(json_path, 'r') as jfile:
        data = json.load(jfile)
        return data

def write_to_json(_dict, path):
    '''
    Writes dict of shape {wallet1: {prop_id_1, prop_id_2,...}, wallet2: ...}
    to json file located in path variable.
    '''
    with open(path, 'w') as jfile:
        json_object = json.dump(_dict, jfile, indent=1)

def get_tweets(keyw, max_amount):
    '''
    Returns all tweets from within last week containing a keyword
    '''
    search_results = [status._json for status in tweepy.Cursor(api.search, q=keyw).items(max_amount)]

    # Temporarily convert to json to enable json.dump() to work later on
    json_strings = [json.dumps(json_obj) for json_obj in search_results]
    json_compatible = [json.loads(x)for x in json_strings]

    out_d = {}

    # create nested dictionary of tweet metadata
    for t in json_compatible:
        d = tweet_to_dict(t)
        out_d[d['id']] = d

    return out_d

def get_tweets_raw(keyw, max_amount):
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

def load_TWEETS_from_json(TWEETS_json_path):
    '''
    Populates global variable TWEETS when the script is starting.
    '''
    global TWEETS
    TWEETS = read_from_json(TWEETS_json_path)

def save_TWEETS_to_json(TWEETS_json_path):
    global TWEETS
    write_to_json(TWEETS, TWEETS_json_path)

def load_USERS_from_json(USERS_json_path):
    '''
    Populates global variable TWEETS when the script is starting.
    '''
    global USERS
    USERS = read_from_json(USERS_json_path)

def save_USERS_to_json(USERS_json_path):
    global USERS
    write_to_json(USERS, USERS_json_path)

def populate_USERS_from_TWEETS():
    '''
    Takes TWEETS, gets all users from there,
    queries Twitter API to fetch their data
    and stores it in the USERS dictionary.
    '''
    global TWEETS
    global USERS

    users = set()
    for tweet in TWEETS:
        users.add(get_user_id(tweet))
    for u in users:
        d = get_user(u)
        USERS[u] = d


def tweet_to_dict(t, fill_with_nan=False):
    '''
    Converts a Twitter API tweet object to a python dictionary
    with the tweet id as key. This version returns jsonable entries.
    '''
    d = {}

    # option to return nan values for error handling:
    if fill_with_nan:
        keys = [
            'id', 'author', 'ts', 'entities', 'geo', 'is_quote_status',
            'lang', 'retweet', 'was_retweeted', 'retweets', 'retw_count',
            'retweeted_bool', 'source', 'text', 'truncated', 'user'
            ]
        for k in keys:
            d[k] = np.nan
            d['text'] = '======= User suspended: No data avaiable! ======='
        return d


    # create dictionary of tweet metadata
    d['id'] = t['id_str']
    d['ts'] = t['created_at']
    d['entities'] = t['entities']
    d['geo'] = t['geo']
    d['id'] = str(t['id'])
    d['is_quote_status'] = t['is_quote_status']
    d['lang'] = t['lang']
    d['was_retweeted'] = t['retweeted']
    d['source'] = t['source']
    d['text'] = t['text']
    d['truncated'] = t['truncated']
    d['user'] = t['user']
    d['retweet_count'] = t['retweet_count']
    d['in_reply_to_status_id'] = t['in_reply_to_status_id_str']
    d['in_reply_to_user_id'] = t['in_reply_to_status_id_str']
    d['in_reply_to_screen_name'] = t['in_reply_to_screen_name']
#    d['orig_retw_tweet_data'] = t['retweeted_status']
    d['favorite_count'] = t['favorite_count']
    d['retweeted_bool'] = t['retweeted']
    d['favorited_bool'] = t['favorited']


    return d

def tweet_to_dict_raw(t, fill_with_nan=False):
    '''
    Converts a Twitter API tweet object to a python dictionary
    with the tweet id as key. This version returns the original
    callable objects from the API. Can't be serialized.
    '''
    d = {}

    # option to return nan values for error handling:
    if fill_with_nan:
        keys = [
            'id', 'author', 'ts', 'entities', 'geo', 'is_quote_status',
            'lang', 'retweet', 'was_retweeted', 'retweets', 'retw_count',
            'retweeted_bool', 'source', 'text', 'truncated', 'user'
            ]
        for k in keys:
            d[k] = np.nan
            d['text'] = '======= User suspended: No data avaiable! ======='
        return d

    # create dictionary of tweet metadata
    d['id'] = t.id_str
    d['ts'] = t.created_at
    d['entities'] = t.entities
    d['geo'] = t.geo
    d['id'] = t.id_str
    d['is_quote_status'] = t.is_quote_status
    d['lang'] = t.lang
    d['was_retweeted'] = t.retweeted
    d['source'] = t.source
    d['text'] = t.text
    d['truncated'] = t.truncated
    d['user'] = t.user
    d['retweet_count'] = t.retweet_count
    d['retweets'] = t.retweets
    d['retweeted_bool'] = t.retweeted
    d['retweet'] = t.retweet
    return d

def user_to_dict(user_status, fill_with_nan=False):
    '''
    Converts a Twitter API tweet object to a python dictionary
    with the tweet id as key. This version returns jsonable entries.
    '''

    d = {}
    t = user_status

    # option to return nan values for error handling:
    if fill_with_nan:
        keys = [
            'id', 'created_at', 'bio', 'handle', 'entities',
            'followers_count', 'friends_count', 'favourites_count',
            'tweets_count', 'most_recent_tweet', 'lang'
            ]

        for k in keys:
            d[k] = np.nan
            d['bio'] = '======= User not found or suspended. No data avaiable! ======='
        return d


    # create dictionary of user metadata
    d['id'] = t['id_str']
    d['created_at'] = t['created_at']
    d['bio'] = t['description']
    d['handle'] = t['screen_name']
    d['entities'] = t['entities']
    d['followers_count'] = t['followers_count']
    d['friends_count'] = t['friends_count']
    d['favourites_count'] = t['favourites_count']
    d['tweets_count'] = t['statuses_count']
    d['most_recent_tweet'] = t['status']
    d['lang'] = t['lang']

    return d

def get_TWEETS():
    global TWEETS
    return TWEETS

def get_USERS():
    global USERS
    return USERS




###  getter functions

def query_API_for_tweet_obj(_id):
    global TWEETS
    TweepError = tweepy.error.TweepError

    try:
        tweet_obj = api.get_status(_id)
        jsonized = tweet_obj._json
        result = tweet_to_dict(jsonized)
        TWEETS[_id] = result
        return result

    # Catch error of suspended twitter users
    except TweepError as e:
        print(f'\nUser suspended! (Tweet {_id})\n')
        print('\nError description:\n', e)
        return tweet_to_dict(None, fill_with_nan=True)

def query_API_for_user_obj(user_id):
    global USERS
    TweepError = tweepy.error.TweepError

    try:
        user_obj = api.get_user(user_id)
        jsonized = user_obj._json
        result = user_to_dict(jsonized)
        USERS[user_id] = result
        return result

    # Catch error of suspended twitter users
    except TweepError as e:
        print(f'\nID wrong or user suspended! (ID {_id})')
        print('\nError description:\n', e)
        return user_to_dict(None, fill_with_nan=True)

def get_tweet(tweet_id):
    '''
    Wrapper to save on API queries per minute:
    Checks local json object containing tweets first for a tweet id.
    If it's not in there, queries twitter API. Returns available
    tweet metadata.
    '''
    global TWEETS

    # try downloaded tweets first
    if TWEETS != {} and tweet_id in TWEETS:
        return TWEETS[tweet_id]

    # if not in there, query Twitter API
    else:
        print(f'Had to query API for Tweet ID {tweet_id}')
        return query_API_for_tweet_obj(tweet_id)

def get_user(user_id):
    '''
    Wrapper to save on API queries per minute:
    Checks local json object containing users first for a user id.
    If it's not in there, queries twitter API. Returns available
    tweet metadata.
    '''
    global USERS

    # try downloaded tweets first
    if USERS != {} and user_id in USERS:
        return USERS[user_id]

    # if not in there, query Twitter API
    else:
        print(f'Had to query API for User ID {user_id}')
        return query_API_for_user_obj(user_id)




### tweet-related

def get_text(tweet_id):
    return get_tweet(tweet_id)['text']

def get_retweet_count(_id):
    if not get_tweet(_id)['was_retweeted']:
        return 0
    else:
        print('\nFunction not done yet!')
        pass

def get_n_quotes(_id):
    print('\nFunction not done yet!')
    pass

def get_n_likes(tweet_id):
    return get_tweet(tweet_id)['favorite_count']

def has_been_retweeted(_id):
    return get_tweet(_id)['retweeted_bool']

def has_been_liked(_id):
    return get_tweet(_id)['favorited_bool']

def get_mentions(_id):
    mentions = get_tweet(_id)['entities']['user_mentions']
    names = [x['screen_name'] for x in mentions]
    return names

def get_n_past_tweets(user_id=None, tweet_id=None):
    if user_id:
        pass
    if tweet_id:
        return get_user_dict(tweet_id)['statuses_count']

def get_source_tweet_id(tweet_id):
    return get_source_tweet_from_retweet(tweet_id)['id_str']

def get_source_tweet_user_id(tweet_id):
    return get_source_tweet_from_retweet(tweet_id)['user']['id_str']

def get_source_tweet_handle(tweet_id):
    return get_source_tweet_from_retweet(tweet_id)['user']['screen_name']

def get_source_tweet_n_likes(tweet_id):
    return get_source_tweet_from_retweet(tweet_id)['favorite_count']


### user-related

def get_friends_count(user_id):
    return get_user(user_id)['friends_count']

def get_favorites_count(user_id):
    return get_user(user_id)['favourites_count']

def get_n_tweets(user_id):
    return get_user(user_id)['tweets_count']

def get_last_tweet(user_id):
    return get_user(user_id)['most_recent_tweet']


def get_followers_count(user_id=None, tweet_id=None):
    if user_id:
        return get_user(user_id)['followers_count']
    if tweet_id:
        return get_user_dict(tweet_id)['followers_count']

def get_user_dict(tweet_id):
    return get_tweet(tweet_id)['user']

def get_user_id(tweet_id):
    return get_user_dict(tweet_id)['id_str']

def get_handle(tweet_id):
    return get_user_dict(tweet_id)['screen_name']

def get_user_bio(tweet_id):
    return get_user_dict['descripton']

def orig_quote_or_rt(tweet_id):
    if get_text(tweet_id).startswith('RT'):
        return 'retweet'
    elif get_text(tweet_id).startswith('@'):
        return 'quote'
    else:
        return 'original'


# Uncomment if there is not TWEETS json file yet
#TWEETS = get_tweets(keyword, max_tweets)


# Populate memo variables with past known jediswap tweets (TWEETS) and their users
load_TWEETS_from_json(TWEETS_json_path)
load_USERS_from_json(USERS_json_path)


#At the end, save TWEETS to json file
#save_TWEETS_to_json()
