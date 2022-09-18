import os, inspect, json, tweepy
import numpy as np
from dotenv import load_dotenv
from itertools import zip_longest


TWEETS = {}
USERS = {}
SUSPENDED_USERS = set()
UNAVAILABLE_TWEETS = set()
TWEETS_json_path = './TWEETS_memo.json'
USERS_json_path = './USERS_memo.json'
FOUND_IN_TWEETS = 0
HAD_TO_QUERY_TWEETS = 0
HAD_TO_QUERY_USERS = 0
HAD_TO_QUERY_CLIENT_FOR_TWEETS = 0
UNIVERSAL_MEMO = set()    # for use by functions called like df.apply(). Is wiped before use each time.
engagement_dict_path = './engagement_metrics_memo.json'


# only used to populate TWEETS when script is run for the first time
keyword = 'jediswap'
max_tweets = 2000

# Instantiate Twitter API
load_dotenv('./.env')
b_t, c_k, c_s, a_t, a_s = (
    os.environ['TW_BEARER_TOKEN'],
    os.environ['TW_CONSUMER_KEY'],
    os.environ['TW_CONSUMER_SECRET'],
    os.environ['TW_ACCESS_TOKEN'],
    os.environ['TW_ACCESS_SECRET']
    )
auth = tweepy.OAuthHandler(c_k, c_s)
auth.set_access_token(a_t, a_s)
api = tweepy.API(auth)


# Instantiate Twitter Client
client = tweepy.Client(
    bearer_token=os.environ['CLIENT_BEARER_TOKEN'],
    consumer_key=os.environ['CLIENT_API_ACCESS_TOKEN'],
    consumer_secret=os.environ['CLIENT_API_ACCESS_TOKEN_SECRET'],
    access_token=os.environ['OAUTH_CLIENT_ID'],
    access_token_secret=os.environ['OAUTH2_CLIENT_SECRET']
)



###  batch API querying / file processing

def obvious_print(msg):
    out_str = '\n' + '='*75 + '\n\t' + msg + '\n' + '='*75 + '\n'
    print(out_str)

def prettyprint(dict_, keys_label='metric', values_label='Twitter User ID'):
    print('\n{:^35} | {:^6}'.format(keys_label, values_label))
    print('-'*65)
    for k,v in dict_.items():
        print("{:35} | {:<20}".format(k,v))

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

def write_list_to_json(_list, path):
    '''
    Writes list to json file located in path variable.
    '''
    json_str = json.dumps(_list)
    with open(path, 'w') as jfile:
        json.dump(json_str, jfile)

def read_list_from_json(json_path):
    '''
    Reads json, returns list with contents of json file
    '''
    with open(json_path, 'r') as jfile:
        return json.loads(json.loads(jfile.read()))

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

def load_TWEETS_from_json(json_path):
    '''
    Populates global variable TWEETS when the script is starting.
    '''
    global TWEETS
    TWEETS = read_from_json(json_path)

def save_TWEETS_to_json(json_path):
    global TWEETS
    write_to_json(TWEETS, json_path)

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
    In case of querying error outside this scope, the first arg is assumed to be
    the tweet id.
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
            d['id'] = t    # id is taken from first arg in case of a querying error!
            d['text'] = '======= No data avaiable! Possibly due to country-specific age-restriction block by API ======='
        return d

    # prevent truncated tweet texts and lesser metadata for retweets:
    def text_wrapper(raw_tweet):
        if hasattr(raw_tweet, 'retweeted_status'):
            return raw_tweet.retweeted_status.full_text
        else:
            return raw_tweet['full_text']

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
    d['text'] = text_wrapper(t)
    d['truncated'] = t['truncated']
    d['user'] = t['user']
    d['retweet_count'] = t['retweet_count']
    d['in_reply_to_status_id'] = t['in_reply_to_status_id_str']
    d['in_reply_to_user_id'] = t['in_reply_to_status_id_str']
    d['in_reply_to_screen_name'] = t['in_reply_to_screen_name']
    d['favorite_count'] = t['favorite_count']
    d['retweeted_bool'] = t['retweeted']
    d['favorited_bool'] = t['favorited']
    d['in_reply_to_status_id'] = t['in_reply_to_status_id']


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
            d['id'] = t    # id is taken from first arg in case of a querying error!
            d['suspended'] = True
            d['bio'] = '======= User suspended or identified as bot and banned. No data avaiable! ======='
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

def grouper(iterable, n, fillvalue=None):
    '''
    Helper function to slice iterable int chunks of size n (for querying)
    '''
    args = [iter(iterable)] * n
    result = zip_longest(*args, fillvalue=fillvalue)
    return [list(x) for x in result]

def get_suspended_tweets(tweet_id_list):
    '''
    Takes list of tweet ids, returns subset (list) of
    tweet ids from suspended accounts (batch-querying client).
    '''
    chunked_ids = grouper(tweet_id_list, 100)
    suspended_tweet_ids = set()

    for chunk in chunked_ids:
        chunk = [x for x in chunk if x != None]
        response = client.get_tweets(chunk)
        to_add = {x['resource_id'] for x in response.errors}
        suspended_tweet_ids.update(to_add)

    return list(suspended_tweet_ids)

def get_engagement_batchwise(tweet_id_list, chunk_size=100):
    '''
    Takes list of tweet ids, returns a dictionary of shape
    {tweet_id: (n_retweets, n_replies, n_likes, n_quotes)}.
    '''
    def transform(metrics):
        return (metrics['retweet_count'],
                metrics['reply_count'],
                metrics['like_count'],
                metrics['quote_count']
                )

    chunked_ids = grouper(tweet_id_list, chunk_size)
    out_dict = {}

    for chunk in chunked_ids:
        chunk = [x for x in chunk if x != None]
        response = client.get_tweets(chunk, tweet_fields=['id', 'public_metrics'])
        to_add = {x['id']: transform(x['public_metrics']) for x in response[0]}
        out_dict.update(to_add)

    return out_dict

def expand_truncated(tweet_ids):
    '''
    Searches for truncated = True within list of tweet ids.
    Queries for full text of tweet, updates TWEETS global var
    and saves updated version to json.
    '''
    global TWEETS
    successfully_expanded = 0
    id_text_tups = []

    truncated = [x for x in tweet_ids if get_tweet(x)['truncated']]
    print(f'Found {len(truncated)} truncated tweets. Attempting to expand them...')

    # Query client for full texts
    chunked_ids = grouper(truncated, 100)

    for chunk in chunked_ids:
        chunk = [x for x in chunk if x != None]
        print('chunk size:', len(chunk))
        t_fields = ['attachments', 'author_id', 'context_annotations', 'conversation_id', 'created_at',
                'entities', 'geo', 'id', 'in_reply_to_user_id', 'lang', 'public_metrics',
                'referenced_tweets', 'reply_settings', 'source', 'text', 'withheld']
        response = client.get_tweets(chunk, tweet_fields=t_fields)
        [id_text_tups.append((str(t['id']), t['text'])) for t in response.data]

    # Save full texts to TWEETS global var
    for tup in id_text_tups:
        old_len = len(TWEETS[tup[0]]['text'])
        if old_len == len(tup[1]):
            continue
        TWEETS[tup[0]]['text'] = tup[1]
        new_len = len(TWEETS[tup[0]]['text'])
        if old_len < new_len:
            successfully_expanded += 1

    print(f'Successfully added the full text to {successfully_expanded} truncated tweets.')
    return truncated

def apply_to_series(pd_ser, func_name, **arg_dict):
    '''
    Wrapper to perform function on pandas column as list simultaneously.
    Separates values from index and column name, performs function on list of values,
    reattaches name+index, so that returned Series matches the input Series.
    '''
    ser = pd_ser
    name = ser.name
    index = ser.index
    data = ser.tolist()

    out_data = func_name(data, **arg_dict)

    return pd.Series(data=out_data, index=index, name=name)

def get_ts_creation_batchwise(tweet_ids):
    '''
    Takes list of tweet ids, returns timestamps of Tweet creation (batch-querying client)
    or alternatively an error description, maintaining the list order.
    Floats are assumed to be nans and result in a blank.
    '''
    # replace nans with 0
    tweet_ids = [0 if type(x) == float else x for x in tweet_ids]

    chunked_ids = grouper(tweet_ids, 100)
    out_dict = {k: '' for k in tweet_ids}

    for chunk in chunked_ids:
        chunk = [x for x in chunk if x != None]
        t_fields = ['created_at']
        response = client.get_tweets(chunk, tweet_fields=t_fields)
        data, errors = response[0], response[2]
        # get timestamp or error per Tweet ID from response
        ids_ts = {str(x['id']): x['created_at'] for x in data}
        ids_errors = {str(x['value']): x['title'] for x in errors}
        out_dict.update(ids_ts)
        out_dict.update(ids_errors)

    out_list = [out_dict[_id] for _id in tweet_ids]
    return out_list

def update_memos(u_p=USERS_json_path, tw_p=TWEETS_json_path):
    global TWEETS
    global USERS
    write_to_json(TWEETS, tw_p)
    write_to_json(USERS, u_p)
    print(f'Updated {TWEETS_json_path.strip("./")} and {USERS_json_path.strip("./")}')

def update_engagement_memo(tweet_ids, eng_dict_path=engagement_dict_path, chunk_size=100):
    '''
    Wrapper function for get_engagement_batchwise(). Queries Twitter client in chunks
    of 100 tweet IDs (allowed maximum) for engagement metrics (retweets, replies, likes, quotes).
    Updates the local json file as specified in {eng_dict_path}.
    Returns dictionary of gathered data (for debugging only).
    '''

    # Query for engagement data
    print(f'Updating {eng_dict_path.lstrip("./")} with up-to-date tweet engagement metrics...')
    engagement_dict = get_engagement_batchwise(tweet_ids, chunk_size=chunk_size)

    # Update local memo file
    write_to_json(engagement_dict, eng_dict_path)
    print(f'Successfully updated {eng_dict_path.lstrip("./")}.')
    print(f'Engagement data for {len(engagement_dict)} tweets updated.')

    return engagement_dict





###  getter functions

def get_TWEETS():
    global TWEETS
    return TWEETS

def get_USERS():
    global USERS
    return USERS

def get_SUSPENDED_USERS():
    global SUSPENDED_USERS
    return SUSPENDED_USERS

def get_UNIVERSAL_MEMO():
    global UNIVERSAL_MEMO
    return UNIVERSAL_MEMO

def reset_UNIVERSAL_MEMO():
    global UNIVERSAL_MEMO
    UNIVERSAL_MEMO = set()

def query_API_for_tweet_obj(_id):
    global TWEETS
    global UNAVAILABLE_TWEETS
    global HAD_TO_QUERY_TWEETS

    # Case: Tweet flagged as by suspended user this session. Don't query API
    if _id in UNAVAILABLE_TWEETS:
        print(f'Found {_id} in UNAVAILABLE_TWEETS set')
        return tweet_to_dict(_id, fill_with_nan=True)

    TweepyException = tweepy.errors.TweepyException
    # If not suspended, try querying API for tweet data
    print(f'\nHad to query API for Tweet {_id}')
    try:
        tweet_obj = api.get_status(_id, tweet_mode='extended')
        jsonized = tweet_obj._json
        result = tweet_to_dict(jsonized)
        TWEETS[_id] = result
        HAD_TO_QUERY_TWEETS += 1
        return result

    # if not available, return dataset filled with nans
    except TweepyException as e:
        print('Caught an exception for this one:')

        if isinstance(e, tweepy.errors.NotFound):
            print('Exception:', e)
            print(f'No data available for tweet id {_id}')
            link = 'www.twitter.com/i/web/status/'+str(_id)
            print('\nCountry-specific age-restriction block? Check manually:')
            print(link)
            print('\n')
            UNAVAILABLE_TWEETS.add(_id)
            print(f'{_id} added to UNAVAILABLE_TWEETS')
            return tweet_to_dict(_id, fill_with_nan=True)

        elif isinstance(e, tweepy.errors.Forbidden):
            print('Tweet id:', _id)
            print('User suspended, no tweets available.')
            print('Error description:\n', e)
            UNAVAILABLE_TWEETS.add(_id)
            print(f'{_id} added to UNAVAILABLE_TWEETS')
            return tweet_to_dict(_id, fill_with_nan=True)

        else:
            print(f'Caught an unhandled error for id {_id}')
            print(type(e))
            print('\nError description:\n', e)
            return tweet_to_dict(_id, fill_with_nan=True)

def query_API_for_user_obj(user_id):
    global USERS
    global SUSPENDED_USERS
    global HAD_TO_QUERY_USERS

    # Case: User flagged as by suspended user this session. Don't query API, return dummy dict with nans
    if user_id in SUSPENDED_USERS:
        print(f'Found {user_id} in SUSPENDED_USERS set')
        return user_to_dict({'id_str': user_id}, fill_with_nan=True)

    TweepyException = tweepy.errors.TweepyException

    # If not suspended, try querying API for tweet data
    print(f'\nHad to query API for User {user_id}')
    try:
        user_obj = api.get_user(user_id=user_id)
        jsonized = user_obj._json
        result = user_to_dict(jsonized)
        USERS[user_id] = result
        HAD_TO_QUERY_USERS += 1
        return result
    # if not available, return dataset filled with nans

    except TweepyException as e:
        func_name = inspect.currentframe().f_code.co_name
        print(f'Caught an unhandled error for user: {user_id}')
        print(type(e))
        print('\nError description:\n', e)
        SUSPENDED_USERS.add(user_id)
        print('Added user to SUSPENDED_USERS')
        return user_to_dict(user_id, fill_with_nan=True)

def query_client_for_tweet_data(tweet_id):
    '''
    This function uses API v2, maybe faster?
    Returns Twitter api.Client response for tweet or list of tweets.
    Returns None if tweet doesn't exist anymore.
    Also prints error message if querying the client goes wrong in any way.
    '''
    global UNAVAILABLE_TWEETS
    global HAD_TO_QUERY_CLIENT_FOR_TWEETS

    # Case: Tweet doesn't exist anymore
    if tweet_id in UNAVAILABLE_TWEETS:
        print(f'Tweet {tweet_id} found in UNAVAILABLE_TWEETS. Not querying.')
        return None

    response = client.get_tweets(
        ids=[tweet_id],
        tweet_fields=["public_metrics"],
        expansions=["attachments.media_keys"],
        media_fields=["public_metrics"]
        )
    HAD_TO_QUERY_CLIENT_FOR_TWEETS += 1

    # Case: Tweet found. Return tweet data
    if response.errors == []:
        return response.data[0]

    # Case: Tweet not found or other error occurred
    else:
        func_name = inspect.currentframe().f_code.co_name
        print(f'\n{func_name}(): Caught an error querying client for Tweet ID {tweet_id}.')
        print('\nError:\n')
        [print(x) for x in response.errors]
        UNAVAILABLE_TWEETS.add(tweet_id)
        print('\n\t---> tweet has been added to UNAVAILABLE_TWEETS')
        return None

def get_tweet(tweet_id):
    '''
    Wrapper to save on API queries per minute:
    Checks local json object containing tweets first for a tweet id.
    If it's not in there, queries twitter API. Returns available
    tweet metadata.
    '''
    global TWEETS
    global FOUND_IN_TWEETS

    # try downloaded tweets first
    if TWEETS != {} and tweet_id in TWEETS:
        FOUND_IN_TWEETS += 1
        return TWEETS[tweet_id]

    # if not in there, query Twitter API
    else:
        return query_API_for_tweet_obj(tweet_id)

def get_user(user_id):
    '''
    Wrapper to save on API queries per minute:
    Checks local json object containing users first for a user id.
    If it's not in there, queries twitter API. Returns user
    object.
    '''
    global USERS

    def cond_log(msg):
        if user_id == None:
            print(msg)

    # try downloaded tweets first
    if USERS != {} and user_id in USERS:
        cond_log(f'trying to get user id {user_id} from USERS dict...')
        return USERS[user_id]

    # if not in there, query Twitter API
    else:
        cond_log(f'calling query_API_for_user_obj(user_id) with user id {user_id}...')
        print(f'Had to query API for User ID {user_id}')
        return query_API_for_user_obj(user_id)



### tweet-related

def get_text(tweet_id):
    try:
        return get_tweet(tweet_id)['text']
    except TypeError:
        return np.nan

def get_retweet_count(_id):
    result = 0
    try:
        result = int(get_tweet(_id)['retweet_count'])
    except KeyError:
        result = int(get_tweet(_id)['retw_count'])
    finally:
        if result == np.nan:
            return np.nan
        else:
            return int(result)

def get_engagement(tweet_id):
    '''
    Queries Twitter api.Client (API v2) for a tweet id,
    returns a dict of these engagement metrics:
    quote count, retweet count, reply count, like count
    '''
    tweet_data = query_client_for_tweet_data(tweet_id)
    if tweet_data is None:
        return None
    else:
        return tweet_data.public_metrics

def get_retr_repl_likes_quotes_count(tweet_id, memo_path=engagement_dict_path):
    '''
    Queries Twitter api.Client (API v2), returns tuple of
    engagement counts (retweets, replies, likes, quotes).
    Returns tuple of np.nan's if tweet doesn't exist anymore.
    '''
    memo_d = {}
    global UNAVAILABLE_TWEETS

    # Case: Tweet is from suspended user. Return tuple of nan's
    if tweet_id in UNAVAILABLE_TWEETS:
        return (np.nan, np.nan, np.nan, np.nan)

    # Querying the client will fail after ~150 requests. That's why a json file is used as memo storage
    if memo_path is not None:
        memo_d = read_from_json(memo_path)

    # Case: Tweet engagement is stored in local json file. Read from there.
    if tweet_id in memo_d:
        p = memo_path.strip('./')
        print(f'Found {tweet_id} in {p}.')
        return tuple(memo_d[tweet_id])

    # Case: Tweet is not contained in json: Query Twitter client (max ~150 queries per 15 min.)
    else:
        d = get_engagement(tweet_id)
        p = memo_path.strip('./')
        print(f'Couldn\'t find {tweet_id} in {p}. Queried client instead')

    # Case: Tweet doesn't exist anymore. Return tuple of nan's
    if d is None:
        out_tup = (np.nan, np.nan, np.nan, np.nan)

    else:
        print('d:', d)
        out_tup = (
            d['retweet_count'],
            d['reply_count'],
            d['like_count'],
            d['quote_count']
        )

    return out_tup

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
    try:
        return get_user_dict(tweet_id)['id_str']
    except TypeError:
        return np.nan

def get_handle(tweet_id):
    return get_user_dict(tweet_id)['screen_name']

def get_user_bio(tweet_id):
    return get_user_dict['descripton']

def orig_quote_or_rt(tweet_id):
    try:
        if get_text(tweet_id).startswith('RT'):
            return 'retweet'
        elif get_text(tweet_id).startswith('@'):
            return 'quote'
        else:
            return 'original'
    # Case: User suspended, no tweet data available anymore
    except AttributeError:
        return np.nan



### pandas-related

def check_for_nan(val):
    if np.isnan(float(val)):
        return True
    else:
        return ' '

def flag_as_duplicate(_id):
    '''
    For use as df.apply() over a DataFrame sorted by timestamps.
    Flags duplicate entries of a column as duplicates if a version
    with an earlier version (according to timestamp) exists.
    '''
    global UNIVERSAL_MEMO
    if _id not in UNIVERSAL_MEMO:
        UNIVERSAL_MEMO.add(_id)
        return ' '
    elif np.isnan(float(_id)):
        return ' '
    else:
        return True

def update_suspension_flags(df):
    '''
    batch-queries all tweet ids in the dataset and
    updates SUSPENDED_USERS and the suspension flags.
    '''
    # Get list of all tweet ids from suspended accounts
    all_ids = list(df['Tweet ID'].unique())
    all_ids = [x for x in all_ids if not np.isnan(float(x))]
    suspended_tweets = get_suspended_tweets(all_ids)

    def flag_as_suspended(_id):
        if _id not in suspended_tweets:
            return ' '
        elif np.isnan(float(_id)):
            return ' '
        else:
            return True

    # Add flag to df for each tweet id from a suspended account
    df['Suspended Twitter User'] = df['Tweet ID'].apply(flag_as_suspended)
    return df

def set_reply_flags(df):
    '''
    Queries each tweet ID and adds a bool flag if tweet is a reply.
    '''
    def set_flag(t_id):
        global UNAVAILABLE_TWEETS
        if t_id in UNAVAILABLE_TWEETS:
            return ''
        tweet = get_tweet(t_id)
        in_reply_to = tweet['in_reply_to_status_id']
        if in_reply_to == None:
            return ''
        else:
            return True

    df['Tweet is reply'] = df['Tweet ID'].apply(set_flag)
    return df

def set_thread_flags(df):
    '''
    Queries each tweet ID and adds a bool flag for all tweets out of threads except for the first tweet.
    '''
    def set_flag(t_id):
        global UNAVAILABLE_TWEETS
        if t_id in UNAVAILABLE_TWEETS:
            return ''
        tweet = get_tweet(t_id)
        if (tweet['user']['screen_name'] == tweet['in_reply_to_screen_name']) and (
            tweet['in_reply_to_status_id'] != None):
            return True
        else:
            return ''

    df['Follow-up tweet from thread'] = df['Tweet ID'].apply(set_flag)
    return df

def set_mentions_flags(df):
    '''
    Queries each tweet ID and adds a bool flag if tweet has more than 5 mentions of unique Twitter handles.
    '''
    def set_flag(t_id):
        global UNAVAILABLE_TWEETS
        if t_id in UNAVAILABLE_TWEETS:
            return ''
        tweet = get_tweet(t_id)
        mentions = tweet['entities']['user_mentions']
        if mentions == []:
            return ''
        else:
            unique_handles = {x['screen_name'] for x in mentions}
            if len(unique_handles) > 5:
                return True
            else:
                return ''

    df['>5 mentions'] = df['Tweet ID'].apply(set_flag)

    return df


def set_more_than_5_tweets_flag(df):
    '''
    Adds a column 'Tweet #6 or higher per month' to the dataset.
    Copies the dataset and sorts it by total points. Iterates through rows and keeps count
    of each twitter handle. Flags every 6th or higher occurence of this handle.
    '''

    df['Total Points'] = df['Total Points'].replace(' ', 0)
    months = list(df['Month'].unique())
    df['Handle Counter'] = 0

    def set_flag(handle_count):
        if handle_count > 5:
            return True
        else:
            return ''

    for month in months:
        monthly_subset = df.loc[df['Month'] == month]
        sorted_by_points = monthly_subset.sort_values('Total Points', ascending=False)
        sorted_by_points['Handle Counter'] = sorted_by_points.groupby('Twitter Handle').cumcount()+1
        df['Handle Counter'].update(sorted_by_points['Handle Counter'])

    # Only count twitter-related submissions
    non_twitter = df['Non-Twitter Submission'] == True
    df.loc[non_twitter, 'Handle Counter'] = 0
    df['Tweet #6 or higher per month'] = df['Handle Counter'].apply(set_flag)

    return df


def set_multiple_links_flag(row):
    '''
    Adds a flag 'Multiple links submitted' to each row containing a valid twitter
    link and a ' ', indicating another link has been submitted.
    '''
    if (row['Non-Twitter Submission'] != True) and (
        row['Submit a Link to your tweet, video or article'].find(' ') != -1):
        return True
    else:
        return False

def add_multiple_links_comment(row):
    '''
    Adds a comment 'Please submit each link as a single entry' to each row
    with the 'Multiple links submitted' flag set
    '''
    if (row['Multiple links submitted'] == True):
        return 'Please submit each link as a single entry'
    else:
        return ' '

def tweet_points_formula(n_followers, n_retweets, n_quotes, duplicate, inval_link):
    if (duplicate == True) or (inval_link == True):
        return np.nan
    retweet_bonus = 3.5 * (n_retweets + n_quotes)**(1/1.2)
    tweet_points = 0.1 * n_followers **(1/1.6) + retweet_bonus
    rounded = int(round(tweet_points)) if not np.isnan(tweet_points) else tweet_points
    return rounded

def follower_points_formula(n_followers, duplicate, inval_link):
    if (duplicate == True) or (inval_link == True):
        return np.nan
    follower_points = 0.1 * n_followers **(1/1.6)
    rounded = int(round(follower_points)) if not np.isnan(follower_points) else follower_points
    return rounded

def retweet_points_formula(n_retweets, n_quotes, duplicate, inval_link):
    if (duplicate == True) or (inval_link == True):
        return np.nan
    retweet_points = 3.5 * (n_retweets + n_quotes)**(1/1.2)
    rounded = int(round(retweet_points)) if not np.isnan(retweet_points) else retweet_points
    return rounded

def safe_to_int(val):
    '''
    Coverts float to int and str or np.nan to ' '.
    '''
    if isinstance(val, str) or np.isnan(float(val)):
        return ' '
    else:
        return int(val)

def set_red_flag(_id, trigger='airdrop'):
    content = get_text(_id).lower()
    if trigger in content:
        return True
    else:
        return ' '

def set_contains_jediswap_flag(_id, trigger='jediswap'):
    content = get_text(_id).lower()
    if trigger in content:
        return True
    else:
        return ' '

def set_jediswap_quote_flag(_id):
    t = get_tweet(_id)
    # Catch TypeError for suspended users (nan values everywhere)
    if type(t['entities']) != dict:
        return ' '

    urls = t['entities']['urls']
    jediswap_quotes = [x for x in urls if 'jediswap' in x['expanded_url'].lower()]
    if jediswap_quotes != []:
        return True
    else:
        return ' '

def check_if_unrelated(row):
    if (row['Non-Twitter Submission'] == True) or (
            row['Suspended Twitter User'] == True) or (
                row['Twitter Handle'] == '@JediSwap'):
        return ' '

    elif (row['contains jediswap'] != True) and (
            row['quotes jediswap'] != True):
        return True
    else:
        return ' '

def row_handler(row):
    if (row['Suspended Twitter User'] == True) or (
        row['Non-Twitter Submission'] == True):
        return ' '
    else:
        return row['Tweet Preview']

def correct_follower_p(row):
    if (row['Suspended Twitter User'] == True) or (
        row['Non-Twitter Submission'] == True) or (
        row['Multiple links submitted'] == True) or (
        row['Duplicate'] == True) or (
        row['Red Flag'] == True):
        return ' '
    else:
        return row['Follower Points']

def correct_retweet_p(row):
    if (row['Suspended Twitter User'] == True) or (
        row['Non-Twitter Submission'] == True) or (
        row['Multiple links submitted'] == True) or (
        row['Duplicate'] == True) or (
        row['Red Flag'] == True):
        return ' '
    else:
        return row['Retweet Points']

def correct_total_p(row):
    if (row['Suspended Twitter User'] == True) or (
        row['Multiple links submitted'] == True) or (
        row['Duplicate'] == True) or (
        row['Red Flag'] == True) or(
        row['Non-Twitter Submission'] == True) or (
        row['Tweet #6 or higher per month'] == True) or (
        row['Tweet is reply'] == True) or (
        row['Unrelated to JediSwap'] == True):
        return 0
    else:
        return row['Total Points']

def add_points_denied_comment(row):
    msg_list = []
    flag_list = [
        'Duplicate', 'Suspended Twitter User', 'Red Flag', 'Non-Twitter Submission',
        'Multiple links submitted', 'Tweet #6 or higher per month', 'Tweet is reply',
        'Unrelated to JediSwap'
        ]
    for flag in flag_list:
        if row[flag] == True:
            msg_list.append(flag)
    if msg_list != []:
        comment = 'No points given. Reason: ' + ', '.join(msg_list)
        comment = comment.replace('Suspended Twitter User', 'User suspended or tweet deleted')
        comment = comment.replace('Non-Twitter Submission', 'Not a tweet')
        return comment
    else:
        return row['Comments']

# Uncomment if TWEETS json file is still empty (if running for first time i.e.)
#TWEETS = get_tweets(keyword, max_tweets)

# Populate memo variables with past known jediswap tweets (TWEETS) and their users
load_TWEETS_from_json(TWEETS_json_path)
load_USERS_from_json(USERS_json_path)
