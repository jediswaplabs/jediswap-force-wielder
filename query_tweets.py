"""
In this file the functions for the scheduled querying of Twitter are defined.
Some filtering is also done at this level, i.e. any retweets are dropped.
"""

import inspect
import requests
from dotenv import load_dotenv
from functions_twitter import *
load_dotenv('./.env')

# Json file containing the most recent tweet id per function
last_queried_path = "./last_queried.json"
jediswap_user_id = "1470315931142393857"
bearer_token = os.environ.get("TW_BEARER_TOKEN")

new_jediswap_tweets = []   # New tweets by official JediSwap will be appended here
new_mentions = []          # New tweets mentioning JediSwap will be appended here
new_quotes = []            # New tweets quoting JediSwap tweets will be appended here

def bearer_oauth(r) -> dict:
    """Method required by bearer token authentication."""
    r.headers["Authorization"] = f"Bearer {bearer_token}"
    return r

def connect_to_endpoint(url, params, bearer_token):
    """Wrapper for Twitter API queries."""
    response = requests.request("GET", url, auth=bearer_oauth, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )
    return response.json()

def paginated_query(url, params, bearer_token, infinite=False) -> list:
    """
    Queries pagewise for max results until last page. Returns list of tweets.
    Will abort if no end_trigger is set, unless "infinite" is set to True.
    """
    if not infinite:
        assert "since_id" in params, ("No end for querying defined. Will query until rate limit reached!")

    out_list = []

    # First query. If no results & no error -> Return emtpy list
    json_response = connect_to_endpoint(url, params, bearer_token)
    meta = json_response["meta"]
    if "data" not in json_response:
        return []

    # Else continue querying until last (=oldest) page reached
    tweets = json_response["data"]
    out_list.extend(tweets)

    while "next_token" in meta:

        params["pagination_token"] = meta["next_token"]
        json_response = connect_to_endpoint(url, params, bearer_token)
        meta = json_response["meta"]

        if "data" in json_response:

            tweets = json_response["data"]
            out_list.extend(tweets)

    return out_list

def backup_end_triggers(json_path) -> None:
    """Creates a backup of most recent known tweet ids before running script."""
    out_path = json_path.replace(".json", "BAK.txt")
    id_dict = read_from_json(json_path)
    write_to_json(id_dict, out_path)

def get_query_params() -> dict:
    """Tweet information returned by api is defined here."""
    params = {
        "tweet.fields": "created_at,public_metrics,in_reply_to_user_id," + \
            "referenced_tweets,conversation_id",
        "user.fields": "id,username,entities,public_metrics",
        "max_results": "100"
    }
    return params

def get_new_mentions(user_id, last_queried_path, bearer_token):
    """
    Queries mentions timeline of Twitter user until tweet id from
    {last_queried_path} encountered. Returns list of all tweets newer
    than that id. Updates this tweet id with newest id from this query.
    """
    # Get most recent tweet id fetched by this method last time
    last_queried = read_from_json(last_queried_path)
    end_trigger = last_queried["id_last_mentioned_jediswap"]
    end_trigger = '1622149104812843008' # TODO: Remove (for testing only)

    # Define query parameters & query for tweets. Skip rest if no results
    url = "https://api.twitter.com/2/users/{}/mentions".format(user_id)
    params = get_query_params()
    params["since_id"] = end_trigger
    new_mentions = paginated_query(url, params, bearer_token)
    if new_mentions == []:
        return []

    # Update most recent id in json file
    newest_from_query = sorted(new_mentions, key=lambda x: x["id"])[-1]["id"]
    newest_id = max(end_trigger, newest_from_query)
    last_queried["id_last_mentioned_jediswap"] = newest_id
    write_to_json(last_queried, last_queried_path)

    # Add source attribute to tweets to trace potential bugs back to origin
    func_name = str(inspect.currentframe().f_code.co_name + "()")
    [x.update({"source": func_name}) for x in new_mentions]

    return new_mentions

def get_new_tweets_by_user(user_id, last_queried_path, bearer_token):
    """
    Queries tweets timeline of Twitter user until tweet id from
    {last_queried_path} encountered. Returns list of all tweets newer
    than that id. Updates this tweet id in the end. Retweets are filtered out!
    """
    # Get most recent tweet id fetched by this method last time
    last_queried = read_from_json(last_queried_path)
    end_trigger = last_queried["id_last_jediswap_tweet"]
    end_trigger = "1621149172740268032" # Random tweet id (for testing only)

    # Define query parameters & query for tweets. Skip rest if no results
    url = "https://api.twitter.com/2/users/{}/tweets".format(user_id)
    params = get_query_params()
    params["since_id"] = end_trigger
    new_tweets = paginated_query(url, params, bearer_token)
    if new_tweets == []:
        return []

    # Update most recent id in json file
    newest_from_query = sorted(new_tweets, key=lambda x: x["id"])[-1]["id"]
    newest_id = max(end_trigger, newest_from_query)
    last_queried["id_last_jediswap_tweet"] = newest_id
    write_to_json(last_queried, last_queried_path)

    # Filter out retweets
    new_tweets = [t for t in new_tweets if not t["text"].startswith("RT")]

    # Add source attribute to tweets to trace potential bugs back to origin
    func_name = str(inspect.currentframe().f_code.co_name + "()")
    [x.update({"source": func_name}) for x in new_tweets]

    return new_tweets

def get_quotes_for_tweet(tweet_id, bearer_token):
    """Queries API for all quote tweets of {tweet_id}."""

    # Define query parameters & query for tweets. Skip rest if no results
    url = "https://api.twitter.com/2/tweets/{}/quote_tweets".format(tweet_id)
    params = get_query_params()
    quotes = paginated_query(url, params, bearer_token, infinite=True)
    if quotes == []:
        return []

    # Add source attribute to tweets to trace potential bugs back to origin
    func_name = str(inspect.currentframe().f_code.co_name + "()")
    [x.update({"source": func_name}) for x in quotes]

    return quotes

def get_new_quote_tweets(user_id, last_queried_path, bearer_token):
    """
    Queries API for all JediSwap tweets since the tweet id stored in the
    json file in {last_queried_path}. Discards retweets, iterates through
    results & returns all quote tweets for these tweets.
    Updates json from {last_queried_path} with new most recent JediSwap tweet id.
    """
    new_quotes = []
    new_jediswap_tweets = get_new_tweets_by_user(user_id, last_queried_path, bearer_token)
    tweet_ids = [t["id"] for t in new_jediswap_tweets]

    # Get quotes of each new tweet
    for t_id in tweet_ids:
        quotes = get_quotes_for_tweet(t_id, bearer_token)
        new_quotes.extend(quotes)

    return new_quotes

#backup_end_triggers(last_queried_path)
#new_mentions = get_new_mentions(jediswap_user_id, last_queried_path, bearer_token)
#new_jediswap_tweets = get_new_tweets_by_user(jediswap_user_id, last_queried_path, bearer_token)
#new_quotes = get_new_quote_tweets(jediswap_user_id, last_queried_path, bearer_token)


# DONE: Implement querying based on mentions of JediSwap account
# DONE: Implement querying based on quote tweets of tweets of JediSwap account
# DONE: Abstract away repetetive code
# TODO: Check which tweet attributes are needed, include expansion object while querying
# TODO: Filter out retweets using t["text"].startswith("RT") right after querying
# TODO: Filter out tweets with too many mentions right after querying using regex
# TODO: Merge tweet lists using sets & unions in the end to rule out doubles
# TODO: Rewrite main script to work with now very different input data
