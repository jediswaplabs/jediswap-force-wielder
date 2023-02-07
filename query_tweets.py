# query_tweets.py (new & python 3.9)

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

def bearer_oauth(r):
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

def get_new_mentions(user_id, last_queried_path, bearer_token):
    """
    Query mentions timeline of Twitter user until tweet id from
    {last_queried_path} encountered. Return list of all tweets newer
    than that id. Update this tweet id in the end.
    """
    new_mentions = []
    last_queried = read_from_json(last_queried_path)

    # Get most recent tweet id fetched by this method last time
    end_trigger = last_queried["id_last_mentioned_jediswap"]
    #end_trigger = '1622149104812843008' # Random tweet id (for testing only)
    newest_id = end_trigger
    func_name = inspect.currentframe().f_code.co_name

    # Define query parameters & return first page. Continue if more exist
    url = "https://api.twitter.com/2/users/{}/mentions".format(user_id)
    params = {
        "tweet.fields": "created_at,public_metrics,in_reply_to_user_id," + \
            "referenced_tweets,source",
        "user.fields": "id,username,entities,public_metrics",
        "max_results": "100",
        "since_id": end_trigger
    }
    json_response = connect_to_endpoint(url, params, bearer_token)
    meta, tweets = json_response["meta"], json_response["data"]
    new_mentions.extend(tweets)

    # Continue querying until last (=oldest) page reached
    while "next_token" in meta:

        params["pagination_token"] = meta["next_token"]
        json_response = connect_to_endpoint(url, params, bearer_token)
        meta = json_response["meta"]

        if "data" in json_response:

            tweets = json_response["data"]
            new_mentions.extend(tweets)

            if meta["newest_id"] > newest_id:
                newest_id = meta["newest_id"]

    # Update most recent id queried -> store in json file
    last_queried["id_last_mentioned_jediswap"] = newest_id
    write_to_json(last_queried, last_queried_path)

    # Add source attribute to tweets to trace potential bugs back to origin
    func_name = str(inspect.currentframe().f_code.co_name + "()")
    [x.update({"source": func_name}) for x in new_mentions]

    return new_mentions


new_mentions = get_new_mentions(jediswap_user_id, last_queried_path, bearer_token)

# DONE: Implement querying based on mentions of JediSwap account
# TODO: Implement querying based on quote tweets of tweets of JediSwap account
# TODO: Check which tweet attributes are needed, include expansion object while querying
# TODO: Filter out retweets using t["text"].startswith("RT") right after querying
# TODO: Filter out tweets with too many mentions right after querying using regex
# TODO: Merge tweet lists using sets & unions in the end to rule out doubles
# TODO: Rewrite main script to work with now very different input data
