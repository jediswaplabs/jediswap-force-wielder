#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
In this file the functions for the scheduled querying of Twitter are defined.
Some filtering is also done at this level, i.e. any retweets are dropped.
"""

import os
import inspect
import requests
import re
from pprint import pp, pformat
from copy import deepcopy
from dotenv import load_dotenv
from helpers import *
load_dotenv('./.env')


jediswap_user_id = "1470315931142393857"
bearer_token = os.environ.get("TW_BEARER_TOKEN")

# Json file containing the most recent tweet id queried per function
last_queried_path = "./last_queried.json"

# Any filtered-out tweets go here for checking if filters work correctly
discarded_path = "./discarded_tweets.json"

# Regex filter patterns. Any tweet where a pattern matches the tweet text gets dropped.
filter_patterns = [
    {
    "name": "more_than_5_mentions",
    "pattern" : r"@\w+\s.*@\w+\s.*@\w+\s.*@\w+\s.*@\w+\s.*@\w+",
    "flag": "dotall"
    },
    {
    "name": "red_flag",
    "pattern" : r"airdrop",
    "flag": "ignorecase"
    },
]


def bearer_oauth(r) -> dict:
    """Method required by bearer token authentication."""
    r.headers["Authorization"] = f"Bearer {bearer_token}"
    return r

def get_query_params() -> dict:
    """Tweet information returned by api is defined here."""
    params = {
        "tweet.fields": "created_at,public_metrics,in_reply_to_user_id," + \
            "referenced_tweets,conversation_id",
        "user.fields": "id,username,entities,public_metrics",
        "expansions": "author_id,in_reply_to_user_id",
        "max_results": "100"
    }
    return params

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

def merge_user_data(tweets_list, users_list):
    """
    Helper function needed while querying the Twitter API.
    Takes the ["data"] and ["includes"]["users"] lists from the json_response,
    adds user parameters to their respective tweets matching "author_id" & "id"
    """
    out_list = []
    users_dict = {u["id"]: u for u in users_list}

    # Iterate over all tweets & copy over their user information from users_list
    for t in tweets_list:

        user_id = t["author_id"]
        u = users_dict[user_id]

        t["username"] = u["username"]
        t["followers_count"] = u["public_metrics"]["followers_count"]
        t["following_count"] = u["public_metrics"]["following_count"]
        t["tweet_count"] = u["public_metrics"]["tweet_count"]
        t["listed_count"] = u["public_metrics"]["listed_count"]

        out_list.append(t)

    return out_list

def paginated_query(url, params, bearer_token, infinite=False) -> list:
    """
    Queries pagewise for max results until last page. Returns list of tweets.
    Will abort if no end_trigger is set, unless "infinite" is set to True.
    """
    if not infinite:
        assert "since_id" in params, ("No end for querying defined. Will query until rate limit reached!")

    tweets_list = []
    users_list = []

    # First query. If no results & no error -> Return emtpy list
    json_response = connect_to_endpoint(url, params, bearer_token)
    meta = json_response["meta"]
    if "data" not in json_response:
        return []

    # Else continue querying until last (=oldest) page reached
    tweets = json_response["data"]
    users = json_response["includes"]["users"]

    tweets_list.extend(tweets)
    users_list.extend(users)

    while "next_token" in meta:

        params["pagination_token"] = meta["next_token"]
        json_response = connect_to_endpoint(url, params, bearer_token)
        meta = json_response["meta"]

        if "data" in json_response:

            tweets = json_response["data"]
            users = json_response["includes"]["users"]
            tweets_list.extend(tweets)
            users_list.extend(users)

    # Add user data back to original tweets
    out_list = merge_user_data(tweets_list, users_list)

    return out_list

def backup_end_triggers(json_path) -> None:
    """Creates a backup of most recent known tweet ids before running script."""
    out_path = json_path.replace(".json", "BAK.txt")
    id_dict = read_from_json(json_path)
    write_to_json(id_dict, out_path)

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

def remove_if_regex_matches(tweets, regex_p, discarded_json_path, discarded_key, regex_flag=None) -> list:
    """
    Takes a list of tweets. Discards where {regex_pattern} matches in tweet["text"].
    Saves/overwrites all discarded tweets to {discarded_json_path}, according to the
    {discarded_key} specified.
    """
    discarded = []
    out_tweets = []
    flags = 0

    if regex_flag == "multiline":
        flags = re.M
    if regex_flag == "dotall":
        flags |= re.S
    if regex_flag == "verbose":
        flags |= re.X
    if regex_flag == "ignorecase":
        flags |= re.I
    if regex_flag == "uni_code":
        flags |= re.U

    # Drop each tweet where regex matches
    for t in tweets:
        if re.search(regex_p, t["text"], flags=flags):
            discarded.append(t)
        else:
            out_tweets.append(t)

    # Save discarded tweets to json (for loading)
    out_d = read_from_json(discarded_json_path)
    out_d[discarded_key] = discarded
    write_to_json(out_d, discarded_json_path)

    # Save discarded tweets to txt (for viewing)
    outf = discarded_json_path.replace(".json", f"_{discarded_key}.txt")
    with open(outf, "w") as f:
        pf = pformat(discarded)
        f.write(pf)

    return out_tweets

def apply_filters(tweets, filters, discarded_json_path) -> list:
    """
    Takes a list of tweets. Returns the same list with all tweets removed where
    one of the regex {filter_patterns} matches in tweet["text"].
    Stores discarded tweets in json file, ordered by pattern name.
    """

    filtered_tweets = deepcopy(tweets)

    # Wipe old json file
    write_to_json(dict(), discarded_json_path)

    # Apply filters iteratively
    for f in filters:

        filtered_tweets = remove_if_regex_matches(
            filtered_tweets,
            regex_p=f["pattern"],
            discarded_json_path=discarded_json_path,
            discarded_key=f["name"],
            regex_flag=f["name"]
        )

    return filtered_tweets

def get_filtered_tweets():
    """Wrapper function to cleanly return dict of tweets ordered by tweet id."""

    # Fetch all new data since last queried
    backup_end_triggers(last_queried_path)
    new_mentions = get_new_mentions(jediswap_user_id, last_queried_path, bearer_token)
    new_quotes = get_new_quote_tweets(jediswap_user_id, last_queried_path, bearer_token)

    # Merge to one list & keep only 1 entry per tweet id
    tweets = merge_unique([new_mentions, new_quotes], unique_att="id")

    # Apply regex filters
    filtered_tweets = apply_filters(tweets, filter_patterns, discarded_path)

    # Convert to dictionary and return
    out_d = {t["id"]: t for t in filtered_tweets}
    return out_d


if __name__ == "__main__":
    get_filtered_tweets()


# DONE: Implement querying based on mentions of JediSwap account
# DONE: Implement querying based on quote tweets of tweets of JediSwap account
# DONE: Abstract away repetetive code
# DONE: Check which tweet attributes are needed, include "expansions" object while querying
# DONE: Filter out retweets using t["text"].startswith("RT") right after querying
# DONE: Filter out tweets with too many mentions right after querying using regex
# DONE: Merge tweet lists and discard doubles based on tweet id
# TODO: Rewrite main script to work with the new input data
# TODO: Add other filters
