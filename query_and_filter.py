#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
If executed directly, runs get_filtered_tweets(). Tweet ids until which the
paginated querying moves backwards in time along the Twitter timelines can be
passed via {cutoff_ids}. After a filtering stage a merged dictionary of unique
tweets is returned, containing:

    1) every new mention of {target_user_id} since last execution
    2) every new quote tweet of tweets by {target_user_id} since last execution

Filtering: Any tweet will be dropped if a regex pattern from {filter_patterns}
matches within tweet["text"]. If any query function is called directly, stopping
parameters can be passed using {add_params}. Examples:

    add_params["since_id"] = "<tweet id>"
    add_params["end_time"] = "2023-01-10T00:00:00.000Z"
    add_params["start_time"] = "2023-02-01T00:00:00.000Z"
"""

import os
import inspect
import requests
import re
from ast import literal_eval
from os.path import exists
from pprint import pp, pformat
from copy import deepcopy
from time import sleep
from dotenv import load_dotenv
from helpers import *
load_dotenv('./.env')

target_user_id = os.environ.get("TWITTER_USER_ID")
bearer_token = os.environ.get("API_BEARER_TOKEN")
N_TWEETS_QUERIED = 0

# Any filtered-out tweets go here for checking if filters work correctly
discarded_path = "./discarded_tweets.json"

# Regex filter patterns. Any tweet where a pattern matches the tweet text gets dropped.
filter_patterns = [
    #{
    #"name": "more_than_5_mentions",
    #"pattern" : r"@\w+.?\s.*@\w+.?\s.*@\w+.?\s.*@\w+.?\s.*@\w+.?\s.*@\w+",
    #"flag": "dotall"
    #},
    {
    "name": "red_flag",
    "pattern" : r"airdrop",
    "flag": "ignorecase"
    },
    {
    "name": "retweets",
    "pattern" : r"^RT",
    "flag": None
    }
]

def bearer_oauth(r) -> dict:
    """Method required by bearer token authentication."""
    r.headers["Authorization"] = f"Bearer {bearer_token}"
    return r

def get_query_params() -> dict:
    """Tweet information returned by api is defined here."""
    params = {
        "tweet.fields": "created_at,public_metrics,in_reply_to_user_id," + \
            "referenced_tweets,conversation_id,entities",
        "user.fields": "id,username,entities,public_metrics",
        "expansions": "author_id,in_reply_to_user_id",
        "max_results": "100"
    }
    return params

def connect_to_endpoint(url, params, bearer_token) -> tuple:
    """Wrapper for Twitter API queries. Returns response & status code."""
    response = requests.request("GET", url, auth=bearer_oauth, params=params)
    print(response.status_code)

    handled_quietly = {200, 429}

    if response.status_code not in handled_quietly:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )
    return (response.json(), response.status_code)

def merge_user_data(tweets_list, users_list) -> list:
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

def simple_query(url, params, bearer_token, infinite=False) -> list:
    """
    Queries Twitter API as specified in {url} & {params}.
    Returns tuple (list_of_tweets, response_status_code).
    Tweets causing Authorization or Not Found Error are dropped.
    """
    global N_TWEETS_QUERIED
    json_response, status_code = connect_to_endpoint(url, params, bearer_token)

    if status_code == 429:
        print("Rate limit reached (429: Too many requests). Waiting for 16m to continue querying.")
        sleep(16*60)
        json_response, status_code = connect_to_endpoint(url, params, bearer_token)
    
    if "errors" in json_response:
        [print(x["title"] + ":", x["detail"]) for x in json_response["errors"]]
    if "data" not in json_response:
        return ([], status_code)

    tweets = json_response["data"]
    N_TWEETS_QUERIED += len(tweets)
    users = json_response["includes"]["users"]
    merged = merge_user_data(tweets, users)

    return (merged, status_code)

def paginated_query(url, params, bearer_token, infinite=False) -> list:
    """
    Queries pagewise for max results until last page. Returns list of tweets
    and most recent query status code. Will abort if no end_trigger is set,
    unless "infinite" is set to True.
    """
    global N_TWEETS_QUERIED
    if not infinite:
        assert ("since_id" or "start_time" in params), ("No end for querying defined. Will query until rate limit reached!")

    tweets_list = []
    users_list = []

    # First query. If no results & no error -> Return emtpy list
    json_response, status_code = connect_to_endpoint(url, params, bearer_token)

    # If rate limit reached (TooManyRequests) -> wait for 16m and continue querying
    if status_code == 429:
            print("Rate limit reached (429: Too many requests). Waiting for 16m to continue querying.")
            sleep(16*60)
            json_response, status_code = connect_to_endpoint(url, params, bearer_token)

    # If end of data reached (last page) -> abort here & return emtpy list
    meta = json_response["meta"]
    if "data" not in json_response:
        return ([], status_code)

    # Else continue querying until last (=oldest) page reached
    tweets = json_response["data"]
    users = json_response["includes"]["users"]

    tweets_list.extend(tweets)
    users_list.extend(users)

    N_TWEETS_QUERIED += len(tweets)

    # Query for a next page as long as there is one & API rate limit is not exceeded
    while ("next_token" in meta) and status_code != 429:

        params["pagination_token"] = meta["next_token"]
        json_response, status_code = connect_to_endpoint(url, params, bearer_token)
        meta = json_response["meta"]

        if "data" in json_response:

            tweets = json_response["data"]
            users = json_response["includes"]["users"]
            tweets_list.extend(tweets)
            users_list.extend(users)
            N_TWEETS_QUERIED += len(tweets)

    # Add user data back to original tweets
    out_list = merge_user_data(tweets_list, users_list)

    return (out_list, status_code)

def parse_date_range(tweets: list) -> str:
    """Takes a tweets list, returns a str of the earliest & latest tweet date."""
    dates = sorted([t["created_at"] for t in tweets])
    stripped = [d[:d.find('T')] for d in dates]
    earliest = stripped[0]
    latest = stripped[-1]
    return f"{earliest} - {latest}"

def get_cutoffs(csv_path) -> dict:
    """
    Loads DataFrame from {csv_path}. Searches through column "source".
    Returns a dictionary of type {func_1: "<highest tweet id>", ...}
    """

    cutoff_d = {}
    js_tweet_ids = set()

    # Load df
    df = csv_to_df(csv_path)

    # Get most recent mention, skip if none found
    mentions = df[df["source"] == "get_new_mentions()"]["id"].tolist()
    if mentions != []:
        cutoff_d["get_new_mentions()"] = max(mentions)

    # Get ids of quoted JediSwap tweets
    quoted_referenced_tweets = df[df["source"] == "get_quotes_for_tweet()"]["referenced_tweets"]

    for l in quoted_referenced_tweets:
        referenced_tweets_list = literal_eval(l)
        for t in referenced_tweets_list:
            if t["type"] == "quoted":
                js_tweet_ids.add(t["id"])

    # Add most recent id of quoted tweets, skip if none found.
    if js_tweet_ids != set():
        cutoff_d["get_quotes_for_tweet()"] = max(js_tweet_ids)

    return cutoff_d

def tweets_to_json(tweets: list, name: str) -> None:
    """Saves a tweets list to json. Appends its date range to name."""
    if tweets == []:
        return f"EMPTY {name}.json"

    date_range = parse_date_range(tweets)
    out_name = f"{date_range} unfiltered {name}.json"
    write_list_to_json(tweets, out_name)

def query_tweets(url, params, bearer_token) -> list:
    """
    Queries for multiple (max 100) tweets. Merges user & tweet data.
    Returns list of tweets and most recent query status code.
    """
    global N_TWEETS_QUERIED
    tweets_list = []
    users_list = []

    # Query. If no results & no error -> Return emtpy list
    json_response, status_code = connect_to_endpoint(url, params, bearer_token)

    # If rate limit reached (TooManyRequests) -> wait for 16m and continue querying
    if status_code == 429:
            print("Rate limit reached (429: Too many requests). Waiting for 16m to continue querying.")
            sleep(16*60)
            json_response, status_code = connect_to_endpoint(url, params, bearer_token)

    # If nothing found -> abort here & return emtpy list
    if "data" not in json_response:
        return ([], status_code)

    # Merge tweet data with corresponding user data
    tweets = json_response["data"]
    users = json_response["includes"]["users"]
    tweets_list.extend(tweets)
    users_list.extend(users)

    N_TWEETS_QUERIED += len(tweets)

    # Add user data back to original tweets
    out_list = merge_user_data(tweets_list, users_list)

    return (out_list, status_code)

def get_tweets(id_list, bearer_token, add_params=None) -> list:
    """
    Assumes list of tweet ids.
    Queries Twitter API in chunks of 100 tweets per query (maximum).
    Returns list of tweet dictionaries.
    """
    def chunk_list(_list, n):
        for i in range(0, len(_list), n):
            yield _list[i:i+n]

    out_tweets = []
    tweets_per_query = 100
    id_chunk = list(chunk_list(id_list, tweets_per_query))

    params = get_query_params()
    if add_params:
        params.update(add_params)
    del params["max_results"]
    
    # Query 100 tweets at a time
    for ids in id_chunk:
        
        id_str = "ids=" + ",".join(ids)
        url = "https://api.twitter.com/2/tweets?{}".format(id_str)
        tweets, status_code = query_tweets(url, params, bearer_token)
        out_tweets.extend(tweets)

        if status_code == 429:
                print("Rate limit reached (429: Too many requests). Waiting for 16m to continue querying.")
                sleep(16*60)
                tweets, status_code = query_tweets(url, params, bearer_token)
                out_tweets.extend(tweets)

    if out_tweets == []:
        return []

    # Add function name to tweets & save queried data to json as backup
    func_name = str(inspect.currentframe().f_code.co_name + "()")
    [x.update({"source": func_name}) for x in out_tweets]
    tweets_to_json(out_tweets, func_name)

    return out_tweets

def get_new_mentions(user_id, bearer_token, add_params=None) -> list:
    """
    Queries mentions timeline of Twitter user until tweet id from
    {last_queried_path} encountered. Returns list of all tweets newer
    than that id. Updates this tweet id with newest id from this query.
    """

    # Define query parameters
    url = "https://api.twitter.com/2/users/{}/mentions".format(user_id)
    params = get_query_params()

    # Add any additional query parameters from {add_params} dictionary
    if add_params:
        params.update(add_params)

    # Query for tweets. Skip rest if no results or rate limit reached.
    new_mentions, status_code = paginated_query(url, params, bearer_token)

    if status_code == 429:
        print(f"Api rate limit reached. Waiting for 16m to get new mentions for user {user_id}.")
        sleep(16*60)
        new_mentions, status_code = paginated_query(url, params, bearer_token)

    if new_mentions == []:
        return []

    # Add source attribute to tweets to trace potential bugs back to origin
    func_name = str(inspect.currentframe().f_code.co_name + "()")
    [x.update({"source": func_name}) for x in new_mentions]

    # Save queried data to json as backup
    tweets_to_json(new_mentions, func_name)

    return new_mentions

def get_new_tweets_by_user(user_id, bearer_token, add_params=None) -> list:
    """
    Queries tweets timeline of Twitter user until tweet id from
    {last_queried_path} encountered. Returns list of all tweets newer
    than that id. Updates this tweet id in the end. Retweets are filtered out.
    """

    # Define query parameters
    url = "https://api.twitter.com/2/users/{}/tweets".format(user_id)
    params = get_query_params()

    # Add any additional query parameters from {add_params} dictionary
    if add_params:
        params.update(add_params)

    # Query for tweets. Skip rest if no results
    new_tweets, status_code = paginated_query(url, params, bearer_token)

    if status_code == 429:
        print(f"Api rate limit reached. Stopped querying for tweets by user {user_id}.")
        sleep(16*60)
        new_tweets, status_code = paginated_query(url, params, bearer_token)

    if new_tweets == []:
        return []

    # Filter out retweets
    new_tweets = [t for t in new_tweets if not t["text"].startswith("RT")]

    # Add source attribute to tweets to trace potential bugs back to origin
    func_name = str(inspect.currentframe().f_code.co_name + "()")
    [x.update({"source": func_name}) for x in new_tweets]

    # Save queried data to json as backup
    tweets_to_json(new_tweets, func_name)

    return new_tweets

def get_quotes_for_tweet(tweet_id, bearer_token) -> tuple:
    """Queries API for all quote tweets of {tweet_id}."""

    # Define query parameters & query for tweets. Skip rest if no results
    url = "https://api.twitter.com/2/tweets/{}/quote_tweets".format(tweet_id)
    params = get_query_params()
    quotes, status_code = paginated_query(url, params, bearer_token, infinite=True)

    if status_code == 429:
        print(f"Api rate limit reached while querying quote tweets of tweet {tweet_id}.")
        print("Waiting for 16m and continuing to query after.")
        sleep(16*60)
        quotes, status_code = paginated_query(url, params, bearer_token, infinite=True)

    if quotes == []:
        return ([], status_code)

    # Add source attribute to tweets to trace potential bugs back to origin
    func_name = str(inspect.currentframe().f_code.co_name + "()")
    [x.update({"source": func_name}) for x in quotes]

    return (quotes, status_code)

def get_new_quote_tweets(user_id, bearer_token, add_params=None) -> list:
    """
    Queries API for all JediSwap tweets since the tweet id stored in the
    json file in {last_queried_path}. Discards retweets, iterates through
    results & returns all quote tweets for these tweets.
    Updates json from {last_queried_path} with new most recent JediSwap tweet id.
    """

    new_quotes = []
    new_jediswap_tweets = get_new_tweets_by_user(
        user_id,
        bearer_token,
        add_params=add_params
    )
    tweet_ids = [t["id"] for t in new_jediswap_tweets]

    print(f"In get_new_quote_tweets(): Getting quotes for {len(tweet_ids)} tweets...")

    # Get quotes of each new tweet
    for t_id in tweet_ids:
        quotes, status_code = get_quotes_for_tweet(t_id, bearer_token)

        # If api limit reached -> Abort & return what was fetched so far
        if status_code == 429:
            print(f"Rate limit reached. Waiting for 16m. Paused fetching quote tweets at tweet {t_id}.")
            sleep(16*60)
            quotes, status_code = get_quotes_for_tweet(t_id, bearer_token)

        new_quotes.extend(quotes)

    if new_quotes != []:
        # Save queried data to json as backup
        func_name = str(inspect.currentframe().f_code.co_name + "()")
        tweets_to_json(new_quotes, func_name)

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

    # Save discarded tweets to json (for debugging)
    out_d = read_from_json(discarded_json_path)
    out_d[discarded_key] = discarded
    write_to_json(out_d, discarded_json_path)

    # Append discarded tweets to csv (for keeping track of filters)
    csv_path = discarded_json_path.replace(".json", f"_{discarded_key}.csv")

    if discarded != []:
        include = ["id", "text", "created_at", "username", "author_id"]
        new_data = pd.DataFrame(discarded)[include]

        if exists(csv_path):
            known_data = csv_to_df(csv_path)
            new_data = pd.concat([known_data, new_data]) \
                .drop_duplicates("id") \
                .sort_values("id")
        
        df_to_csv(new_data, csv_path)
        #new_data.to_csv(csv_path, index=False, header=not exists(csv_path))

    return out_tweets

def apply_filters(tweets, filters, discarded_json_path) -> list:
    """
    Takes a list of tweets. Returns the same list with all tweets removed where a regex pattern from
    {filter_patterns} matches in tweet["text"]. Discarded tweets are stored in json file, ordered by
    filter name.
    """

    # Wipe old json file
    write_to_json(dict(), discarded_json_path)

    # Apply regex filters iteratively
    for f in filters:

        tweets = remove_if_regex_matches(
            tweets,
            regex_p=f["pattern"],
            discarded_json_path=discarded_json_path,
            discarded_key=f["name"],
            regex_flag=f["flag"]
        )

    return tweets

def discount_mentions(tweets_dict) -> dict:
    """
    Tweets fetched from the mentions timeline might not mention JediSwap at all, but
    instead "inherit" some or all mentions from tweets higher up in the conversation thread.
    For reply tweets, this method subtracts mentions that have been present in the tweet
    that's been replied to. For replies to JediSwap, the mention is discounted in any case.
    """

    discarded = []
    reply_ids = set()

    def is_reply(tweet_dict) -> bool:
        if "referenced_tweets" in tweet_dict:
            ref_types = {x["type"] for x in tweet_dict["referenced_tweets"]}
            if "replied_to" in ref_types:
                return True
        return False
    
    def is_reply_to_jediswap(tweet_dict) -> bool:
        if "in_reply_to_user_id" in tweet_dict:
            if tweet_dict["in_reply_to_user_id"] == "1470315931142393857":
                return True
        return False

    def is_quote(tweet_dict) -> bool:
        if "referenced_tweets" in tweet_dict:
            ref_types = {x["type"] for x in tweet_dict["referenced_tweets"]}
            if "quoted" in ref_types:
                return True
        return False

    def get_reply_id(tweet_dict) -> str:
        for ref in tweet_dict["referenced_tweets"]:
            if ref["type"] == "replied_to":
                return ref["id"]

    def get_mentions(tweet_dict) -> list:
        """Returns a [potentially empty] list of all usernames mentioned in the tweet."""
        if "entities" in tweet_dict:
            if "mentions" in tweet_dict["entities"]:
                mentions = tweet_dict["entities"]["mentions"]
                usernames = {x["username"] for x in mentions}
                return list(usernames)      
        return []
   
    def remove_leading_mentions_from_text(tweet_dict) -> dict:
        """Takes one tweet at a time & removes all leading mentions from the tweet text."""
        text = tweet_dict["text"]
        no_space_char = None

        while text.startswith("@") and not no_space_char:
            newline_index = text.find("\n")
            space_index = text.find(" ")
            no_space_char = (newline_index == -1) and (space_index == -1)

            if newline_index == -1:
                text = text[space_index+1:]
            elif space_index == -1:
                text = text[newline_index+1:]
            else:
                first_trigger = min(space_index, newline_index)
                text = text[first_trigger+1:]

        tweet_dict["text"] = text
        return tweet_dict

    # Trim all leading mentions from all tweets' text attributes
    out_dict = {k: remove_leading_mentions_from_text(v) for k, v in tweets_dict.items()}      

    # Collect ids of all tweets being replied to
    for t in tweets_dict.values():
        if is_reply(t):
            parent_tweet_id = get_reply_id(t)
            reply_ids.add(parent_tweet_id)
    
    # Query tweet data & create dict for these "parent tweets"
    tweets_list = get_tweets(list(reply_ids), bearer_token)
    parent_tweets = {t["id"]: t for t in tweets_list}
    
    # Discount mentions inherited from other tweets & drop conditionally from data
    for _id, t in tweets_dict.items():
        
        if is_quote(t):
            mentions = get_mentions(t)
            out_dict[_id]["discounted_mentions"] = mentions
            continue

        if is_reply_to_jediswap(t):
            t["comment"] = "Tweet is a reply to a JediSwap tweet."
            discarded.append(t)
            del out_dict[_id]
            continue

        if is_reply(t):
            parent_id = get_reply_id(t)
            mentions = get_mentions(t)
            if parent_id in parent_tweets:
                parent_mentions = get_mentions(parent_tweets[parent_id])
            else:
                parent_mentions = []    # <- tweet is reply to deleted tweet
            
            # Keep only the difference of the mentions sets
            discounted_mentions = list(set(mentions)^set(parent_mentions))
           
            if "JediSwap" not in discounted_mentions:
                t["comment"] = "Inherited JediSwap mention from other tweet in conversation."
                discarded.append(t)
                del out_dict[_id]
                continue
                
            else:
                 out_dict[_id]["discounted_mentions"] = discounted_mentions

        else:
            mentions = get_mentions(t)
            out_dict[_id]["discounted_mentions"] = mentions
            if "JediSwap" not in mentions:
                t["comment"] = "No JediSwap mention found. Not a quote tweet either."
                discarded.append(t)
                del out_dict[_id]
    
    # Append discarded tweets to csv (to keep track of all filtered out tweets)
    csv_path = "not_mentioning_jediswap.csv"

    if discarded != []:
        include = ["id", "text", "comment", "created_at", "username", "author_id", "source"]
        new_data = pd.DataFrame(discarded)[include]

        if exists(csv_path):
            known_data = csv_to_df(csv_path)
            new_data = pd.concat([known_data, new_data]) \
                .drop_duplicates("id") \
                .sort_values("id")
        
        df_to_csv(new_data, csv_path)
        print(f"Sorted out {len(discarded)} tweets not actually mentioning jediswap. See {csv_path}.")

    return out_dict

def get_filtered_tweets(cutoff_ids=None, add_params=None) -> dict:
    """
    Main wrapper function. Calls query functions, applies filtering, returns dictionary
    of filtered tweets. Queries backwards in time. End triggers can be defined in
    {add_params} (all queries) or {cutoff_ids} (per function). Examples:
    cutoff_ids = {"get_new_mentions()": "<tweet id>", "get_new_quotes()":<other tweet id>"}
    add_params = {"start_time" = "2023-03-01T00:00:00.000Z"}
    """
    obvious_print("Fetching new tweets...")

    if cutoff_ids:
        print(f"Querying until tweet ids:")
        [print(f"{k}\t{v}") for k, v in cutoff_ids.items()]
    elif add_params:
        print("Querying using these constraints:")
        [print(f"{k}\t{v}") for k, v in add_params.items()]
    else:
        input("Querying until rate limit reached per function. Continue?")

    new_mentions_params = {}
    new_quotes_params = {}

    # Add query parameters from {add_params} (preferred) or {cutoff_ids}.
    if add_params:
        new_mentions_params.update(add_params)
        new_quotes_params.update(add_params)

    elif cutoff_ids:
        if "get_new_mentions()" in cutoff_ids:
            new_mentions_params["since_id"] = cutoff_ids["get_new_mentions()"]
        if "get_quotes_for_tweet()" in cutoff_ids:
            new_quotes_params["since_id"] = cutoff_ids["get_quotes_for_tweet()"]
    else:
        pass

    # Fetch all mentions new since last execution of script
    new_mentions = get_new_mentions(
        target_user_id,
        bearer_token,
        add_params=new_mentions_params
    )

    # Fetch all mentions new since last execution of script
    new_quotes = get_new_quote_tweets(
        target_user_id,
        bearer_token,
        add_params=new_quotes_params
    )

    # Merge to one list & keep only 1 entry per tweet id
    tweets = merge_unique([new_mentions, new_quotes], unique_att="id")

    # Apply regex filters
    filtered_tweets = apply_filters(tweets, filter_patterns, discarded_path)

    # Convert to dictionary
    out_d = {t["id"]: t for t in filtered_tweets}

    return out_d



if __name__ == "__main__":
    get_filtered_tweets()
