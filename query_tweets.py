"""
Placeholder file for new functionality until implemented.
"""
import requests
import os
import json

last_mentioned_path = None
jediswap_user_id = "1470315931142393857"
bearer_token = os.environ.get("BEARER_TOKEN")

new_jediswap_tweets = [] # new tweets by official JediSwap will be appended here
new_mentions = []        # new tweets mentioning JediSwap will be appended here
new_quotes = []          # new tweets quoting JediSwap tweets will be appended here

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

def get_new_mentions(user_id, last_mentioned_path, bearer_token):
    """
    Query mentions timeline of Twitter user until tweet id from
    {last_mentioned_path} encountered. Replace last
    """
    new_mentions = []
    url = "https://api.twitter.com/2/users/{}/mentions".format(user_id)
    params = {
        "tweet.fields": "public_metrics",
        "max_results": "100"
    }
    json_response = connect_to_endpoint(url, params, bearer_token)

    # TODO: Manually set first last_mentioned in json to first tweet mentioning JediSwap in Feb 2023
    # Get last_mentioned (tweet id) from json
    # Iterate over tweet ids, query next 100 if last_mentioned not in results
    # Do until last_mentioned found. Then slice tweet id list at last_mentioned & return

    return json.dumps(json_response, indent=4, sort_keys=True)

    # return new_mentions


new_mentions = get_new_mentions(jediswap_user_id, last_mentioned_path, bearer_token)
