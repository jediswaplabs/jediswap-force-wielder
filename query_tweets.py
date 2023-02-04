"""
Placeholder file for new functionality until implemented.
"""
import requests
import os
import json

last_mentioned_path = None
jediswap_user_id = "1470315931142393857"
bearer_token = os.environ.get("BEARER_TOKEN")

def get_new_mentions(user_id, last_mentioned_path):
    """
    Query mentions timeline of Twitter user until tweet id from
    {last_mentioned_path} encountered.
    """

    def get_params():
        params = {
            "tweet.fields": "public_metrics",
            "max_results": "100"
        }
        return params

    def bearer_oauth(r):
        """Method required by bearer token authentication."""
        r.headers["Authorization"] = f"Bearer {bearer_token}"
        return r

    def connect_to_endpoint(url, params):
        response = requests.request("GET", url, auth=bearer_oauth, params=params)
        print(response.status_code)
        if response.status_code != 200:
            raise Exception(
                "Request returned an error: {} {}".format(
                    response.status_code, response.text
                )
            )
        return response.json()

    url = "https://api.twitter.com/2/users/{}/mentions".format(user_id)
    params = get_params()
    json_response = connect_to_endpoint(url, params)
    return json.dumps(json_response, indent=4, sort_keys=True)



new_mentions = get_new_mentions(jediswap_user_id, last_mentioned_path)
