#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This script is intended to be run manually once a month to generate
the final dataset for whichever month is specified in {month}.
All metrics are updated and the monthly filters are applied.
Tweets to be dropped: -deleted tweets, -tweets from suspended accounts.
"""

from os.path import exists
from pandas_pipes import *
from helpers import csv_to_df, df_to_csv
from main.py import out_path
from query_and_filter import (
    get_tweets,
    apply_filters,
    bearer_token,
    filter_patterns,
    discarded_path
)

month = "February"
assert exists(out_path), f"No database found under {out_path}. Please run main.py first."


# Get tweet ids
data = csv_to_df(data_path)
tweet_ids = data[data["month"] == month]["id"].to_list()
assert len(tweet_ids) == len(set(tweet_ids)), "Some tweets appear more than once in data. Check data."

# Update metrics for all tweets, drop deleted & suspended
tweets = get_tweets(tweet_ids, bearer_token, add_params=None)

# Apply low level regex filters
tweets = apply_filters(tweets, filter_patterns, discarded_path)
tweets_d = {t["id"]: t for t in tweets}

# Apply high level filters
in_df = pd.DataFrame.from_dict(new_tweets, orient="index")

# TODO: pandas pipeline here -> export
