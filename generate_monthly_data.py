#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script intended to be run manually once a month to generate
the final dataset for whichever month is specified in {month}.
All metrics are updated and monthly filters are applied.
Tweets to be dropped: -deleted tweets, -tweets from suspended accounts.
"""

from os.path import exists
from pandas_pipes import *
from helpers import csv_to_df, df_to_csv
from main import out_path as db_path
from query_and_filter import (
    get_tweets,
    apply_filters,
    bearer_token,
    filter_patterns,
    discarded_path
)

month = "March"
out_path = f"./{month} Tweet Data.csv"
assert exists(db_path), f"No database found under {db_path}. Please run main.py first."

# Get tweet ids
data = csv_to_df(db_path)
tweet_ids = data[data["month"] == month]["id"].to_list()
assert len(tweet_ids) == len(set(tweet_ids)), "Some tweets appear more than once in dataset. Check data."

# Query metrics for all tweets as of today, drop deleted & suspended tweets
tweets = get_tweets(tweet_ids, bearer_token, add_params=None)

# Apply filters
tweets = apply_filters(tweets, filter_patterns, discarded_path)
tweets_d = {t["id"]: t for t in tweets}
in_df = pd.DataFrame.from_dict(tweets_d, orient="index")

# Define output format & data to be ignored
to_drop.extend(["created_at", "source"])
to_drop = list(set(to_drop))
monthly_order = [
    'month', 'parsed_time', 'id', 'conversation_id', 'author_id', 'user', 'points',
    'followers_per_retweets', '>5 mentions', 'impression_count', 'reply_count',
    'retweet_count', 'like_count', 'quote_count', 'followers_count', 'following_count',
    'tweet_count', 'listed_count', 'referenced_tweets', 'text', 'in_reply_to_user_id'
]

# Reshape data
out_df = (in_df.pipe(start_pipeline)
    .pipe(replace_nans)
    .pipe(add_parsed_time)
    .pipe(extract_public_metrics)
    .pipe(add_followers_per_retweets)
    .pipe(add_month)
    .pipe(add_more_than_5_mentions_flag)
    .pipe(assign_points)
    .pipe(keep_five_per_author)
    .pipe(add_prefix, "username", "Twitter, ")
    .pipe(sort_rows, "id")
    .pipe(rename_columns, to_rename)
    .pipe(reorder_columns, monthly_order)
    .pipe(drop_columns, to_drop)
)

# Save final dataset & preserve type information in 2nd row
df_to_csv(out_df, out_path, mode="w", sep=",")
print(f"Stored monthly data ({out_df.shape[0]} tweets) as", out_path.lstrip("./"), "\n")