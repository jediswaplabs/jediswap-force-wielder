#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script intended to be run manually once a month to generate
the final dataset for whichever month is specified in {month}.
When run, all Twitter metrics are updated and the monthly filters are applied.
Deleted tweets & tweets from suspended accounts are being dropped at this stage.
"""

from os.path import exists
from pandas_pipes import *
from helpers import csv_to_df, df_to_csv
from main import out_path as db_path
from query_and_filter import (
    get_tweets,
    apply_filters,
    discount_mentions,
    bearer_token,
    filter_patterns,
    discarded_path,
)

month = "December"
out_path = f"./{month} Tweet Data.csv"
assert exists(db_path), f"No database found in {db_path}. Please run main.py first."

# Get tweet ids
data = csv_to_df(db_path)
tweet_ids = data[data["month"] == month]["id"].to_list()
assert len(tweet_ids) == len(set(tweet_ids)), "Some tweets appear more than once in dataset. Check data."

# Query metrics for all tweets as of today, drop deleted & suspended tweets
tweets = get_tweets(tweet_ids, bearer_token, add_params=None)

# Apply filters
tweets = apply_filters(tweets, filter_patterns, discarded_path)
tweets_d = {t["id"]: t for t in tweets}
tweets_d = discount_mentions(tweets_d)
in_df = pd.DataFrame.from_dict(tweets_d, orient="index")

# Define output format & data to be ignored
to_drop.extend(["created_at", "source"])
to_drop = list(set(to_drop))
monthly_order = [
    'month', 'parsed_time', 'id', 'conversation_id', 'author_id', 'user', 'points',
    'followers_per_retweets', 'n mentions', 'mentions', '>5 mentions', 'truncated_text',
    'impression_count', 'reply_count', 'retweet_count', 'like_count', 'quote_count',
    'followers_count', 'following_count', 'tweet_count', 'listed_count', 'referenced_tweets',
    'text', 'in_reply_to_user_id'
]

# Reshape data
out_df = (in_df.pipe(start_pipeline)
    .pipe(replace_nans)
    .pipe(add_parsed_time)
    .pipe(extract_public_metrics)
    .pipe(add_followers_per_retweets)
    .pipe(add_month)
    .pipe(add_more_than_5_mentions_flag)
    .pipe(add_truncated_text_flag)
    .pipe(add_n_mentions)
    .pipe(assign_points)
    .pipe(keep_five_per_author)
    .pipe(sort_rows, "id")
    .pipe(rename_columns, to_rename)
    .pipe(reorder_columns, monthly_order)
    .pipe(drop_columns, to_drop)
)

# Save final dataset & preserve type information in 2nd row
df_to_csv(out_df, out_path, mode="w", sep=",")
print(f"Stored monthly data ({out_df.shape[0]} tweets) as", out_path.lstrip("./"), "\n")
