#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Main script intended for regular scheduled execution. Fetches tweets and appends
to database. Can be run daily or several times a week. It will only ever query
until it encounters the last known tweet per category, unless no db exists yet.

Twitter API limitations:
    Lookback range for mentions timeline: 800 tweets
    Lookback range for tweets timeline:   3200 tweets

Written by Al Matty - github.com/al-matty
"""

from sys import exit
from os.path import exists
from query_and_filter import discount_mentions, get_filtered_tweets, get_cutoffs
from helpers import csv_to_df, df_to_csv
from pandas_pipes import *

out_path = "./Force_Wielders_Data_beta.csv"
first_run = not exists(out_path)
add_params = None


def run(add_params=add_params):

    # Get most recent known tweets from dataset if it exists
    query_until_ids = None if (first_run or add_params) else get_cutoffs(out_path)

    # Fetch new tweets since last execution.
    new_tweets = get_filtered_tweets(cutoff_ids=query_until_ids, add_params=add_params)
    if new_tweets == {}:
        print("No new mentions or quote tweets since last execution.")
        exit(0)

    # Drop reply tweets & discount mentions inherited from elsewhere in the conversation
    new_tweets = discount_mentions(new_tweets)

    # Create DataFrame & perform all needed transformations of the data
    in_df = pd.DataFrame.from_dict(new_tweets, orient="index")

    out_df = (in_df.pipe(start_pipeline)
        .pipe(replace_nans)
        .pipe(add_parsed_time)
        .pipe(extract_public_metrics)
        .pipe(add_month)
        .pipe(drop_columns, to_drop)
        .pipe(reorder_columns, final_order)
    )

    # Merge with database / keep only latest fetched version per tweet
    if exists(out_path):

        known_data = csv_to_df(out_path)
        out_df = pd.concat([known_data, out_df]) \
            .sort_values("impression_count") \
            .drop_duplicates("id", keep="last") \
            .sort_values("id")
        known_len = known_data.shape[0]
    
    else:
        known_len = 0
        
    # Save updated database & preserve type information in 2nd row
    df_to_csv(out_df, out_path, mode="w", sep=",")
    n_rows = out_df.shape[0] - known_len
    print(f"Appended {n_rows} tweets to", out_path.lstrip("./"), "\n")
    from query_and_filter import N_TWEETS_QUERIED
    print(f"\nQueried {N_TWEETS_QUERIED} tweets in total.\n")


if __name__ == "__main__":
    run(add_params)