#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Main script intended for regular scheduled execution. Can be run
daily or several times a week. It will only ever query until it
encounters the last known tweet per category.

Twitter API limitations:
    Lookback range for mentions timeline: 800 tweets
    Lookback range for tweets timeline: 3200 tweets

Written by Al Matty - github.com/al-matty
"""

from query_and_filter import get_filtered_tweets
from helpers import obvious_print
from pandas_pipes import *

out_path = "./Force_Wielders_Data_beta.csv"

# Fetch new tweets since last execution & convert to DataFrame
obvious_print("Fetching new tweets...")
new_tweets = get_filtered_tweets()
in_df = pd.DataFrame.from_dict(new_tweets, orient="index")

# Perform all needed transformations of data
out_df = (in_df.pipe(start_pipeline)
    .pipe(replace_nans)
    .pipe(add_parsed_time)
    .pipe(rename_columns, to_rename)
    .pipe(extract_public_metrics)
    .pipe(add_month)
    .pipe(drop_columns, to_drop)
    .pipe(reorder, final_order)
)

# Save data locally as csv & show preview
out_df.to_csv(out_path, sep=",", index=False)
print("Created", out_path.lstrip("./"), "\n")
print(out_df.head(10))



# DONE: Implement querying based on mentions of JediSwap account
# DONE: Implement querying based on quote tweets of tweets of JediSwap account
# DONE: Abstract away repetetive code
# DONE: Check which tweet attributes are needed, include "expansions" object while querying
# DONE: Filter out retweets using t["text"].startswith("RT") right after querying
# DONE: Filter out tweets with too many mentions right after querying using regex
# DONE: Merge tweet lists and discard doubles based on tweet id
# DONE: Rewrite main script to work with the new input data
# DONE: Add last missing filters
# DONE: Sanity check on filters

# TODO: Append to monthly csv instead of replacing
# TODO: Handle TooManyRequests Errors (notice of gap + append fetched data anyway)
# TODO: Infer query cutoff ts's from data & create empty csv if no data yet (for appending)
# TODO: Add & use .env key for first ts string if script run for first time
# TODO: Wrapper for monthly filters + discretizing csv into monthly files
