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

from query_and_filter import get_filtered_tweets, get_cutoffs
from helpers import obvious_print, get_max
from pandas_pipes import *

out_path = "./Force_Wielders_Data_beta.csv"
first_run = not os.path.exists(out_path)

# Get most recent known tweets from dataset if it exists
cutoff_timestamps = None if first_run else get_cutoffs(out_path)

# Fetch new tweets since last execution
obvious_print("Fetching new tweets...")
new_tweets = get_filtered_tweets(cutoff_timestamps)

# Perform all needed transformations of data
in_df = pd.DataFrame.from_dict(new_tweets, orient="index")

out_df = (in_df.pipe(start_pipeline)
    .pipe(replace_nans)
    .pipe(add_parsed_time)
    .pipe(rename_columns, to_rename)
    .pipe(extract_public_metrics)
    .pipe(add_month)
    .pipe(drop_columns, to_drop)
    .pipe(reorder, final_order)
)

# Save/append data locally to csv & show preview
df.to_csv(out_path, mode="a", sep=",", index=False, header=not os.path.exists(out_path))

# Show preview of newly appended data
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

# DONE: Infer query cutoff ts's from data if ther is data
# DONE: Handle TooManyRequests Errors (notice of gap + append fetched data anyway)
# DONE: Base case: Query until TooManyRequests error if not specified differently
# TODO: Implement cutoff timestamps arg to get_filtered_tweets(), get_new_mentions() &
#       get_new_tweets_by_user()
# TODO: Test both starting cases for script (with & w/o csv)
# TODO: Append to monthly csv instead of replacing
# TODO: Add & use .env key for first ts string if script run for first time
# TODO: Wrapper for monthly filters + discretizing csv into monthly files
