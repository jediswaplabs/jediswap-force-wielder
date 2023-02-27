#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Main script intended for regular scheduled execution. Fetches tweets and appends
to csv. Can be run daily or several times a week. It will only ever query until
it encounters the last known tweet per category.

Twitter API limitations:
    Lookback range for mentions timeline: 800 tweets
    Lookback range for tweets timeline: 3200 tweets

Written by Al Matty - github.com/al-matty
"""

from os.path import exists
from query_and_filter import get_filtered_tweets, get_cutoffs
from pandas_pipes import *

out_path = "./Force_Wielders_Data_beta.csv"
first_run = not exists(out_path)

# Get most recent known tweets from dataset if it exists
query_until_ids = None if first_run else get_cutoffs(out_path)

# Fetch new tweets since last execution
new_tweets = get_filtered_tweets(cutoff_ids=query_until_ids)

# Create DataFrame & perform all needed transformations of the data
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

# Save/append data locally to csv
out_df.to_csv(out_path, mode="a", sep=",", header=not exists(out_path))
print(f"Appended {in_df.shape[0]} tweets to", out_path.lstrip("./"), "\n")


# TODO: lenster integration?
# TODO: Monthly filtering & updating:
#       -flag / sort out tweets in excess of 5/month
#       -sort out doubles / keep only latest version
#       -or sort out doubles at the beginning of each run? Which one to keep?
#       -update metrics for all rows using pagination (batch-query tweet ids)
