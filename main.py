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
from pandas_pipeline import *
from helpers import obvious_print

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
out_df.to_csv(out_path, sep=sep, index=False)
print("Created", out_path.lstrip("./"), "\n")
print(out_df.head(10))

# Create csv files for dropped tweets
obvious_print("Creating csv files for tweets dropped during filtering stage...")
create_discarded_csvs()

obvious_print("Done.")
