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
from csv_handler import transform, save_csv, create_discarded_csvs
from helpers import obvious_print

out_path = "./Force_Wielders_Data_beta.csv"

# Fetch new tweets since last execution & convert to DataFrame
obvious_print("Fetching new tweets...")
new_tweets = get_filtered_tweets()
in_df = pd.DataFrame.from_dict(new_tweets, orient="index")

# Perform all needed transformations of data
obvious_print("Generating output data...")
out_df = transform(in_df)

# Save result locally as csv
obvious_print("Saving csv...")
save_csv(out_df, out_path, sep=",", sort_by=None)

# Create csv files for dropped tweets
obvious_print("Creating csv files for tweets dropped during filtering stage...")
create_discarded_csvs()


# Print preview of df
print(f"Created {out_path.lstrip('./')}:\n")
print(out_df.head(10))

obvious_print("Done.")
