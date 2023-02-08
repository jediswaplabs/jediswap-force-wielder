#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Written by Al Matty - github.com/al-matty
"""

from query_and_filter import get_filtered_tweets
from helpers import obvious_print
from csv_handler import create_csv, save_csv


out_path = './Force_Wielders_Data_beta.csv'

# Fetch new tweets since last execution
obvious_print('Fetching new tweets...')
#new_tweets = get_filtered_tweets()

# Create csv
#obvious_print('Creating csv...')
#df = create_csv(new_tweets)

# Save result locally as csv
#obvious_print('Saving csv...')
#out_df = save_csv(df, out_path, sep=',', sort_by=None)

# Print preview of df
#print(f'Saved {out_path.lstrip("./")}\n')
#print(out_df.head())

print("Yo.")
