import os
import pandas as pd
from functions_twitter import *

# Load csv
in_csv = './Force Wielders (Responses) - Form Responses 1.tsv'
cols = ['Submit a Link to your tweet, video or article',
        'Timestamp', 'Submission Type', 'Status', 'Followers',
        'Retweets', 'Views', 'Follower Points', 'Retweet Points',
        'Total Points', 'Comments']
with open(in_csv) as infile:
    data = pd.read_csv(infile, sep='\t', header='infer', usecols=cols)
    data = data.rename(columns = {
    'Submit a Link to your tweet, video or article': 'Link'
    })
df = data.copy()

# Sort out doubles
df = df.drop_duplicates(subset='Link')

# Filter out submissions with invalid links
contains_status = df['Link'].str.contains('status')
contains_twitter = df['Link'].str.contains('twitter.com')
df = df[contains_status & contains_twitter]

# Grab Tweet ID out of link and add as new column
df['Tweet ID'] = df['Link'].str.extract('(?<=status/)(\d{19})', expand=True)

# Grab Twitter handle and add as new column, rearrange column order
df['User'] = df['Link'].str.extract(
    '(?<=twitter\.com/)(.+?)(?=/status)', expand=True
    )
rearranged = [
    'User', 'Tweet ID', 'Retweets', 'Views',
    'Status', 'Comments', 'Follower Points',
    'Retweet Points'
    ]
df = df[rearranged]


# Add column: tweet type (str)
df['Tweet Tpye'] = df['Tweet ID'].apply(orig_quote_or_rt)
