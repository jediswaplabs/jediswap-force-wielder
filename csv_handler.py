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
