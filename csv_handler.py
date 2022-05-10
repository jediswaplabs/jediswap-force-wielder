'''
This file encapsulates file and data handling.
'''
import os
import pandas as pd
from functions_twitter import *

# Define paths
in_csv = './Force Wielders (Responses) - Form Responses 1.tsv'
out_path = './Force_Wielders_Followers_Retweets.csv'


def load_csv(in_csv, sep=','):
    '''
    Takes csv file, returns pandas DataFrame.
    '''
    cols = ['Submit a Link to your tweet, video or article',
            'Timestamp', 'Submission Type', 'Status', 'Followers',
            'Retweets', 'Views', 'Follower Points', 'Retweet Points',
            'Total Points', 'Comments']
    with open(in_csv) as infile:
        data = pd.read_csv(infile, sep='\t', header='infer', usecols=cols)
        data = data.rename(columns = {
        'Submit a Link to your tweet, video or article': 'Link'
        })
    return data


def fill_missing_data(df):
    '''
    Takes DataFrame, queries Twitter & fills in missing data
    as specified.
    '''
    # Load csv
    data = load_csv(in_csv, sep='\t')
    df = data.copy()

    # Sort out doubles
    df = df.drop_duplicates(subset='Link')

    # Filter out submissions with invalid links
    contains_status = df['Link'].str.contains('status')
    contains_twitter = df['Link'].str.contains('twitter.com')
    df = df[contains_status & contains_twitter]

    # Grab Tweet ID out of link and add as new column
    df['Tweet ID'] = df['Link'].str.extract('(?<=status/)(\d{19})', expand=True)

    # Grab Twitter handle and add as new column
    df['User'] = df['Link'].str.extract(
        '(?<=twitter\.com/)(.+?)(?=/status)', expand=True
        )

    # Rearrange column order
    rearranged = [
        'User', 'Tweet ID', 'Retweets', 'Views',
        'Status', 'Comments', 'Follower Points',
        'Retweet Points'
        ]
    df = df[rearranged]

    # Add column: tweet type (str)
    df['Tweet Tpye'] = df['Tweet ID'].apply(orig_quote_or_rt)

    # Add column: tweet preview
    def get_preview(_id, n):
        return get_text(_id)[:n]
    df['Tweet Preview'] = df['Tweet ID'].apply(
        lambda column_name: get_preview(column_name, 40)
        )

    # Add column: user id
    df['User ID'] = df['Tweet ID'].apply(get_user_id)

    # Add column: n_followers per user
    df['Followers'] = df['User ID'].apply(get_followers_count)

    # Add column: number of retweets for submitted tweet
    df['Retweets'] = df['Tweet ID'].apply(get_retweet_count)

    # Drop duplicates (entries pointing to the same tweet)
    df = df.drop_duplicates('Tweet ID')

    return df


def save_csv(df, out_path, sort_by=col):
    '''
    Saves DataFrame with specified column and row order to disk.
    '''
    cols = [
        'Tweet ID', 'User', 'Followers', 'Retweets', 'Status', 'Follower Points',
        'Retweet Points', 'Tweet Preview'
        ]
    out_df = df[cols]
    #out_df.rename(columns={'Tweet Content':'Tweet Preview'}, inplace=True)
    out_df = out_df.sort_values(col, ascending=False)
    out_df.to_csv(out_path, index=False)
    return out_df
