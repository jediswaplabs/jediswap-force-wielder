'''
This file encapsulates file and data handling.
'''

import os
import pandas as pd
from functions_twitter import *





def apply_and_concat(dataframe, field, func, column_names):
    '''
    Helper function. Applies a function returning a tuple to a specified
    input field and adds the result as new columns to the df. The elements
    of the tuple are attached as new columns, labled as spec_ed in column_names.
    '''
    return pd.concat((
        dataframe,
        dataframe[field].apply(
            lambda cell: pd.Series(func(cell), index=column_names))), axis=1)

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
    print('\n\tSorting out doubles...')
    df = df.drop_duplicates(subset='Link')

    # Filter out submissions with invalid links
    print('\n\tFiltering out invalid links...')
    contains_status = df['Link'].str.contains('status')
    contains_twitter = df['Link'].str.contains('twitter.com')
    df = df[contains_status & contains_twitter]

    # Grab Tweet ID out of link and add as new column
    print('\n\tExtracting Tweet ID using regex...')
    df['Tweet ID'] = df['Link'].str.extract('(?<=status/)(\d{19})', expand=True)

    # Grab Twitter handle and add as new column
    print('\n\tExtracting Twitter handle using regex...')
    df['User'] = df['Link'].str.extract(
        '(?<=twitter\.com/)(.+?)(?=/status)', expand=True
        )

    # Rearrange column order
    print('\n\tRearranging column order...')
    rearranged = [
        'User', 'Tweet ID', 'Retweets', 'Views',
        'Status', 'Comments', 'Follower Points',
        'Retweet Points'
        ]
    df = df[rearranged]

    # Add column: tweet type (str)
    print('\n\tDetermining tweet type (original, quote, RT, or nan (suspended or too old))...')
    df['Tweet Tpye'] = df['Tweet ID'].apply(orig_quote_or_rt)

    # Add column: tweet preview
    print('\n\tAdding column: Tweet preview...')
    def get_preview(_id, n):
        return get_text(_id)[:n]
    df['Tweet Preview'] = df['Tweet ID'].apply(
        lambda column_name: get_preview(column_name, 40)
        )

    # Add column: user id
    print('\n\tQuerying for user ID and adding as new column (get_user_id())...')
    df['User ID'] = df['Tweet ID'].apply(get_user_id)

    # Remove suspended users (User ID nan triggered error in next block)
    print('\n\tFiltering out suspended users (User ID = nan)...')
    suspended = df.loc[df['User ID'].isnull()]
    df = df.dropna(subset=['User ID'])
    print('These users have been suspended by Twitter and have been filtered out:\n')
    user_ids_d = suspended[['User', 'User ID']].set_index('User').to_dict()['User ID']
    prettyprint(user_ids_d, 'Twitter Handle', 'User ID')

    # Add column: n_followers per user
    print('\nQuerying for follower count and adding as new column (get_followers_count())...')
    df['Followers'] = df['User ID'].apply(get_followers_count)

    # Add column: number of retweets for submitted tweet
#    print('\nQuerying for retweet count and adding as new column (get_retweet_count())...')
#    df['Retweets'] = df['Tweet ID'].apply(get_retweet_count)
    df.rename(columns = {'Retweets':'RTs_orig'}, inplace = True)

    # Drop duplicates (entries pointing to the same tweet)
    print('\nDropping rows containing the same Tweet ID...')
    df = df.drop_duplicates('Tweet ID')

    # Add columns: retweets, tweet replies, tweet likes, tweet replies
    # Adds (np.nan if tweet no longer exists!)
    new_cols = ['Retweets', 'Replies', 'Likes', 'Quotes']
    df = apply_and_concat(df, 'Tweet ID', get_retr_repl_likes_quotes_count, new_cols)

    # Remove rows pointing to deleted tweets or suspended users (Tweet ID in UNAVAILABLE_TWEETS)
    suspended = df['Tweet ID'].isin(UNAVAILABLE_TWEETS)
#    not_found_by_client = df['Retweets'].isnull()

    suspended_df = df[suspended]
    suspended_users = suspended_df['User'].values.tolist()
    print('''
    These users have been dropped from the data, because
    their jediswap-related tweet is no longer available
    or they have been suspended from Twitter altogether:
    ''')
    [print('\t'+x) for x in suspended_users]
    df = df[~suspended]

    # Convert values of some columns from float to int
    to_convert = ['Retweets', 'Replies', 'Likes', 'Quotes']
    df[to_convert] = df[to_convert].astype(int)

    return df




def save_csv(df, out_path, sort_by='User'):
    '''
    Saves DataFrame with specified column and row order to disk.
    '''
    cols = [
        'Tweet ID', 'User', 'Followers', 'Retweets', 'Replies', 'Likes', 'Quotes',
        'Status', 'Follower Points', 'Retweet Points', 'Tweet Preview', 'Comments'
        ]


    out_df = df[cols]
    out_df.rename(columns={'Tweet Content':'Tweet Preview'}, inplace=True)  # was commented out!
    out_df = out_df.sort_values(sort_by, ascending=False)
    out_df.to_csv(out_path, index=False)
    return out_df
