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

def get_cols(in_csv, sep=','):
    '''
    Takes csv file, returns pandas DataFrame.
    '''
    cols = ['Submit a Link to your tweet, video or article',
            'Timestamp', 'Submission Type', 'Status', 'Followers',
            'Retweets', 'Views', 'Follower Points', 'Retweet Points',
            'Total Points', 'Comments']
    with open(in_csv) as infile:
        data = pd.read_csv(infile, sep='\t', header='infer')
    return list(data.columns)

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

def load_csv(in_csv, sep=','):
    '''
    Takes csv file, returns pandas DataFrame.
    '''
    with open(in_csv) as infile:
        data = pd.read_csv(infile, sep='\t', header='infer')
    return data

def fill_missing_data(df):
    '''
    Takes DataFrame, queries Twitter & fills in missing data
    as specified.
    '''
    # Load csv
    data = load_csv(in_csv, sep='\t')
    df = data.copy()
    print(df.shape)

    # Grab Tweet ID out of link and add as new column
    print('\n\tExtracting Tweet ID using regex...')
    df['Tweet ID'] = df['Link'].str.extract('(?<=status/)(\d{19})', expand=True)
    print(df.shape)

    # Flag Non-Twitter Submissions (where no Tweet ID can be grabbed)
    df['Non-Twitter Submission'] = df['Tweet ID'].apply(check_for_nan)

    # Flag duplicate tweets as duplicates
    reset_UNIVERSAL_MEMO()
    df['Duplicate'] = df['Tweet ID'].apply(flag_as_duplicate)
    reset_UNIVERSAL_MEMO()
    print(df.shape)

    # Set 'Red Flag' trigger if word 'airdrop' contained in tweet text
    df['Red Flag'] = df['Tweet ID'].apply(set_red_flag)

    # Grab Twitter handle and add as new column
    print('\n\tExtracting Twitter handle using regex...')
    df['Twitter Handle'] = df['Link'].str.extract(
        '(?<=twitter\.com/)(.+?)(?=/status)', expand=True
        )
    print(df.shape)

    # Add column: user id
    print('\n\tQuerying for user ID and adding as new column (get_user_id())...')
    df['Twitter User ID'] = df['Tweet ID'].apply(get_user_id)
    print(df.shape)

    # Add column: tweet preview
    print('\n\tAdding column: Tweet preview...')
    def get_preview(_id, n):
        return get_text(_id)[:n]
    df['Tweet Preview'] = df['Tweet ID'].apply(
        lambda column_name: get_preview(column_name, 40)
        )
    print(df.shape)

    # Rearrange column order
#    print('\n\tRearranging column order...')
#    rearranged = [
#        'Twitter Handle', 'Link', 'Tweet ID', 'Retweets', 'Views', 'Duplicate', 'Non-Twitter Submission',
#        'Red Flag', 'Status', 'Comments', 'Tweet Preview', 'Twitter User ID'
#        ]
#    df = df[rearranged]
#    print(df.shape)

    # Add column: n_followers per user
    print('\nQuerying for follower count and adding as new column (get_followers_count())...')
    print(df.head(2))
    df['Followers'] = df['Twitter User ID'].apply(get_followers_count)
    print(df.shape)

    # Add columns: retweets, tweet replies, tweet likes, tweet replies
    # Adds (np.nan if tweet no longer exists!)
    df.rename(columns = {'Retweets':'RTs_orig'}, inplace = True)
    new_cols = ['Retweets', 'Replies', 'Likes', 'Quotes']
    df = apply_and_concat(df, 'Tweet ID', get_retr_repl_likes_quotes_count, new_cols)
    print(df.shape)

#    # Remove rows pointing to deleted tweets or suspended users (Tweet ID in UNAVAILABLE_TWEETS)
#    suspended = df['Tweet ID'].isin(UNAVAILABLE_TWEETS)
#    suspended_df = df[suspended]
#    suspended_users = suspended_df['Twitter Handle'].values.tolist()
#    print('''
#    These users have been flagged as suspended, because
#    their jediswap-related tweet is no longer available
#    or they have been suspended from Twitter altogether:
#    ''')
#    [print('\t'+x) for x in suspended_users]
#    df = df[~suspended]
#    print(df.shape)

    # Flag tweets from suspended users
    print('Flagging suspended Twitter users based on Tweet IDs...')
    df['Suspended Twitter User'] = df['Tweet ID'].apply(flag_as_suspended)
    suspended = df.loc[df['Suspended Twitter User'] == True]
    print('These users have been flagged as suspended by Twitter:\n')
    user_ids_d = suspended[['Twitter Handle', 'Twitter User ID']].set_index('Twitter Handle').to_dict()['Twitter User ID']
    prettyprint(user_ids_d, 'Twitter Handle', 'Twitter User ID')
    print(df.shape)

    # Calculate Twitter points
    print('Calculating Force Wielder points...')
    df['Follower Points'] = df.apply(lambda x: follower_points_formula(
        x['Followers'], x['Duplicate'], x['Non-Twitter Submission']), axis=1)

    df['Retweet Points'] = df.apply(lambda x: retweet_points_formula(
        x['Retweets'], x['Duplicate'], x['Non-Twitter Submission']), axis=1)

    df['Total Points'] = df.apply(lambda x: tweet_points_formula(
        x['Followers'], x['Retweets'],
        x['Duplicate'], x['Non-Twitter Submission']
        ), axis=1)

    # Convert values of some columns from float to int (and nan or str to ' ')
    print('Converting some columns to int')
    to_convert = ['Retweets', 'Replies', 'Likes', 'Quotes', 'Follower Points',
        'Retweet Points', 'Total Points', 'Followers']
    for col in to_convert:
        df[col] = df[col].apply(safe_to_int)
    print(df.shape)

    # Delete tweet preview for invalid links and suspended users
    df['Tweet Preview'] = df.apply(row_handler, axis=1)

    # No points for duplicate entries, [invalid links] and suspended users
    df['Follower Points'] = df.apply(correct_follower_p, axis=1)
    df['Retweet Points'] = df.apply(correct_retweet_p, axis=1)
    df['Total Points'] = df.apply(correct_total_p, axis=1)
    # correct_total_p() is ignoring non-twitter submissions

    return df

def save_csv(df, out_path, sort_by=None):
    '''
    Saves DataFrame with specified column and row order to disk.
    '''
    # Determine which columns to include
#    cols = [
#        'Tweet ID', 'Twitter Handle', 'Followers', 'Retweets', 'Replies', 'Likes', 'Quotes',
#        'Follower Points', 'Retweet Points', 'Total Points',
#        'Duplicate', 'Non-Twitter Submission', 'Suspended Twitter User',
#        'Red Flag', 'Tweet Preview', 'Status', 'Comments', 'Link'
#        ]
    out_df = df[cols]

    # Replace ' ' with nan temporarily to avoid error while sorting
    def switch_nan(val):
        if val == ' ':
            return np.nan
        if np.isnan(float(val)):
            return ' '
        else:
            return val

    # Sort data for final arrangement
    if sort_by:
        out_df[sort_by] = out_df[sort_by].apply(switch_nan)
        out_df = out_df.sort_values(sort_by, ascending=False)
        out_df[sort_by] = out_df[sort_by].apply(safe_to_int)

    # Save to csv
    out_df.to_csv(out_path, index=False)
    print('Csv saved as', out_path)

    return out_df
