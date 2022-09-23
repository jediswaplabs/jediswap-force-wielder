'''
This file encapsulates all handling of the actual data (csv, DataFrame).
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
    print('\n\tExtracting Tweet IDs using regex...')
    try:
        df['Tweet ID'] = df['Submit a Link to your tweet, video or article'].str.extract('(?<=status/)(\d{19})', expand=True)
    except KeyError:
        old_name = 'Submit the tweet/medium article/youtube video/mirror article link'
        new_name = 'Submit a Link to your tweet, video or article'
        df.rename(columns = {old_name: new_name}, inplace = True)
        print(f'Had to change a column name:\nBEFORE: {old_name}\nAFTER: {new_name}.')
        df['Tweet ID'] = df[ 'Submit a Link to your tweet, video or article'].str.extract('(?<=status/)(\d{19})', expand=True)
    print(df.shape)

    # Update engagement metrics memo file
    tweet_ids = [x for x in list(df['Tweet ID'].unique()) if x is not np.nan]
    update_engagement_memo(tweet_ids)

    # Expand truncated tweets contained in TWEETS global variable
    expand_truncated(list(TWEETS.keys()))

    # Update USERS global variable and file (for up-to-date follower count i.e.)
    update_USERS(list(USERS.keys()))

    # Flag Non-Twitter Submissions (where no Tweet ID can be grabbed)
    df['Non-Twitter Submission'] = df['Tweet ID'].apply(check_for_nan)

    # Set 'no content' flag (if submission not from twitter, medium, etc.)
    df['no content'] = df['Submit a Link to your tweet, video or article'].apply(set_no_content_flag)

    # Flag duplicate tweets as duplicates
    reset_UNIVERSAL_MEMO()
    df['Duplicate'] = df['Tweet ID'].apply(flag_as_duplicate)
    reset_UNIVERSAL_MEMO()
    print(df.shape)

    # Set 'Red Flag' trigger if word 'airdrop' contained in tweet text
    df['Red Flag'] = df['Tweet ID'].apply(set_red_flag)

    # Grab Twitter handle and add as new column
    print('\n\tExtracting Twitter handles using regex...')
    df['Twitter Handle'] = df[ 'Submit a Link to your tweet, video or article'].str.extract(
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

    # Add column: Content creation date
    print('\nAdding timestamps of tweet creation...')
    s, args = df['Tweet ID'], {}
    df['Content creation date'] = apply_to_series(s, get_ts_creation_batchwise, **args)
    print(df.shape)

    # Add column: n_followers per user
    print('\nQuerying for follower count and adding as new column (get_followers_count())...')
    df['Followers'] = df['Twitter User ID'].apply(get_followers_count)
    print(df.shape)

    # Add columns: retweets, tweet replies, tweet likes, tweet replies
    # Adds (np.nan if tweet no longer exists!)
    df.rename(columns = {'Retweets':'RTs_orig'}, inplace = True)
    new_cols = ['Retweets', 'Replies', 'Likes', 'Quotes']
    df = apply_and_concat(df, 'Tweet ID', get_retr_repl_likes_quotes_count, new_cols)
    print(df.shape)

    # update (set) flags for suspended Twitter users (or deleted tweets)
    print('Flagging suspended Twitter users based on Tweet IDs...')
    df = update_suspension_flags(df)
    suspended = df.loc[df['Suspended Twitter User'] == True]
    print('These users have been flagged as suspended by Twitter:\n')
    user_ids_d = suspended[['Twitter Handle', 'Twitter User ID']].set_index('Twitter Handle').to_dict()['Twitter User ID']
    prettyprint(user_ids_d, 'Twitter Handle', 'Twitter User ID')
    print(df.shape)

    # Set reply tweet flag ('is reply' True if tweet is reply)
    print('Setting flags for reply tweets...')
    df = set_reply_flags(df)
    print(df.shape)

    # Set mentions flag ('>5 mentions' True if contains more than 2 mentions from different users)
    print('Setting flags for tweets mentioning more than 2 accounts...')
    df = set_mentions_flags(df)
    print(df.shape)

    # Set thread flag ('Follow-up tweet from thread' True if user's tweet is reply to himself)
    print('Setting flags for follow-up tweets inside threads')
    df = set_thread_flags(df)
    print(df.shape)

    # Calculate Twitter points
    print('Calculating Force Wielder points...')
    df['Follower Points'] = df.apply(lambda x: follower_points_formula(
        x['Followers'], x['Duplicate'], x['Non-Twitter Submission']), axis=1)

    df['Retweet Points'] = df.apply(lambda x: retweet_points_formula(
        x['Retweets'], x['Quotes'], x['Duplicate'], x['Non-Twitter Submission']), axis=1)

    df['Total Points'] = df.apply(lambda x: tweet_points_formula(
        x['Followers'], x['Retweets'], x['Quotes'],
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

    # Set flag if more than one link has been submitted
    df['Multiple links submitted'] = df['Submit a Link to your tweet, video or article'].apply(set_multiple_links_flag)

    # Add comment 'Please submit each link as a single entry' if multiple links flag set
    df['Comments'] = ' '
    df['Comments'] = df.apply(add_multiple_links_comment, axis=1)

    # Add '@' to every Twitter handle (except if entry is nan)
    not_nan = df['Twitter Handle'].notnull()
    df.loc[not_nan, 'Twitter Handle'] = ('@' + df.loc[not_nan]['Twitter Handle'])
    df.head()

    # Add column 'Month'
    df['parsed_time'] = pd.to_datetime(df['Timestamp'], infer_datetime_format=True)
    df['Month'] = df['parsed_time'].dt.month_name()

    # Flag every tweet in excess of the 5 tweets with top points per user per month
    # Caution: Replaces 'Total Points' value ' ' with 0
    print('Setting flags for more than 5 tweets per month...')
    df = set_more_than_5_tweets_flag(df)
    print(df.shape)

    # Set flag to determine if tweet is unrelated to JediSwap
    df['contains jediswap'] = df['Tweet ID'].apply(set_contains_jediswap_flag)
    df['quotes jediswap'] = df['Tweet ID'].apply(set_jediswap_quote_flag)
    df['Unrelated to JediSwap'] = df.apply(check_if_unrelated, axis=1)

    # Set flag for Twitter submissions not linking to a tweet
    df['Not a tweet'] = df.apply(set_not_a_tweet_flag, axis=1)

    # Set flag categorizing the type of submitted content
    df['Submission Type'] = df.apply(set_submission_type, axis=1)




    # No points for duplicate entries, [invalid links], suspended users, or multiple links submitted
    df['Follower Points'] = df.apply(correct_follower_p, axis=1)
    df['Retweet Points'] = df.apply(correct_retweet_p, axis=1)
    df['Total Points'] = df.apply(correct_total_p, axis=1)

    # Add explaining comment if a flag has been triggered and points denied
    print('Adding explanatory comment wherever points have been denied...')
    df['Comments'] = df.apply(add_points_denied_comment, axis=1)
    print(df.shape)

    return df

def save_csv(df, out_path, sep=',', sort_by=None):
    '''
    Saves DataFrame with specified column and row order to disk.
    '''
    # Determine included columns & column order
    cols = [
        'Timestamp', 'Submit a Link to your tweet, video or article',
        'Choose your verification option', 'Provide your Twitter handle(username)',
        'Wallet', 'Followers', 'Retweets',
        'Replies', 'Likes', 'Quotes', 'Follower Points', 'Retweet Points',
        'Total Points','Twitter Handle', 'Tweet ID', 'Twitter User ID', 'Duplicate',
        'Non-Twitter Submission', 'Suspended Twitter User', 'Tweet is reply',
        '>5 mentions', 'Follow-up tweet from thread', 'Tweet #6 or higher per month',
        'Unrelated to JediSwap', 'Red Flag', 'Tweet Preview', 'Month', 'Content creation date',
        'Submission Type', 'Comments'
    ]
    out_df = df[cols]

    # Rename column
    out_df.rename(columns = {'Suspended Twitter User':'User suspended or tweet deleted'}, inplace = True)

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
    out_df.to_csv(out_path, sep=sep, index=False)
    print('Csv saved as', out_path)

    return out_df
