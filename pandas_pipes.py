"""
All pandas-related functions are defined here.
"""

import pandas as pd
import datetime as dt

to_rename = {}    # {"old_name": "new_name", ...}
to_drop = ["edit_history_tweet_ids", "public_metrics"]
final_order = [
    "month",
    "parsed_time",
    "id",
    "conversation_id",
    "impression_count",
    "reply_count",
    "retweet_count",
    "like_count",
    "quote_count",
    "text",
    "in_reply_to_user_id",
    "created_at",
    "source",
    "username",
    "author_id",
    "followers_count",
    "following_count",
    "tweet_count",
    "listed_count",
    "referenced_tweets",
]


def start_pipeline(df) -> pd.DataFrame:
    """Copy df for inplace operations to work as expected."""
    return df.copy()

def replace_nans(df) -> pd.DataFrame:
    """Replace any nan with False (bool)."""
    df.fillna(value=False, inplace=True)
    return df

def rename_columns(df, old_new_dict) -> pd.DataFrame:
    df.rename(columns=old_new_dict, inplace=True)
    return df

def sort_rows(df, column) -> pd.DataFrame:
    df.sort_values(column, inplace=True)
    return df

def reorder_columns(df, columns) -> pd.DataFrame:
    return df[columns]

def drop_columns(df, col_list) -> pd.DataFrame:
    """Drop all columns specified in {col_list}."""
    df.drop(columns=col_list, axis=1, inplace=True)
    return df

def add_parsed_time(df) -> pd.DataFrame:
    df['parsed_time'] = pd.to_datetime(df['created_at'], infer_datetime_format=True)
    return df

def add_month(df) -> pd.DataFrame:
    df['month'] = df['parsed_time'].dt.month_name()
    return df

def keep_five_per_author(df) -> pd.DataFrame:
    """Keep only the 5 highest impression tweets per Twitter user."""

    df.sort_values("impression_count", ascending=False, inplace=True)
    df["Handle Counter"] = df.groupby('username').cumcount()+1
    df.drop(df[df["Handle Counter"] > 5].index, inplace=True)
    del df['Handle Counter']

    return df

def apply_and_concat(dataframe, field, func, column_names) -> pd.DataFrame:
    """
    Helper function. Applies a function returning a tuple to a specified
    input field and adds the result as new columns to the df. The elements
    of the tuple are attached as new columns, labled as spec_ed in column_names.
    """
    return pd.concat((
        dataframe,
        dataframe[field].apply(
            lambda cell: pd.Series(func(cell), index=column_names))), axis=1)

def extract_public_metrics(df) -> pd.DataFrame:
    """Adds specific dictionary entries as new columns."""
    def extract_from_dict(dict_field) -> tuple:
        d = dict_field
        return (
            d["impression_count"],
            d["reply_count"],
            d["retweet_count"],
            d["like_count"],
            d["quote_count"]
        )
    new_cols = ["impression_count", "reply_count", "retweet_count", "like_count", "quote_count"]
    df = apply_and_concat(df, 'public_metrics', extract_from_dict, new_cols)
    return df
