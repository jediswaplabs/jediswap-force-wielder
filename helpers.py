import os
import json
import pandas as pd


def write_to_json(_dict, path) -> None:
    with open(path, 'w') as jfile:
        json_object = json.dump(_dict, jfile, indent=1, default=str)

def read_from_json(json_path) -> dict:
    with open(json_path, 'r') as jfile:
        data = json.load(jfile)
        return data

def write_list_to_json(_list, path) -> None:
    json_str = json.dumps(_list)
    with open(path, 'w') as jfile:
        json.dump(json_str, jfile)

def read_list_from_json(json_path) -> dict:
    """Reads json, returns list with contents of json file."""
    with open(json_path, 'r') as jfile:
        return json.loads(json.loads(jfile.read()))

def df_to_csv(df, csv_path, **kwargs) -> None:
    """Saves DataFrame to csv & preserves dtypes in 2nd line."""
    df2 = df.copy()

    # Replace index with numerical one
    df2.reset_index(drop=True, inplace=True)

    # Prepend dtypes to top of df
    df2.loc[-1] = df2.dtypes
    df2.index = df2.index + 1
    df2.sort_index(inplace=True)

    # Save to csv
    df2.to_csv(csv_path, index=False, **kwargs)

def csv_to_df(csv_path, **kwargs) -> pd.DataFrame:
    """Reads DataFrame from csv with dtypes preserved in 2nd line."""

    # Read dtypes from 2nd line of csv
    dtypes = {key:value for (key,value) in pd.read_csv(csv_path,
              nrows=1).iloc[0].to_dict().items() if 'date' not in value}

    parse_dates = [key for (key,value) in pd.read_csv(csv_path,
                   nrows=1).iloc[0].to_dict().items() if 'date' in value]

    # Read the rest of the lines with the dtypes from above
    return pd.read_csv(csv_path, dtype=dtypes, parse_dates=parse_dates, skiprows=[1], **kwargs)

def merge_unique(list_of_lists, unique_att="id"):
    """Merge multiple lists of dicts. Keep only one element with {unique_att}"""
    unique_tracker = set()
    out_l = []

    for l in list_of_lists:
        for ele in l:
            if ele[unique_att] not in unique_tracker:
                out_l.append(ele)
                unique_tracker.add(ele[unique_att])
    return out_l

def obvious_print(msg) -> None:
    out_str = '\n' + '='*75 + '\n\t' + msg + '\n' + '='*75 + '\n'
    print(out_str)
