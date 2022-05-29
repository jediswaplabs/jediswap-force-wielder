from csv_handler import *

# Define csv paths
in_csv = './Force Wielders (Responses) - Form Responses 1.tsv'
out_path = './Force_Wielders_Data.csv'

# Load csv
obvious_print('Loading csv...')
df = load_csv(in_csv, sep='\t')

# Query Twitter, fill in missing data
obvious_print('Querying Twitter API for missing data...')
df = fill_missing_data(df)

# Save result locally as csv
obvious_print('Saving csv...')
out_df = save_csv(df, out_path, sort_by=None)

# Update json files used for memoization (TWEETS, USERS, SUSPENDED_USERS)
obvious_print('Updating memos...')
update_memos()

# Print preview of df
print(f'Saved {out_path.lstrip("./")}\n')
print(out_df.head())
