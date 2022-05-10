from csv_handler import *

# Define csv paths
in_csv = './Force Wielders (Responses) - Form Responses 1.tsv'
out_path = './Force_Wielders_Followers_Retweets.csv'

# Load csv
df = load_csv(in_csv, sep='\t')

# Query Twitter, fill in missing data
df = fill_missing_data(df)

# Save result locally as csv
out_df = save_csv(df, out_path, sort_by='Retweets')

# Print preview of df
print(f'Saved {out_path.lstrip('./')}')
print(out_df.head())
