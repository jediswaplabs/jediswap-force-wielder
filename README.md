# jediswap-force-wielder

Some scripts needed to automate the Jediswap community incentive program.
To run, an account at http://developer.twitter.com/ is needed. API and client
credentials go in the .env file. See sample.env for the env variable names.
functions_twitter.py is making extensive use of memoization via global variables
and local json files to avoid excessive querying of the API. No tweet id is
queried twice. Tweet contents and metadata are stored locally before the script
ends and loaded again the next time the script is run.

Takes a tsv file containing twitter links.
Queries Twitter API & Twitter client to
get the following data for each tweet:

- engagement metrics (number of retweets, replies, quotes and likes)
- number of followers per user
- preview of tweet content
- timestamp

Additionally, flags are added to catch:
- users suspended by twitter
- invalid twitter links
- a literal 'red flag' that is triggered by a keyword contained in tweet (can be set)
- duplicate entries pointing to the same tweet (tweet with earliest timestamp is treated as original)
