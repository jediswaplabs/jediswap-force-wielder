# jediswap-force-wielder
Some scripts needed to automate the Jediswap community incentive program.
To run, an account at http://developer.twitter.com/ is needed. API and client
credentials go in .env file. See sample.env for env variable names.


Takes a tsv file containing twitter links. Queries Twitter API & Twitter client to
get the following data for each tweet:

- engagement metrics (number of retweets, replies, quotes and likes)
- number of followers per user
- preview of tweet content
- timestamp

Flags are added to catch:
- users suspended by twitter
- invalid twitter links
- a literal 'red flag' that is triggered by a keyword contained in tweet (can be set)
- duplicate entries pointing to the same tweet (tweet with earliest timestamp is treated as original)
