# Jediswap Force Wielder

Some scripts needed to automate the Jediswap community incentive program.
Takes a tsv file containing twitter links. Queries Twitter API & Twitter
client to get tweet contents, tweet metadata and user information. Saves
results locally as a new csv. No rows are dropped. A comment is added
whenever points are denied due to a flag.

To run, an account at http://developer.twitter.com/ is needed. API and client
credentials go in the `.env` file. See sample.env for the env variable names.
`functions_twitter.py` is making extensive use of memoization via global variables
and local json files to avoid excessive querying of the API. No tweet id is
queried twice. Queried tweet contents and metadata are stored locally before the
script ends and loaded again the next time the script is run.

### For each tweet, the following data is obtained:
- tweet author's twitter handle & user ID
- engagement metrics (number of retweets, replies, quotes and likes)
- number of followers of tweet author
- preview of tweet contents
- tweet timestamp & dedicated month column

### Additionally, flags are added to catch:
- invalid submissions (i.e. multiple links)
- users suspended by twitter
- links unrelated to whitelisted content platforms
- tweets unrelated to JediSwap
- a literal 'red flag' that is triggered by a keyword contained in tweet (can be set)
- duplicate entries pointing to the same tweet (tweet with earliest timestamp is treated as original)
- tweets containing 3 or more mentions
- follow-up tweets from within a thread
- tweets from an author with 5 prior tweets already for the same month

### Usage

Enter the Twitter API & Twitter Client Authentication Details into `.env` to their respective keys, as shown in [sample.env](https://github.com/jediswaplabs/jediswap-force-wielder/blob/main/sample.env).

Run `main.py` to start the script!
```
python main.py
```

### License

This project is licensed under the MIT license. See the [LICENSE](https://github.com/jediswaplabs/jediswap-force-wielder/blob/main/LICENSE) file for details.
