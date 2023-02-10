# Jediswap Force Wielder

![GitHub](https://img.shields.io/github/license/jediswaplabs/jediswap-force-wielder)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/release/python-390/)


A script interacting with the Twitter API on a regular basis, fetching, filtering & storing data.
Any tweet that mentions the [JediSwap Twitter account](https://twitter.com/JediSwap) or
quotes a tweet posted by the account will be fetched and passed through a filtering stage.
Any tweets that are not dropped during filtering are stored in monthly csv files with columns
for the attributes mentioned below:

**Data obtained per tweet**

* tweet contents, timestamp, referenced tweets, conversation id
* tweet views, replies, quotes, retweets, likes
* author id, username, followers, following, tweet count, listed count

Each time the script is run, it searches mentions and quotes backwards through time until it
encounters the most recent tweet it fetched during the last execution and stops there.
These cut-off points are stored in `last_queried.json`. In case of errors during the
execution of the script, they can always be recreated from `last_queriedBAK.txt`.

This is an attempt at the most sparse implementation possible with regards to the total
amount of requests, so no tweet should ever be queried for twice. How often the script has to
be run to avoid any gaps in the data depends solely on your API tier and the expected activity
of your Twitter account & followers. For example, if you are allowed to query for the last 800 mentions using the Twitter mentions timeline, you need to run the script often enough so that there will be less than 800 new mentions since the last time the script fetched data from the API. No harm is done by running the script much more often than necessary to ensure not missing
out on data.


### Usage

To run, a [Twitter developer account](http://developer.twitter.com/) is needed. Once an
account is registered, paste your API bearer token next to the key `API_BEARER_TOKEN` in
the `.env` file, as shown in [sample.env](https://github.com/jediswaplabs/jediswap-force-wielder/blob/main/sample.env), omitting any quotes. Paste the Twitter user id you want to use the
script for next to the key `TWITTER_USER_ID`, also without any quotes. In [main.py](https://github.com/jediswaplabs/jediswap-force-wielder/blob/main/main.py), set `out_path` to where you want the csv file to be generated. If run for the very first time, you will also need to
enter two tweet ids as first cut-off points for the queries. Add this information to [last_queried.json](https://github.com/jediswaplabs/jediswap-force-wielder/blob/main/last_queried.json):

```
"id_of_last_tweet": "<tweet id of some recent tweet by yout Twitter account>"
"id_of_last_mention": "<tweet id of some recent tweet mentioning a tweet by your Twitter account>"
```

These will define the starting points for your database. Only tweets younger than these will ever be fetched. They will be updated after every execution.

Run [main.py](https://github.com/jediswaplabs/jediswap-force-wielder/blob/main/main.py) to start the script:

```
python main.py
```


### Configuration

* Query parameters can be customized via `get_query_params()` in [query_and_filter.py](https://github.com/jediswaplabs/jediswap-force-wielder/blob/main/query_and_filter.py). This will
affect each API response and alter the returned response fields.

* If called directly, the main function `get_filtered_tweets()` in [query_and_filter.py](https://github.com/jediswaplabs/jediswap-force-wielder/blob/main/query_and_filter.py) accepts additional
query parameters as a dictionary `add_params`, which will be appended to the parameters defined in `get_query_params()`. This way, an API search can be restricted to a specific time interval
for example.

* `filter_patterns` in [query_and_filter.py](https://github.com/jediswaplabs/jediswap-force-wielder/blob/main/query_and_filter.py) can be expanded to drop tweets programmatically. It
uses regex to exclude any tweet where a search pattern matches the tweet contents.

* For more advanced filtering and filtering based on tweet attributes other than `tweet["text"]`, functions can be appended to [pandas_pipes.py](https://github.com/jediswaplabs/jediswap-force-wielder/blob/main/pandas_pipes.py) and added to the pipeline in [main.py](https://github.com/jediswaplabs/jediswap-force-wielder/blob/main/main.py).


### License

This project is licensed under the MIT license. See the [LICENSE](https://github.com/jediswaplabs/jediswap-force-wielder/blob/main/LICENSE) file for details. Collaboration welcome!
