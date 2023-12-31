# Jediswap Force Wielder

![GitHub](https://img.shields.io/github/license/jediswaplabs/jediswap-force-wielder)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/release/python-390/)
![Commits](https://img.shields.io/github/commit-activity/w/jediswaplabs/jediswap-force-wielder)
![Libraries.io dependency status for GitHub repo](https://img.shields.io/librariesio/github/jediswaplabs/jediswap-force-wielder)

A script interacting with the Twitter API on a regular basis, fetching, filtering & storing data.
Any tweet that mentions the [JediSwap Twitter account](https://twitter.com/JediSwap) or
quotes a tweet posted by the account will be fetched and passed through a filtering stage.
Any tweets that are not dropped during filtering are stored in monthly csv files with columns
for the attributes mentioned below:

**Data obtained per tweet**

* tweet contents, timestamp, referenced tweets, conversation id
* tweet views, replies, quotes, retweets, likes
* author id, username, followers, following, tweet count, listed count
* if reply: tagged accounts in media of parent tweet (scraped)
* if reply: mentions of parent tweet

Each time the script is run, it searches mentions and quotes backwards through time until it
encounters the most recent tweet from the known data. This is an attempt at the most sparse implementation possible with regards to the total amount of requests, so no tweet should ever be queried for twice. How often the script has to be run to avoid any gaps in the data depends solely on your API tier and the expected activity of your Twitter account & followers. For example, if you are allowed to query for the last 800 mentions using the Twitter mentions timeline, you need to run the script often enough so that there will be less than 800 new mentions since the last time the script fetched data from the API. No harm is done by running the script much more often than necessary to insure against gaps in the data.

**Web scraping**

Some essential information cannot be queried via the Twitter API 2.0, for example the list of users that are tagged in a photo of a tweet. In these cases, the script scrapes the information from the Twitter frontend using [Selenium](https://www.selenium.dev). For this to work, you will have to install the version of [Chromedriver](https://chromedriver.chromium.org) that most closely matches your installed Google Chrome browser. And since the information is only visible to signed in Twitter users, you'll have to create a user data folder as described [here](https://medium.com/web3-use-case/how-to-stay-logged-in-when-using-selenium-in-the-chrome-browser-869854f87fb7) and run the script [Selenium_Twitter_Login.py](https://github.com/jediswaplabs/jediswap-force-wielder/blob/main/Selenium_Twitter_Login.py) once in order to sign into Twitter manually and create a session cookie that the script can then use for the automated scraping. Should it expire, just repeat this step before running the main script.


### Usage

To run, a [Twitter developer account](http://developer.twitter.com/) is needed. Once an
account is registered, paste your API bearer token next to the key `API_BEARER_TOKEN` in
the `.env` file, as shown in [sample.env](https://github.com/jediswaplabs/jediswap-force-wielder/blob/main/sample.env), omitting any quotes. Paste the Twitter user id you want to use the
script for next to the key `TWITTER_USER_ID`, also without any quotes. In [main.py](https://github.com/jediswaplabs/jediswap-force-wielder/blob/main/main.py), set `out_path` to where you want the csv file to be generated.

Run [main.py](https://github.com/jediswaplabs/jediswap-force-wielder/blob/main/main.py) to start the script:

```
python main.py
```


### Configuration

* Query parameters can be customized via `get_query_params()` in [query_and_filter.py](https://github.com/jediswaplabs/jediswap-force-wielder/blob/main/query_and_filter.py). This will
affect each API response and alter the returned response fields uniformly.

* If called directly, the lower-level querying functions in [query_and_filter.py](https://github.com/jediswaplabs/jediswap-force-wielder/blob/main/query_and_filter.py) accept additional query parameters as a dictionary `add_params`, which will be appended to the parameters defined in `get_query_params()`. This way, an API search can be refined or restricted to a specific time interval.

* `filter_patterns` in [query_and_filter.py](https://github.com/jediswaplabs/jediswap-force-wielder/blob/main/query_and_filter.py) can be expanded to drop tweets programmatically. It
uses regex to exclude any tweet where a search pattern matches the tweet contents.

* For more advanced filtering and filtering based on tweet attributes other than `tweet["text"]`, functions can be appended to [pandas_pipes.py](https://github.com/jediswaplabs/jediswap-force-wielder/blob/main/pandas_pipes.py) and added to the pipeline in [main.py](https://github.com/jediswaplabs/jediswap-force-wielder/blob/main/main.py).


### License

This project is licensed under the MIT license. See the [LICENSE](https://github.com/jediswaplabs/jediswap-force-wielder/blob/main/LICENSE) file for details. Collaboration welcome!
