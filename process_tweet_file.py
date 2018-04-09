import logging
import json
import time
import DatabaseManager, DataCrawler


def processTweet(tweet):

    twitter_user = dc.getUserInfo(tweet)
    dm.insertUserRow(twitter_user)

    tweet_info = dc.getTweetInformation(tweet)
    dm.insertInfoRow(tweet_info)

    tweet['urls'] = dc.getTweetURLs(tweet)
    dm.insertURLRow(tweet)

    tweet['media_content'] = dc.getTweetMedia(tweet)
    dm.insertMediaRow(tweet)

    dm.insertICardRow(tweet)


logging.basicConfig(level=logging.INFO)
# create logger with 'spam_application'
logger = logging.getLogger('twitter_app')
logger.setLevel(logging.INFO)

tweets_data_path = "data/"
tweets_file_name = "twitter_data_sampled"
tweets_file_name_original = tweets_file_name
tweets_file_name = input("Enter name of tweet file(Default: '%s'):" %(tweets_file_name))

if tweets_file_name == "":
    tweets_file_name = tweets_file_name_original

tweets_file_path = tweets_data_path + tweets_file_name + ".txt"
tweets_file = open(tweets_file_path, "r")
dc = DataCrawler.DataCrawler()
dm = DatabaseManager.DatabaseManager(tweets_data_path, tweets_file_name)

logger.info("Check File Size...")
num_lines = sum(1 for line in open(tweets_file_path))
logger.info("Number of Lines in File: %s \n", num_lines)

s_time = time.time()
i = 0
for line in tweets_file:
    try:
        tweet = json.loads(line)
        if 'retweeted_status' in tweet:
            original_tweet = tweet['retweeted_status']
            original_tweet = dc.setTypeFlags(original_tweet)
            original_tweet = dc.resolveTruncated(original_tweet)
            processTweet(original_tweet)

            if 'quoted_status' in original_tweet:
                quoted_tweet = original_tweet['quoted_status']
                quoted_tweet = dc.setTypeFlags(quoted_tweet)
                quoted_tweet = dc.resolveTruncated(quoted_tweet)
                processTweet(quoted_tweet)

            if 'quoted_status' in tweet:
                quoted_tweet = tweet['quoted_status']
                quoted_tweet = dc.setTypeFlags(quoted_tweet)
                quoted_tweet = dc.resolveTruncated(quoted_tweet)
                processTweet(quoted_tweet)

        else:
            if 'quoted_status' in tweet:
                quoted_tweet = tweet['quoted_status']
                quoted_tweet = dc.setTypeFlags(quoted_tweet)
                quoted_tweet = dc.resolveTruncated(quoted_tweet)
                processTweet(quoted_tweet)

        if 'id' in tweet:
            tweet = dc.setTypeFlags(tweet)
            tweet = dc.resolveTruncated(tweet)
            processTweet(tweet)
        else:
            logger.error("This line is corrupted: %s", tweet)

    except:
        logger.error("Could not Read Line: ", line)
        continue

    i += 1
    dc.printProgressBar(i, num_lines, s_time)

