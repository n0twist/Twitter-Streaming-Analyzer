import logging
import json
import time
import DatabaseManager, DataCrawler
from multiprocessing import Pool



def updateProgress(somevalue):
    global worked
    worked += 1
    dc.printProgressBar(worked, num_lines, s_time)

def processTweet(tweet):

    twitter_user = dc.getUserInfo(tweet)
    dm.insertUserRow(twitter_user)

    user_link = dc.getUserLink(tweet)
    if user_link is not None:
        dm.insertUserURLRow(user_link, tweet['id'])

    tweet_info = dc.getTweetInformation(tweet)
    dm.insertInfoRow(tweet_info)

    tweet['urls'] = dc.getTweetURLs(tweet)
    dm.insertURLRow(tweet)

    if 'place' in tweet:
        if tweet['place'] != None:
            place = dc.getPlaceInfo(tweet)
            dm.insertPlaceRow(place)

    tweet['media_content'] = dc.getTweetMedia(tweet)
    dm.insertMediaRow(tweet)

    dm.insertICardRow(tweet)

def findTweet(line):
    try:
        tweet = json.loads(line)
        tweet['extracted'] = False
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

logging.basicConfig(level=logging.INFO)
# create logger with 'spam_application'
logger = logging.getLogger('twitter_app')
logger.setLevel(logging.INFO)

database_selection = input("(1)SQLite\n(2)PostgreSQL\n\nSelect a Database(Default: 1):")
if database_selection == "":
    database_selection = 1
else:
    database_selection = int(database_selection)


tweets_data_path = "data/"
tweets_file_name = "twitter_data_sampled"
tweets_file_name_original = tweets_file_name

tweets_file_name = input("Enter name of tweet file(Default: '%s'):" %(tweets_file_name))

if tweets_file_name == "":
    tweets_file_name = tweets_file_name_original

tweets_file_path = tweets_data_path + tweets_file_name + ".txt"
tweets_file = open(tweets_file_path, "r")
dc = DataCrawler.DataCrawler()
dm = DatabaseManager.DatabaseManager(tweets_data_path, tweets_file_name, database_selection)

logger.info("Check File Size...")
num_lines = sum(1 for line in open(tweets_file_path))
logger.info("Number of Lines in File: %s \n", num_lines)

worked = 0
s_time = time.time()
i = 0
with Pool(processes=1) as pool:
    for line in tweets_file:
        #findTweet(line)
        pool.apply_async(findTweet, args=(line,), callback=updateProgress)

    pool.close()
    pool.join()


print('\nNumber of Tweets processed: %s' %worked)