import logging
import time
import DatabaseManager, DataCrawler
from multiprocessing import Pool
from multiprocessing import freeze_support
import ProcessingTask
if __name__ == '__main__':
    freeze_support()
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

    pt = ProcessingTask.ProcessingTask(dc, dm, num_lines, s_time)
    with Pool(processes=5) as pool:
        for line in tweets_file:
            pt.findTweet(line)
            #pool.apply_async(pt.findTweet, args=(line,), callback=pt.updateProgress)

        pool.close()
        pool.join()


    print('\nNumber of Tweets processed: %s' %worked)