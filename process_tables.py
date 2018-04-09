import DatabaseManager, DataCrawler
import logging
import time

def processURLTable():
    s_time = time.time()
    i = 0
    entries = dm.getURLEntriesToProcess()
    num_entries = len(entries)
    logger.info("Number of URLs to process: %s", num_entries)
    for entry in entries:
        url_info = dc.getURLInformation(entry)
        dm.updateURLEntry(url_info)

        i += 1
        dc.printProgressBar(i, num_entries, s_time)

def processMediaTable():
    s_time = time.time()
    i = 0
    entries = dm.getMediaEntries()
    num_entries = len(entries)
    logger.info("Number of Media Entries to process: %s", num_entries)
    for entry in entries:
        media_info = dc.getMediaInformation(entry, media_folder)
        dm.updateMediaEntry(media_info)

        i += 1
        dc.printProgressBar(i, num_entries, s_time)

def processICardsTable():
    s_time = time.time()
    i = 0
    entries = dm.getICardEntries()
    num_entries = len(entries)
    logger.info("Number of ICard Entries to process: %s", num_entries)
    for entry in entries:
        icard_info = dc.getICardInformation(entry, media_folder)
        dm.updateICardEntry(icard_info)

        i += 1
        dc.printProgressBar(i, num_entries, s_time)


logging.basicConfig(level=logging.INFO)
# create logger with 'spam_application'
logger = logging.getLogger('twitter_app')
logger.setLevel(logging.INFO)

tweets_data_path = "data/"
tweets_file_name = "twitter_data_sampled"
tweets_file_name_original = tweets_file_name
tweets_file_name = input("Enter name of database(Default: '%s'):" %(tweets_file_name))

if tweets_file_name == "":
    tweets_file_name = tweets_file_name_original

media_folder = tweets_data_path + tweets_file_name + "/media/"

dc = DataCrawler.DataCrawler()
dm = DatabaseManager.DatabaseManager(tweets_data_path, tweets_file_name)

table = input("(1) url\n(2) media\n(3) icard\n\nselect a table to process (default: 1):")
if table == None: table = 1;
else:
    if table == "":
        table = 1
    table = int(table)

#if table != 1 or table != 2 or table != 3

if table == 1:
    processURLTable()
if table == 2:
    processMediaTable()
if table == 3:
    processICardsTable()

