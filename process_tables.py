import DatabaseManager, DataCrawler
import logging
import time
from multiprocessing import Pool
i=0

def updateProgress(somevalue):
    global worked
    worked += 1
    dc.printProgressBar(worked, len(entries), s_time)

def processURLTable(entry):
    url_info = dc.getURLInformation(entry)
    dm.updateURLEntry(url_info)

def processUserURLTable(entry):
    url_info = dc.getURLInformation(entry)
    dm.updateUserURLEntry(url_info)

def processUserImagesTable(entry):
    url_info = dc.getUserImageInformation(entry, media_folder)
    dm.updateUserURLEntry(url_info)

def processMediaTable(entry):
    media_info = dc.getMediaInformation(entry, media_folder)
    dm.updateMediaEntry(media_info)

def processICardsTable(entry):
    icard_info = dc.getICardInformation(entry, media_folder)
    dm.updateICardEntry(icard_info)


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

if database_selection == 1:
    tweets_file_name = input("Enter name of database(Default: '%s'):" %(tweets_file_name))

if tweets_file_name == "":
    tweets_file_name = tweets_file_name_original

media_folder = tweets_data_path + tweets_file_name + "/media/"

dc = DataCrawler.DataCrawler()
dm = DatabaseManager.DatabaseManager(tweets_data_path, tweets_file_name, database_selection)

if database_selection == 2:
    media_folder = "PostgreSQL/media/"

table = input("(1) url\n(2) media\n(3) icard\n(4) user url\n(5) user images\n\nselect a table to process (default: 1):")
if table == None: table = 1;
else:
    if table == "":
        table = 1
    table = int(table)

#if table != 1 or table != 2 or table != 3



if table == 1:
    s_time = time.time()
    worked = 0
    entries = dm.getURLEntriesToProcess()
    num_entries = len(entries)
    logger.info("Number of URLs to process: %s", len(entries))

    with Pool(processes=50) as pool:
        for entry in entries:
            pool.apply_async(processURLTable, args=(entry,), callback=updateProgress)

        pool.close()
        pool.join()

if table == 2:

    s_time = time.time()
    worked = 0
    entries = dm.getMediaEntriesPostgreSQL()
    num_entries = len(entries)
    logger.info("Number of Media Entries to process: %s", len(entries))

    with Pool(processes=50) as pool:
        for entry in entries:
            pool.apply_async(processMediaTable, args=(entry,), callback=updateProgress)

        pool.close()
        pool.join()

if table == 3:
    s_time = time.time()
    worked = 0
    entries = dm.getICardEntriesPostreSQL()
    num_entries = len(entries)
    logger.info("Number of ICard Entries to process: %s", len(entries))

    with Pool(processes=10) as pool:
        for entry in entries:
            pool.apply_async(processICardsTable, args=(entry,), callback=updateProgress)

        pool.close()
        pool.join()

if table == 4:
    s_time = time.time()
    worked = 0
    entries = dm.getUserURLEntriesToProcess()
    num_entries = len(entries)
    logger.info("Number of User URLs to process: %s", len(entries))

    with Pool(processes=50) as pool:
        for entry in entries:
            pool.apply_async(processUserURLTable, args=(entry,), callback=updateProgress)

        pool.close()
        pool.join()

if table == 5:
    s_time = time.time()
    worked = 0
    entries = dm.getUserImageEntries()
    num_entries = len(entries)
    logger.info("Number of User Images to process: %s", len(entries))

    with Pool(processes=50) as pool:
        for entry in entries:
            pool.apply_async(processUserImagesTable, args=(entry,), callback=updateProgress)

        pool.close()
        pool.join()