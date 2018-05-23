import DatabaseManager, DataCrawler
import logging
import time
import ProcessingTableTask
from multiprocessing import Pool
i=0
urls = []


def updateProgress(somevalue):
    global worked
    worked += 1
    dc.printProgressBar(worked, len(entries), s_time)

def processURLTable(entry):
    return dc.getURLInformation(entry)

def processUserURLTable(entry):
    return dc.getURLInformation(entry)

def processUserImagesTable(entry):
    return dc.getUserImageInformation(entry, media_folder)

def processMediaTable(entry):
    return dc.getMediaInformation(entry, media_folder)

def processICardsTable(entry):
    return dc.getICardInformation(entry, media_folder)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    # create logger with 'spam_application'
    logger = logging.getLogger('twitter_app')
    logger.setLevel(logging.INFO)

    urls = []

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
        worked = 0
        entries = dm.getURLEntriesToProcess()
        num_entries = len(entries)
        logger.info("Number of URLs to process: %s", len(entries))
        url_list = []

        while num_entries > 0:
            s_time = time.time()
            del url_list[:]
            with Pool(processes=15) as pool:
                for entry in entries:
                    res = pool.apply_async(processURLTable, args=(entry,), callback=updateProgress)
                    url_list.append(res)

                pool.close()
                pool.join()
                print("Updating Table ....")
                t1 = time.time()
                update_urls = []
                for url in url_list:
                    update_urls.append(url.get())

                dm.insertMultipleRowPostgreSQL(update_urls, "tweets_urls")
                t2 = time.time()
                print("Done in %0.2f s" %(t2-t1))

                worked = 0
                print("Getting Remaining URLs ...")
                t1 = time.time()
                entries = dm.getURLEntriesToProcess()
                t2 = time.time()
                print("Done in %0.2f s" % (t2 - t1))
                num_entries = len(entries)
                logger.info("Number of URLs to process: %s", len(entries))

    if table == 2:
        worked = 0
        entries = dm.getMediaEntriesPostgreSQL()
        num_entries = len(entries)
        logger.info("Number of Media Entries to process: %s", len(entries))
        media_list = []

        while num_entries > 0:

            s_time = time.time()
            del media_list[:]
            with Pool(processes=15) as pool:
                for entry in entries:
                    res = pool.apply_async(processMediaTable, args=(entry,), callback=updateProgress)
                    media_list.append(res)
                pool.close()
                pool.join()

                print("Updating Table ....")
                t1 = time.time()
                update_media = []
                for media in media_list:
                    update_media.append(media.get())

                dm.insertMultipleRowPostgreSQL(update_media, "tweets_media")
                t2 = time.time()
                print("Done in %0.2f s" % (t2 - t1))

                worked = 0
                print("Getting Remaining URLs ...")
                t1 = time.time()
                entries = dm.getMediaEntriesPostgreSQL()
                t2 = time.time()
                print("Done in %0.2f s" % (t2 - t1))
                num_entries = len(entries)
                logger.info("Number of Media entries to process: %s", len(entries))

    if table == 3:
        worked = 0
        entries = dm.getICardEntriesPostreSQL()
        num_entries = len(entries)
        logger.info("Number of ICard Entries to process: %s", len(entries))
        url_list = []

        while num_entries > 0:
            s_time = time.time()
            del url_list[:]
            with Pool(processes=15) as pool:
                for entry in entries:
                    res = pool.apply_async(processICardsTable, args=(entry,), callback=updateProgress)
                    url_list.append(res)
                pool.close()
                pool.join()

                print("Updating Table ....")
                t1 = time.time()
                update_urls = []
                for url in url_list:
                    update_urls.append(url.get())

                dm.insertMultipleRowPostgreSQL(update_urls, "tweets_icards")
                t2 = time.time()
                print("Done in %0.2f s" % (t2 - t1))

                worked = 0
                print("Getting Remaining URLs ...")
                t1 = time.time()
                entries = dm.getICardEntriesPostreSQL()
                t2 = time.time()
                print("Done in %0.2f s" % (t2 - t1))
                num_entries = len(entries)
                logger.info("Number of ICard Entries to process: %s", len(entries))

    if table == 4:
        worked = 0
        entries = dm.getUserURLEntriesToProcess()
        num_entries = len(entries)
        logger.info("Number of User URLs to process: %s", len(entries))
        url_list = []

        while num_entries > 0:
            s_time = time.time()
            del url_list[:]
            with Pool(processes=15) as pool:
                for entry in entries:
                    res = pool.apply_async(processUserURLTable, args=(entry,), callback=updateProgress)
                    url_list.append(res)
                pool.close()
                pool.join()

                print("Updating Table ....")
                t1 = time.time()
                update_urls = []
                for url in url_list:
                    update_urls.append(url.get())

                dm.insertMultipleRowPostgreSQL(update_urls, "user_urls")
                t2 = time.time()
                print("Done in %0.2f s" % (t2 - t1))

                worked = 0
                print("Getting Remaining URLs ...")
                t1 = time.time()
                entries = dm.getUserURLEntriesToProcess()
                t2 = time.time()
                print("Done in %0.2f s" % (t2 - t1))
                num_entries = len(entries)
                logger.info("Number of User URLs to process: %s", len(entries))

    if table == 5:
        worked = 0
        entries = dm.getUserImageEntries()
        num_entries = len(entries)
        logger.info("Number of User Images to process: %s", len(entries))
        url_list = []

        while num_entries > 0:
            s_time = time.time()
            del url_list[:]
            with Pool(processes=15) as pool:
                for entry in entries:
                    res = pool.apply_async(processUserImagesTable, args=(entry,), callback=updateProgress)
                    url_list.append(res)
                pool.close()
                pool.join()

                print("Updating Table ....")
                t1 = time.time()
                update_urls = []
                for url in url_list:
                    update_urls.append(url.get())

                dm.insertMultipleRowPostgreSQL(update_urls, "user_images")
                t2 = time.time()
                print("Done in %0.2f s" % (t2 - t1))

                worked = 0
                print("Getting Remaining URLs ...")
                t1 = time.time()
                entries = dm.getUserImageEntries()
                t2 = time.time()
                print("Done in %0.2f s" % (t2 - t1))
                num_entries = len(entries)
                logger.info("Number of User Images to process: %s", len(entries))

