import sqlite3
import psycopg2
from config import config

class DatabaseManager:

    def __init__(self, data_path, data_name, database_selection):
        self.db_selection = database_selection
        if database_selection == 1:
            self.db_info_file = data_path + data_name + "_info.sql"
            self.db_info_connection = self.create_connection(self.db_info_file)
            self.createUserTable()
            self.createInfoTable()
            self.createURLTable()
            self.createMediaTable()
            self.createICardTable()
        if database_selection == 2:
            self.db_info_file = data_path + data_name + "_info.sql"
            self.db_info_connection = self.createPostgreSQLConnection()
            self.createUserTablePostgreSQL()
            self.createInfoTablePostgreSQL()
            self.createURLTablePostgreSQL()
            self.createMediaTablePostgreSQL()
            self.createICardTablePostgreSQL()


    def create_connection(self, db_file):
        """ create a database connection to the SQLite database
            specified by db_file
        :param db_file: database file
        :return: Connection object or None
        """
        try:
            conn = sqlite3.connect(db_file)
            return conn
        except Error as e:
            print(e)

    def createPostgreSQLConnection(self):
        """ Connect to the PostgreSQL database server """
        conn = None
        try:
            # read connection parameters
            params = config()

            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            conn = psycopg2.connect(**params)

            # create a cursor
            cur = conn.cursor()

            # execute a statement
            print('PostgreSQL database version:')
            cur.execute('SELECT version()')

            # display the PostgreSQL database server version
            db_version = cur.fetchone()
            print(db_version)

            # close the communication with the PostgreSQL
            cur.close()

            return conn
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def createInfoTablePostgreSQL(self):
        conn = self.db_info_connection

        create_table_str = """CREATE TABLE IF NOT EXISTS tweets_info ( 
                                       id bigint,
                                       user_id bigint,
                                       text text,
                                       created_at text,
                                       source text,
                                       lang text,
                                       truncated boolean,
                                       is_retweet boolean,
                                       retweet_id bigint,
                                       is_quote boolean,
                                       quote_id bigint,
                                       is_reply boolean,
                                       reply_to_status_id bigint,
                                       reply_to_user_id bigint,
                                       quote_count integer,
                                       reply_count integer,
                                       retweet_count integer,
                                       favorite_count integer,
                                       favorited boolean,
                                       retweeted boolean,
                                       hashtags text,
                                       user_mentions text,
                                       number_of_urls integer,
                                       PRIMARY KEY(id));"""

        c = conn.cursor()
        c.execute(create_table_str)
        conn.commit()
        c.close()

    def createInfoTable(self):
        conn = self.db_info_connection

        create_table_str = """CREATE TABLE IF NOT EXISTS tweets_info ( 
                                       id integer,
                                       user_id,
                                       text,
                                       created_at,
                                       source,
                                       lang,
                                       truncated,
                                       is_retweet,
                                       retweet_id,
                                       is_quote,
                                       quote_id,
                                       is_reply,
                                       reply_to_status_id,
                                       reply_to_user_id,
                                       quote_count,
                                       reply_count,
                                       retweet_count,
                                       favorite_count,
                                       favorited,
                                       reweeted,
                                       hashtags,
                                       user_mentions,
                                       number_of_urls,
                                       PRIMARY KEY(id));"""

        c = conn.cursor()
        c.execute(create_table_str)
        conn.commit()
        c.close()

    def insertInfoRow(self, tweet):
        if self.db_selection == 1:
            self.insertInfoRowSQLite(tweet)
        if self.db_selection == 2:
            self.insertRowPostgreSQL(tweet, "tweets_info")

    def insertInfoRowSQLite(self, tweet):
        conn = self.db_info_connection
        c = conn.cursor()

        ins_row_sql = "INSERT or REPLACE INTO tweets_info( "
        key_str = ""
        value_str = ""
        for key in tweet.keys():

            if tweet[key] == "nan" or tweet[key] == "NaN":
                tweet[key] = None

            if key != "id" and (type(tweet[key]) != bool and None):
                tweet[key] = str(tweet[key])

            if isinstance(tweet[key], list):
                tweet[key] = str(tweet[key])

            key_str += " " + key
            value_str += " ?"

        key_str = key_str[1:].replace(" ", ", ")
        value_str = value_str[1:].replace(" ", ", ")

        ins_row_sql += key_str + ") VALUES ( " + value_str + ");"

        values = tuple(tweet.values())
        c.execute(ins_row_sql, values)

        conn.commit()
        c.close()

    def insertInfoRowPostgreSQL(self, tweet):
        conn = self.db_info_connection
        c = conn.cursor()

        ins_row_sql = "INSERT INTO tweets_info( "
        key_str = ""
        value_str = ""

        for key in tweet.keys():

            if tweet[key] == "nan" or tweet[key] == "NaN":
                tweet[key] = None

            if key != "id" and (type(tweet[key]) != bool and None):
                tweet[key] = str(tweet[key])

            if isinstance(tweet[key], list):
                tweet[key] = str(tweet[key])

            key_str += " " + key
            value_str += " %s"

        key_str = key_str[1:].replace(" ", ", ")
        value_str = value_str[1:].replace(" ", ", ")

        ins_row_sql += key_str + ") VALUES ( " + value_str + ")"

        key_set = ", ".join(tweet.keys())
        key_set_exluded = []
        for key in key_set:
            key_set_exluded.append("EXCLUDED.%s" %key)

        value_replacement_keys = ", ".join(key_set_exluded)

        conflict_bit = "ON CONFLICT (id) DO UPDATE set (%s) = (%s);" %(key_set, value_replacement_keys)

        values = tuple(tweet.values())
        c.execute(conflict_bit, values)

        conn.commit()
        c.close()

    def createURLTablePostgreSQL(self):
        conn = self.db_info_connection

        create_table_str = """CREATE TABLE IF NOT EXISTS tweets_urls ( 
                                tweet_id bigint,
                                short_url text,
                                resolved_url text,
                                response_code integer,
                                domain text,
                                top_level_domain text,
                                is_twitter_url boolean,
                                is_media boolean,
                                is_processed boolean,
                                failed boolean,
                                PRIMARY KEY(tweet_id, short_url));"""

        c = conn.cursor()
        c.execute(create_table_str)
        conn.commit()
        c.close()

    def createURLTable(self):
        conn = self.db_info_connection

        create_table_str = """CREATE TABLE IF NOT EXISTS tweets_urls ( 
                                tweet_id integer REFERENCES tweet_info(id),
                                short_url,
                                resolved_url,
                                response_code,
                                domain,
                                top_level_domain,
                                is_twitter_url,
                                is_media,
                                is_processed,
                                failed,
                                PRIMARY KEY(tweet_id, short_url));"""

        c = conn.cursor()
        c.execute(create_table_str)
        conn.commit()
        c.close()

    def insertURLRow(self, tweet):
        if self.db_selection == 1:
            self.insertURLRowSQLite(tweet)
        if self.db_selection == 2:
            self.insertURLRowPostgreSQL(tweet)

    def insertURLRowSQLite(self, tweet):
        conn = self.db_info_connection
        c = conn.cursor()

        for url in tweet['urls']:
            ins_row_sql = """INSERT or IGNORE INTO tweets_urls( 
                                tweet_id,
                                short_url,
                                resolved_url,
                                response_code,
                                domain,
                                top_level_domain,
                                is_twitter_url,
                                is_media,
                                is_processed,
                                failed)
                                    VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
            values = (tweet['id'], url, None, None, None, None, None, None, False, False)
            c.execute(ins_row_sql, values)
            conn.commit()

        c.close()

    def insertURLRowPostgreSQL(self, tweet):
        conn = self.db_info_connection
        c = conn.cursor()

        for url in tweet['urls']:
            ins_row_sql = """INSERT INTO tweets_urls( 
                                tweet_id,
                                short_url,
                                resolved_url,
                                response_code,
                                domain,
                                top_level_domain,
                                is_twitter_url,
                                is_media,
                                is_processed,
                                failed)
                                    VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                    ON CONFLICT (tweet_id, short_url) DO NOTHING"""
            values = (tweet['id'], url, None, None, None, None, None, None, False, False)
            c.execute(ins_row_sql, values)
            conn.commit()

        c.close()

    def createMediaTablePostgreSQL(self):
        conn = self.db_info_connection

        create_table_str = """CREATE TABLE IF NOT EXISTS tweets_media ( 
                                tweet_id bigint,
                                media_url text,
                                type text,
                                response_code integer,
                                hash text,
                                path text,
                                is_processed boolean,
                                PRIMARY KEY(tweet_id, media_url));"""

        c = conn.cursor()
        c.execute(create_table_str)
        conn.commit()
        c.close()

    def createMediaTable(self):
        conn = self.db_info_connection

        create_table_str = """CREATE TABLE IF NOT EXISTS tweets_media ( 
                                tweet_id integer REFERENCES tweet_info(id),
                                media_url,
                                type,
                                response_code,
                                hash,
                                path,
                                is_processed,
                                PRIMARY KEY(tweet_id, media_url));"""

        c = conn.cursor()
        c.execute(create_table_str)
        conn.commit()
        c.close()

    def insertMediaRow(self, tweet):
        if self.db_selection == 1:
            self.insertMediaRowSQLite(tweet)
        if self.db_selection == 2:
            self.insertMediaRowPostgreSQL(tweet)

    def insertMediaRowSQLite(self, tweet):
        conn = self.db_info_connection
        c = conn.cursor()

        for media in tweet['media_content']:
            ins_row_sql = """INSERT or IGNORE INTO tweets_media( 
                                tweet_id,
                                media_url,
                                type,
                                response_code,
                                hash,
                                path,
                                is_processed)
                                    VALUES ( ?, ?, ?, ?, ?, ?, ?)"""
            values = (tweet['id'], media['url'], media['type'], None, None, None, False)
            c.execute(ins_row_sql, values)
            conn.commit()

        c.close()

    def insertMediaRowPostgreSQL(self, tweet):
        conn = self.db_info_connection
        c = conn.cursor()

        for media in tweet['media_content']:
            ins_row_sql = """INSERT INTO tweets_media( 
                                tweet_id,
                                media_url,
                                type,
                                response_code,
                                hash,
                                path,
                                is_processed)
                                    VALUES ( %s, %s, %s, %s, %s, %s, %s)
                                    ON CONFLICT (tweet_id, media_url) DO NOTHING"""
            values = (tweet['id'], media['url'], media['type'], None, None, None, False)
            c.execute(ins_row_sql, values)
            conn.commit()

        c.close()

    def createICardTablePostgreSQL(self):
        conn = self.db_info_connection

        create_table_str = """CREATE TABLE IF NOT EXISTS tweets_icards ( 
                                tweet_id bigint,
                                url text,
                                title text,
                                text text,
                                domain text,
                                top_level_domain text,
                                response_code integer,
                                img text,
                                hash text,
                                path text,
                                has_icard boolean,
                                is_processed boolean,
                                PRIMARY KEY(tweet_id));"""

        c = conn.cursor()
        c.execute(create_table_str)
        conn.commit()
        c.close()

    def createICardTable(self):
        conn = self.db_info_connection

        create_table_str = """CREATE TABLE IF NOT EXISTS tweets_icards ( 
                                tweet_id integer REFERENCES tweet_info(id),
                                url,
                                title,
                                text,
                                domain,
                                top_level_domain,
                                response_code,
                                img,
                                hash,
                                path,
                                has_icard,
                                is_processed,
                                PRIMARY KEY(tweet_id));"""

        c = conn.cursor()
        c.execute(create_table_str)
        conn.commit()
        c.close()

    def insertICardRow(self, tweet):
        if self.db_selection == 1:
            self.insertICardRow(tweet)
        if self.db_selection == 2:
            self.insertICardRowPostgreSQL(tweet)

    def insertICardRowSQLite(self, tweet):
        conn = self.db_info_connection
        c = conn.cursor()

        ins_row_sql = """INSERT or IGNORE INTO tweets_icards( 
                            tweet_id,
                            url,
                            title,
                            text,
                            domain,
                            top_level_domain,
                            response_code,
                            img,
                            hash,
                            path,
                            has_icard,
                            is_processed)
                                VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        values = (tweet['id'], None, None, None, None, None, None, None, None, None, None, False)
        c.execute(ins_row_sql, values)
        conn.commit()

        c.close()

    def insertICardRowPostgreSQL(self, tweet):
        conn = self.db_info_connection
        c = conn.cursor()

        ins_row_sql = """INSERT INTO tweets_icards( 
                            tweet_id,
                            url,
                            title,
                            text,
                            domain,
                            top_level_domain,
                            response_code,
                            img,
                            hash,
                            path,
                            has_icard,
                            is_processed)
                                VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
                                ON CONFLICT (tweet_id) DO NOTHING"""
        values = (tweet['id'], None, None, None, None, None, None, None, None, None, None, False)
        c.execute(ins_row_sql, values)
        conn.commit()

        c.close()


    def createUserTablePostgreSQL(self):
        conn = self.db_info_connection

        create_table_str = """CREATE TABLE IF NOT EXISTS tweets_users ( 
                                id bigint,
                                name text,
                                location text,
                                url text,
                                description text,
                                protected boolean,
                                verified boolean,
                                followers_count text,
                                friends_count text,
                                listed_count text,
                                favourites_count text,
                                statuses_count text,
                                created_at text,
                                geo_enabled boolean,
                                lang text,
                                PRIMARY KEY(id));"""

        c = conn.cursor()
        c.execute(create_table_str)
        conn.commit()
        c.close()

    def createUserTable(self):
        conn = self.db_info_connection

        create_table_str = """CREATE TABLE IF NOT EXISTS tweets_users ( 
                                id integer REFERENCES tweet_info(id),
                                name,
                                location,
                                url,
                                description,
                                protected,
                                verified,
                                followers_count,
                                friends_count,
                                listed_count,
                                favourites_count,
                                statuses_count,
                                created_at,
                                geo_enabled,
                                lang,
                                PRIMARY KEY(id));"""

        c = conn.cursor()
        c.execute(create_table_str)
        conn.commit()
        c.close()

    def insertUserRow(self, tweet):
        if self.db_selection == 1:
            self.insertRowSQLite(tweet, "tweets_users")
        if self.db_selection == 2:
            self.insertRowPostgreSQL(tweet, "tweets_users")

    def insertRowPostgreSQL(self, tweet, table_name):
        conn = self.db_info_connection
        c = conn.cursor()

        ins_row_sql = "INSERT INTO %s( " %table_name
        key_str = ""
        value_str = ""

        for key in tweet.keys():

            if tweet[key] == "nan" or tweet[key] == "NaN":
                tweet[key] = None

            if key != "id" and (type(tweet[key]) != bool and None):
                tweet[key] = str(tweet[key])

            if isinstance(tweet[key], list):
                tweet[key] = str(tweet[key])

            key_str += " " + key
            value_str += " %s"

        key_str = key_str[1:].replace(" ", ", ")
        value_str = value_str[1:].replace(" ", ", ")

        ins_row_sql += key_str + ") VALUES ( " + value_str + ")"

        key_set = ", ".join(tweet.keys())
        key_set_exluded = []
        for key in tweet.keys():
            key_set_exluded.append("EXCLUDED.%s" %key)

        value_replacement_keys = ", ".join(key_set_exluded)

        conflict_bit = self.getConflictClausePostreSQL(table_name) + " DO UPDATE set (%s) = (%s);" %(key_set, value_replacement_keys)
        ins_row_sql += conflict_bit
        values = tuple(tweet.values())

        c.execute(ins_row_sql, values)

        conn.commit()
        c.close()

    def getConflictClausePostreSQL(self, table_name):
        if table_name == "tweets_urls":
            return "ON CONFLICT (tweet_id, short_url)"
        if table_name == "tweets_icards":
            return "ON CONFLICT (tweet_id)"
        if table_name == "tweets_info":
            return "ON CONFLICT (id)"
        if table_name == "tweets_media":
            return "ON CONFLICT (tweet_id, media_url)"
        if table_name == "tweets_users":
            return "ON CONFLICT (id)"

    def insertRowSQLite(self, tweet, table_name):
        conn = self.db_info_connection
        c = conn.cursor()

        ins_row_sql = "INSERT or REPLACE INTO %s( " %table_name
        key_str = ""
        value_str = ""
        for key in tweet.keys():

            if tweet[key] == "nan" or tweet[key] == "NaN":
                tweet[key] = None

            if key != "id" and (type(tweet[key]) != bool and None):
                tweet[key] = str(tweet[key])

            if isinstance(tweet[key], list):
                tweet[key] = str(tweet[key])

            key_str += " " + key
            value_str += " ?"

        key_str = key_str[1:].replace(" ", ", ")
        value_str = value_str[1:].replace(" ", ", ")

        ins_row_sql += key_str + ") VALUES ( " + value_str + ");"

        values = tuple(tweet.values())
        c.execute(ins_row_sql, values)

        conn.commit()
        c.close()

    def getURLEntriesToProcess(self):
        conn = self.db_info_connection
        c = conn.cursor()

        c.execute("SELECT tweet_id, short_url FROM tweets_urls WHERE is_processed is 0 ")

        entries = c.fetchall()

        return entries

    def getURLEntriesToProcess(self):
        conn = self.db_info_connection
        c = conn.cursor()

        if self.db_selection == 1:
            c.execute("SELECT tweet_id, short_url FROM tweets_urls WHERE is_processed is 0 ")
        if self.db_selection == 2:
            c.execute("SELECT tweet_id, short_url FROM tweets_urls WHERE is_processed is false ")

        entries = c.fetchall()

        return entries

    def updateURLEntry(self, url_info):
        if self.db_selection == 1:
            self.updateURLEntrySQLite(url_info)
        if self.db_selection == 2:
            self.insertRowPostgreSQL(url_info, "tweets_urls")

    def updateURLEntrySQLite(self, url_info):
        conn = self.db_info_connection
        c = conn.cursor()

        ins_row_sql = """INSERT or REPLACE INTO tweets_urls( 
                            tweet_id,
                            short_url,
                            resolved_url,
                            response_code,
                            domain,
                            top_level_domain,
                            is_twitter_url,
                            is_media,
                            is_processed,
                            failed)
                                VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        values = (  url_info['tweet_id'],
                    url_info['short_url'],
                    url_info['resolved_url'],
                    url_info['response_code'],
                    url_info['domain'],
                    url_info['top_level_domain'],
                    url_info['is_twitter_url'],
                    url_info['is_media'],
                    url_info['is_processed'],
                    url_info['failed'])
        c.execute(ins_row_sql, values)
        conn.commit()

        c.close()

    def getMediaEntries(self):
        if self.db_selection == 1:
            return self.getMediaEntriesSQLite()
        if self.db_selection == 2:
            return self.getMediaEntriesPostgreSQL()

    def getMediaEntriesSQLite(self):
        conn = self.db_info_connection
        c = conn.cursor()

        c.execute("SELECT tweet_id, media_url, type FROM tweets_media WHERE is_processed is 0 ")

        entries = c.fetchall()

        return entries

    def getMediaEntriesPostgreSQL(self):
        conn = self.db_info_connection
        c = conn.cursor()

        c.execute("SELECT tweet_id, media_url, type FROM tweets_media WHERE is_processed is false ")

        entries = c.fetchall()

        return entries

    def updateMediaEntry(self, media_info):
        if self.db_selection == 1:
            self.updateMediaEntrySQLite(media_info)
        if self.db_selection == 2:
            self.insertRowPostgreSQL(media_info, "tweets_media")

    def updateMediaEntrySQLite(self, media_info):
        conn = self.db_info_connection
        c = conn.cursor()

        ins_row_sql = """INSERT or REPLACE INTO tweets_media( 
                            tweet_id,
                            media_url,
                            type,
                            response_code,
                            hash,
                            path,
                            is_processed)
                                VALUES ( ?, ?, ?, ?, ?, ?, ?)"""
        values = (  media_info['tweet_id'],
                    media_info['media_url'],
                    media_info['type'],
                    media_info['response_code'],
                    media_info['hash'],
                    media_info['path'],
                    media_info['is_processed'])
        c.execute(ins_row_sql, values)
        conn.commit()

        c.close()

    def getICardEntries(self):
        if self.db_selection == 1:
            return self.getICardEntriesSQLite()
        if self.db_selection == 2:
            return self.getICardEntriesPostreSQL()

    def getICardEntriesSQLite(self):
        conn = self.db_info_connection
        c = conn.cursor()

        c.execute("SELECT tweet_id FROM tweets_icards WHERE is_processed is 0;")

        entries = c.fetchall()

        return entries

    def getICardEntriesPostreSQL(self):
        conn = self.db_info_connection
        c = conn.cursor()

        c.execute("SELECT tweet_id FROM tweets_icards WHERE is_processed is false;")

        entries = c.fetchall()

        return entries

    def updateICardEntry(self, icard_info):
        if self.db_selection == 1:
            self.updateICardEntrySQLite(icard_info)
        if self.db_selection == 2:
            self.insertRowPostgreSQL(icard_info, "tweets_icards")


    def updateICardEntrySQLite(self, icard_info):
        conn = self.db_info_connection
        c = conn.cursor()

        ins_row_sql = """INSERT or REPLACE INTO tweets_icards( 
                            tweet_id,
                            url,
                            title,
                            text,
                            domain,
                            top_level_domain,
                            response_code,
                            img,
                            hash,
                            path,
                            has_icard,
                            is_processed)
                                VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        values = (icard_info['tweet_id'],
                  icard_info['url'],
                  icard_info['title'],
                  icard_info['text'],
                  icard_info['domain'],
                  icard_info['top_level_domain'],
                  icard_info['response_code'],
                  icard_info['img'],
                  icard_info['hash'],
                  icard_info['path'],
                  icard_info['has_icard'],
                  icard_info['is_processed'])
        c.execute(ins_row_sql, values)
        conn.commit()

        c.close()