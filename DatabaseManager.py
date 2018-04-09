import sqlite3

class DatabaseManager:

    def __init__(self, data_path, data_name):
        self.db_info_file = data_path + data_name + "_info.sql"
        self.db_info_connection = self.create_connection(self.db_info_file)
        self.createUserTable()
        self.createInfoTable()
        self.createURLTable()
        self.createMediaTable()
        self.createICardTable()


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

    """
    def createInfoTable(self, tweet):
        conn = self.db_info_connection
        column_names = {'id'}
        for key in tweet.keys():
            column_names.add(key)

        column_names.remove('id')

        c_names_str = "id integer PRIMARY KEY"
        for c_name in column_names:
            c_names_str = c_names_str + ", " + c_name
        create_table_str = "CREATE TABLE IF NOT EXISTS tweets_info ( " + c_names_str + " );"

        c = conn.cursor()
        c.execute(create_table_str)
        conn.commit()
        c.close()
    """

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
        conn = self.db_info_connection
        c = conn.cursor()

        ins_row_sql = "INSERT or REPLACE INTO tweets_users( "
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

    def updateURLEntry(self, url_info):
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
        conn = self.db_info_connection
        c = conn.cursor()

        c.execute("SELECT tweet_id, media_url, type FROM tweets_media WHERE is_processed is 0 ")

        entries = c.fetchall()

        return entries

    def updateMediaEntry(self, media_info):
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
        conn = self.db_info_connection
        c = conn.cursor()

        c.execute("SELECT tweet_id type FROM tweets_icards WHERE is_processed is 0 ")

        entries = c.fetchall()

        return entries

    def updateICardEntry(self, icard_info):
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
        values = (  icard_info['tweet_id'],
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