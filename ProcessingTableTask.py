import json
import logging

class ProcessingTask:

    def __init__(self, dataCrawler, dataManager, num_lines, s_time):
        self.dc = dataCrawler
        self.dm = dataManager
        self.num_lines = num_lines
        self.s_time = s_time
        logging.basicConfig(level=logging.INFO)
        # create logger with 'spam_application'
        self.logger = logging.getLogger('twitter_app')
        self.logger.setLevel(logging.INFO)
        self.worked = 0


    def updateProgress(self):
        self.worked += 1
        if self.worked % 100 == 0:
            self.dc.printProgressBar(self.worked, self.num_lines, self.s_time)

    def processTweet(self, tweet):

        twitter_user = self.dc.getUserInfo(tweet)
        self.dm.insertUserRow(twitter_user)

        user_link = self.dc.getUserLink(tweet)
        if user_link is not None:
            self.dm.insertUserURLRow(user_link, tweet['id'])

        user_images = self.dc.getUserImages(tweet)
        if user_images:
            self.dm.insertUserImageRow(user_images)

        tweet_info = self.dc.getTweetInformation(tweet)
        self.dm.insertInfoRow(tweet_info)

        if tweet_info['hashtags'] is not None:
            self.dm.insertHashtagRow(tweet_info)

        user_m_list = self.dc.getUserMentions(tweet)
        if user_m_list['user_mentions_list'] is not None:
            self.dm.insertUserMentionRow(user_m_list)

        tweet['urls'] = self.dc.getTweetURLs(tweet)
        self.dm.insertURLRow(tweet)

        if 'place' in tweet:
            if tweet['place'] != None:
                place = self.dc.getPlaceInfo(tweet)
                self.dm.insertPlaceRow(place)

        tweet['media_content'] = self.dc.getTweetMedia(tweet)
        self.dm.insertMediaRow(tweet)

        self.dm.insertICardRow(tweet)

    def findTweet(self, line):
        try:
            tweet = json.loads(line)
            tweet['extracted'] = False
            if 'retweeted_status' in tweet:
                original_tweet = tweet['retweeted_status']
                original_tweet = self.dc.setTypeFlags(original_tweet)
                original_tweet = self.dc.resolveTruncated(original_tweet)
                self.processTweet(original_tweet)


                if 'quoted_status' in original_tweet:
                    quoted_tweet = original_tweet['quoted_status']
                    quoted_tweet = self.dc.setTypeFlags(quoted_tweet)
                    quoted_tweet = self.dc.resolveTruncated(quoted_tweet)
                    self.processTweet(quoted_tweet)

                if 'quoted_status' in tweet:
                    quoted_tweet = tweet['quoted_status']
                    quoted_tweet = self.dc.setTypeFlags(quoted_tweet)
                    quoted_tweet = self.dc.resolveTruncated(quoted_tweet)
                    self.processTweet(quoted_tweet)

            else:
                if 'quoted_status' in tweet:
                    quoted_tweet = tweet['quoted_status']
                    quoted_tweet = self.dc.setTypeFlags(quoted_tweet)
                    quoted_tweet = self.dc.resolveTruncated(quoted_tweet)
                    self.processTweet(quoted_tweet)

            if 'id' in tweet:
                tweet = self.dc.setTypeFlags(tweet)
                tweet = self.dc.resolveTruncated(tweet)
                self.processTweet(tweet)
            else:
                self.logger.error("This line is corrupted: %s", tweet)

            self.updateProgress()
        except:
            self.logger.error("Could not Read Line: ", line)