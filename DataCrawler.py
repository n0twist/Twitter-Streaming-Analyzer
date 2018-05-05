import time
import requests
import re
from urllib.parse import urlparse
import logging
import mimetypes
from tld import get_tld
import imagehash
from PIL import Image
from io import BytesIO
import os
from bs4 import BeautifulSoup


module_logger = logging.getLogger('twitter_app.dataprocessor')

class DataCrawler:

    def __init__(self):
        self.logger = logging.getLogger('twitter_app.datcrawler.DataCrawler')

    def getTweetMedia(self, tweet):
        media_content = []

        if 'extended_entities' in tweet:
            if 'media' in tweet['extended_entities']:
                for media in tweet['extended_entities']['media']:
                    media_entry = {}
                    if 'media_url' in media:
                        media_entry['url'] = media['media_url']

                        if 'type' in media:
                            media_entry['type'] = media['type']
                        else:
                            media_entry['type'] = None

                        media_content.append(media_entry)

        return media_content

    def getTweetURLs(self, tweet):
        url_list = []

        if 'entities' in tweet:
            if 'urls' in tweet['entities']:
               for url in tweet['entities']['urls']:
                   if "expanded_url" in url:
                       url_list.append(url["expanded_url"])

        return url_list

    def getUserInfo(self, tweet):
        user = {}

        user['id'] = tweet['user']['id'] if 'id' in tweet['user'] else None
        user['name'] = tweet['user']['name'] if 'name' in tweet['user'] else None
        user['location'] = tweet['user']['location'] if 'location' in tweet['user'] else None
        user['url'] = tweet['user']['url'] if 'url' in tweet['user'] else None
        user['description'] = tweet['user']['description'] if 'description' in tweet['user'] else None
        user['protected'] = tweet['user']['protected'] if 'protected' in tweet['user'] else None
        user['verified'] = tweet['user']['verified'] if 'verified' in tweet['user'] else None
        user['followers_count'] = tweet['user']['followers_count'] if 'followers_count' in tweet['user'] else None
        user['friends_count'] = tweet['user']['friends_count'] if 'friends_count' in tweet['user'] else None
        user['listed_count'] = tweet['user']['listed_count'] if 'listed_count' in tweet['user'] else None
        user['favourites_count'] = tweet['user']['favourites_count'] if 'favourites_count' in tweet['user'] else None
        user['statuses_count'] = tweet['user']['statuses_count'] if 'statuses_count' in tweet['user'] else None
        user['created_at'] = tweet['user']['created_at'] if 'created_at' in tweet['user'] else None
        user['geo_enabled'] = tweet['user']['geo_enabled'] if 'geo_enabled' in tweet['user'] else None
        user['lang'] = tweet['user']['lang'] if 'lang' in tweet['user'] else None


        return user

    def getUserLink(self, tweet):
        short_url = tweet['user']['url'] if 'url' in tweet['user'] else None
        if short_url != None:
            if self.is_url_valid(short_url):
                return short_url
            else:
                return None
        else:
            return None


    def getPlaceInfo(self, tweet):
        place = {}

        place['tweet_id'] = tweet['id']if 'id' in tweet else None
        place['id'] = tweet['place']['id'] if 'id' in tweet['place'] else None
        place['country'] = tweet['place']['country'] if 'country' in tweet['place'] else None
        place['country_code'] = tweet['place']['country_code'] if 'country_code' in tweet['place'] else None
        place['full_name'] = tweet['place']['full_name'] if 'full_name' in tweet['place'] else None
        place['id'] = tweet['place']['id'] if 'id' in tweet['place'] else None
        place['name'] = tweet['place']['name'] if 'name' in tweet['place'] else None
        place['place_type'] = tweet['place']['place_type'] if 'place_type' in tweet['place'] else None
        place['url'] = tweet['place']['url'] if 'url' in tweet['place'] else None

        if 'bounding_box' in tweet['place']:
            place['type'] = tweet['place']['bounding_box']['type'] if 'type' in tweet['place']['bounding_box'] else None
            place['coordinates'] = str(tweet['place']['bounding_box']['coordinates']) if 'coordinates' in tweet['place']['bounding_box'] else None
        else:
            place['type'] = None
            place['coordinates'] = None
        return place

    def getTweetInformation(self, tweet):
        info = {}

        info['id'] = tweet['id'] if 'id' in tweet else None
        info['user_id'] = tweet['user']['id'] if 'user' in tweet else None
        info['text'] = tweet['text'] if 'text' in tweet else None
        info['created_at'] = tweet['created_at'] if 'created_at' in tweet else None
        info['source'] = tweet['source'] if 'source' in tweet else None

        info['truncated'] = tweet['truncated'] if 'truncated' in tweet else None

        info['is_retweet'] = tweet['isRetweet'] if 'isRetweet' in tweet else None
        info['retweet_id'] = tweet['retweet_id'] if 'retweet_id' in tweet else None

        info['is_quote'] = tweet['isQuote'] if 'isQuote' in tweet else None
        info['quote_id'] = tweet['quote_id'] if 'quote_id' in tweet else None

        info['is_reply'] = tweet['isReply'] if 'isReply' in tweet else None
        info['reply_to_status_id'] = tweet['replyToStatusId'] if 'replyToStatusId' in tweet else None
        info['reply_to_user_id'] = tweet['replyToUserID'] if 'replyToUserID' in tweet else None

        info['quote_count'] = tweet['quote_count'] if 'quote_count' in tweet else None
        info['reply_count'] = tweet['reply_count'] if 'reply_count' in tweet else None
        info['retweet_count'] = tweet['retweet_count'] if 'retweet_count' in tweet else None
        info['favorite_count'] = tweet['favorite_count'] if 'favorite_count' in tweet else None
        info['favorited'] = tweet['favorited'] if 'favorited' in tweet else None
        info['retweeted'] = tweet['retweeted'] if 'retweeted' in tweet else None
        info['lang'] = tweet['lang'] if 'lang' in tweet else None
        info['extracted'] = tweet['extracted'] if 'extracted' in tweet else True

        if 'entities' in tweet:
            if 'hashtags' in tweet['entities']:
                hashtags = []
                for hashtag in tweet['entities']['hashtags']:
                    if 'text' in hashtag:
                        hashtags.append(hashtag['text'])

                info['hashtags'] = " ".join(hashtags)

                if not hashtags:
                    info['hashtags'] = None
                else:
                    info['hashtags'] = " ".join(hashtags)
            else:
                info['hashtags'] = None

            if 'user_mentions' in tweet['entities']:
                user_mentions = []
                for user_mention in tweet['entities']['user_mentions']:
                    if 'id_str' in user_mention:
                        user_mentions.append(user_mention['id_str'])

                if not user_mentions:
                    info['user_mentions'] = None
                else:
                    info['user_mentions'] = " ".join(user_mentions)
            else:
                info['user_mentions'] = None

            if 'urls' in tweet['entities']:
                number_of_urls = 0
                for url in tweet['entities']['urls']:
                    if "expanded_url" in url:
                        number_of_urls += 1
                info['number_of_urls'] = number_of_urls
            else:
                info['number_of_urls'] = 0

        return info

    def resolveTruncated(self, tweet):
        if tweet['truncated'] == True:
            tweet['text'] = tweet['extended_tweet']['full_text']
            #self.logger.info(tweet['text'])
            if 'entities' in tweet['extended_tweet']:
                tweet['entities'] = tweet['extended_tweet']['entities']

            if 'extended_entities' in tweet['extended_tweet']:
                tweet['extended_entities'] = tweet['extended_tweet']['extended_entities']

            tweet['truncated'] = True
            return tweet
        else:
            tweet['truncated'] = False
            return tweet

    def setTypeFlags(self,tweet):
        if 'retweeted_status' in tweet:
            tweet["isRetweet"] = True
            tweet["retweet_id"] = tweet["retweeted_status"]['id']
        else:
            tweet["isRetweet"] = False
            tweet["retweet_id"] = None

        if 'quoted_status' in tweet:
            tweet["isQuote"] = True
            tweet["quote_id"] = tweet["quoted_status"]['id']
        else:
            tweet["isQuote"] = False
            tweet["quote_id"] = None

        if 'in_reply_to_status_id':
            if tweet['in_reply_to_status_id']:
                tweet["isReply"] = True
            else:
                tweet["isReply"] = False

            tweet["replyToStatusId"] = tweet["in_reply_to_status_id"]
            tweet["replyToUserID"] = tweet["in_reply_to_user_id"]

        return tweet

    def is_url_valid(self, url):
        regex = re.compile(
            r'^(?:http|ftp)s?://'  # http:// or https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
            r'localhost|'  # localhost...
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
            r'(?::\d+)?'  # optional port
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)

        if re.match(regex, url):
            return True
        else:
            return False

    def getDomain(self, url):
        parsed_uri = urlparse(url)
        domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
        return (domain)

    def getURLResponse(self, url):
        try:
            connect_timeout, read_timeout = 5.0, 10.0
            response = requests.head(url, timeout=(connect_timeout, read_timeout), allow_redirects=True)
            return response

        except requests.exceptions.ConnectionError as err:
            self.logger.error("Connection Error: %s", url)
            if err.request != None:
                last_url = err.request.url
                if last_url != None:
                    return last_url
            return None
        except requests.exceptions.Timeout as err:
            self.logger.error("Connection Timeout: %s", url)
            if err.request != None:
                last_url = err.request.url
                if last_url != None:
                    return last_url
            return None

        except requests.exceptions.TooManyRedirects as err:
            self.logger.error("TooManyRedirects - Exceeded 30 redirects: %s", url)
            return self.traceRedirections(url, 0)
        except requests.exceptions.InvalidSchema as err:
            self.logger.error("Invalid Schema: %s", url)
            if err.request != None:
                last_url = err.request.url
                if last_url != None:
                    return last_url
            return None


    def traceRedirections(self, url, count):
        try:
            connect_timeout, read_timeout = 5.0, 30.0
            response = requests.head(url, timeout=(connect_timeout, read_timeout))
            self.logger.warning(response.elapsed)
            if int(response.status_code / 100) == 3:
                '''
                if count == 0:
                    print(str(count) + " " + str(response.status_code) + " This is too short: " + url)
                if count >= 1:
                    print(str(count) + " " + str(response.status_code) + " This is still too short: " + url + " so I'll check this: " +response.headers['Location'])
                '''
                count += 1
                if count > 10:
                    return response

                if self.is_url_valid(response.headers['Location']):
                    return self.traceRedirections(response.headers['Location'], count)
                else:
                    fixed_url = self.getDomain(url) + response.headers['Location'][1:]
                    # print("\nWow ok that's not a Link! So I'll check this: " + fixed_url + "\n\n")

                    # if recursion is above 10 stop it
                    if count < 10:
                        return self.traceRedirections(fixed_url, count)
                    else:
                        # print("\nOK let's stop here and take this: " + url + "\n\n")
                        return response

            else:
                #if count >= 1:
                    #print(str(count) + " " + str(response.status_code) + " Seems like the Final thing: " + url + "\n")
                return response

        except requests.exceptions.ConnectionError as err:
            self.logger.error("Connection Error", url)
            if err.request != None:
                last_url = err.request.url
                if last_url != None:
                    return last_url
            return None
        except requests.exceptions.Timeout as err:
            self.logger.error("Connection Timeout", url)
            if err.request != None:
                last_url = err.request.url
                if last_url != None:
                    return last_url
            return None
        except requests.exceptions.InvalidSchema as err:
            self.logger.error("Invalid Schema: %s", url)
            if err.request != None:
                last_url = err.request.url
                if last_url != None:
                    return last_url
            return None

    def getURLInformation(self, entry):
        info = {}

        info['tweet_id'] = entry[0]
        url = entry[1]
        info['short_url'] = url

        response = self.getURLResponse(url)

        if response != None and isinstance(response, requests.Response):
            info['resolved_url'] = response.url
            info['response_code'] = response.status_code
            info['domain'] = self.getDomain(response.url)
            info['top_level_domain'] = get_tld(response.url, fail_silently=True)

            if info['top_level_domain'] != None:
                if info['top_level_domain'] == "twitter.com":
                    info['is_twitter_url'] = True
                else:
                    info['is_twitter_url'] = False
            else:
                info['is_twitter_url'] = False

            if 'Content-Type' in response.headers:
                maintype = response.headers['Content-Type'].split(';')[0].lower()

                if maintype in ['image/png', 'image/jpeg', 'image/gif']:
                    info['is_media'] = True
                else:
                    info['is_media'] = False
            else:
                maintype = mimetypes.guess_type(urlparse(url).path)[0]
                if maintype in ('image/png', 'image/jpeg', 'image/gif'):
                    info['is_media'] = True
                else:
                    info['is_media'] = False

            info['failed'] = False
            info['is_processed'] = True
        else:
            info['failed'] = True
            if isinstance(response, str):
                info['resolved_url'] = response
            else:
                info['resolved_url'] = info['short_url']
            info['response_code'] = None
            info['domain'] = self.getDomain(info['resolved_url'] )
            info['top_level_domain'] = None
            info['top_level_domain'] = get_tld(info['resolved_url'], fail_silently=True)

            if info['top_level_domain'] == "twitter.com":
                info['is_twitter_url'] = True
            else:
                info['is_twitter_url'] = False
            info['is_media'] = None
            info['is_processed'] = True

        return info

    def download_media(self, url, media_path):
        response = requests.get(url, verify=False)
        media_info = {}
        media_info["response_code"] = response.status_code
        if response.status_code == 200:
            img = Image.open(BytesIO(response.content))
            media_info["hash"] = str(imagehash.dhash(img))
            unique_file_str = media_info["hash"] + "." + img.format
            path = os.path.join(media_path, unique_file_str)

            if not os.path.exists(media_path):
                os.makedirs(media_path)

            img.save(path, quality=100)
            media_info['path'] = path
        else:
            media_info["hash"] = None
            media_info['path'] = None

        return media_info

    def getMediaInformation(self, media_tuple, media_path):
        media_info = {}
        media_info['tweet_id'] = media_tuple[0]
        media_info['media_url'] = media_tuple[1]
        media_info['type'] = media_tuple[2]

        downloaded_media = self.download_media(media_info['media_url'], media_path)

        media_info["hash"] = downloaded_media["hash"]
        media_info['path'] = downloaded_media["path"]
        media_info['response_code'] = downloaded_media["response_code"]

        media_info["is_processed"] = True

        return media_info

    def getICardInformation(self, icard_tuple, media_path):
        icard_info = {}
        icard_info['tweet_id'] = icard_tuple[0]

        response = requests.get("https://twitter.com/i/cards/tfw/v1/%s" % icard_info['tweet_id'])

        if response.ok:
            soup = BeautifulSoup(response.text, 'html.parser')

            if soup.find_all('h2'):
                icard_info["title"] = soup.h2.text
            else:
                icard_info["title"] = None

            if soup.find_all('p'):
                icard_info["text"] = soup.p.text
            else:
                icard_info["text"] = None

            if soup.find_all('img'):
                icard_info["img"] = soup.img.get("data-src")
                image_info = self.download_media(icard_info["img"], media_path)
                icard_info["hash"] = image_info['hash']
                icard_info["path"] = image_info['path']
            else:
                icard_info["img"] = None
                icard_info["hash"] = None
                icard_info["path"] = None

            # response = requests.get(soup.a.get("href"))
            if soup.find_all('a'):
                try:
                    response = self.getURLResponse(soup.a.get("href"))
                    if response != None and isinstance(response, requests.Response):
                        url = response.url
                        icard_info["url"] = url
                        icard_info["response_code"] = response.status_code
                        icard_info['domain'] = self.getDomain(icard_info["url"])
                        icard_info['top_level_domain'] = get_tld(icard_info["url"], fail_silently=True)
                    else:
                        if isinstance(response, str):
                            icard_info["url"] = response
                            icard_info["response_code"] = None
                            icard_info['domain'] = self.getDomain(icard_info["url"])
                            icard_info['top_level_domain'] = get_tld(icard_info["url"], fail_silently=True)
                        else:
                            icard_info["response_code"] = None
                            icard_info["url"] = None
                            icard_info['domain'] = None
                            icard_info['top_level_domain'] = None
                except:
                    icard_info["response_code"] = None
                    icard_info['domain'] = None
                    icard_info['top_level_domain'] = None
                    icard_info["url"] = None
            else:
                icard_info["response_code"] = None
                icard_info['domain'] = None
                icard_info['top_level_domain'] = None
                icard_info["url"] = None

            icard_info["is_processed"] = True
            icard_info["has_icard"] = True

        else:
            icard_info["title"] = None
            icard_info["response_code"] = None
            icard_info["text"] = None
            icard_info["img"] = None
            icard_info["hash"] = None
            icard_info["path"] = None
            icard_info['top_level_domain'] = None
            icard_info['domain'] = None
            icard_info["url"] = None
            icard_info["has_icard"] = False
            icard_info["is_processed"] = True


        return icard_info


    def printProgressBar(self, iteration, total, s_time, prefix='', suffix='', decimals=1, length=100, fill='█'):
        """
        Call in a loop to create terminal progress bar
        @params:
            iteration   - Required  : current iteration (Int)
            total       - Required  : total iterations (Int)
            prefix      - Optional  : prefix string (Str)
            suffix      - Optional  : suffix string (Str)
            decimals    - Optional  : positive number of decimals in percent complete (Int)
            length      - Optional  : character length of bar (Int)
            fill        - Optional  : bar fill character (Str)
        """
        percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
        filledLength = int(length * iteration // total)
        bar = fill * filledLength + '-' * (length - filledLength)
        c_time = time.time() - s_time
        per_minute = iteration / (c_time / 60)
        print('\r%s |%s| %s%% %s \t %0.2f Tweets/min' % (prefix, bar, percent, suffix, per_minute), end='\r')
        # Print New Line on Complete
        if iteration == total:
            print()

