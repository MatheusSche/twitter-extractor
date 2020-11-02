import tweepy
import re
from datetime import datetime
from textblob import TextBlob
import emoji
from unicodedata import normalize

#keyword = ("'Incêndios no Pantanal' OR 'Incêndios na Amazonia' OR 'SOSPantanal' OR 'SOSAmazonia'" + "-filter:retweets")
keyword = ("'Pantanal' OR 'Amazonia'" + "-filter:retweets")
COUNT = 50000
CHUNK = 500

class TweetAnalyzer():

    def __init__(self, consumer_key, consumer_secret, access_token, access_token_secret):
        '''
            Conectar com o tweepy
        '''
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)

        self.conToken = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True, retry_count=5, retry_delay=10)

    def give_emoji_free_text(self, text):
        return emoji.get_emoji_regexp().sub(r'', text)

    def remover_acentos(self, txt):
        return normalize('NFKD', txt).encode('ASCII', 'ignore').decode('ASCII')

    def __clean_tweet(self, tweets_text):
        '''
        Tweet cleansing.
        '''

        clean_text = re.sub(r'RT+', '', tweets_text)
        clean_text = re.sub(r'@\S+', '', clean_text)
        clean_text = re.sub(r'https?\S+', '', clean_text)
        clean_text = clean_text.replace("\n", " ")
        return clean_text

    def search_by_keyword(self, keyword, result_type='mixed', tweet_mode='extended'):
        '''
        Search for the twitters thar has commented the keyword subject.
        '''
        tweets_iter = tweepy.Cursor(self.conToken.search,
                          q=keyword, tweet_mode=tweet_mode,
                          rpp=COUNT, result_type=result_type,
                          lang='pt',
                          include_entities=True).items(COUNT)

        return tweets_iter


    def store_data_to_file(self, data):
        tweetText = str(data['TweetText'])
        tweetText = tweetText.replace(';', ' ')

        formated_data = str(data['len']) + ';' + str(data['ID']) + ';' + str(data['User']) + \
                        ';' + str(data['UserLocation']) + ';' + tweetText + \
                        ';' + str(data['Date']) + ';' + str(data['Likes']) + '; ' + str(data['Hashtags']) + '; '
        formated_data = self.remover_acentos(formated_data)
        string_size = len(formated_data)
        formated_data += '0' * (CHUNK-string_size)
        formated_data += ';'+'\n'


        with open('twitter-data.bin', 'ab') as fileT:
            formated_data = formated_data.encode('utf-8')
            fileT.write(formated_data)


    def prepare_tweets_list(self, tweets_iter):
        '''
        Transforming the data to DataFrame.
        '''

        for tweet in tweets_iter:
            if not 'retweeted_status' in dir(tweet):
                hashtags = ''
                if len(tweet.entities['hashtags']) > 0:
                    for h in tweet.entities['hashtags']:
                        hashtags += '#' + h['text'] + ' '
                tweet_text = self.__clean_tweet(tweet.full_text)
                tweets_data = {
                    'len' : len(tweet_text),
                    'ID' : tweet.id,
                    'User' : tweet.user.screen_name,
                    'UserLocation' : tweet.user.location,
                    'TweetText' : tweet_text,
                    'Hashtags': hashtags,
                    'Date' : tweet.created_at,
                    'Likes' : tweet.favorite_count
                }

                self.store_data_to_file(tweets_data)

    def sentiment_polarity(self, tweets_text_list):
        tweets_sentiments_list = []

        for tweet in tweets_text_list:
            polarity = TextBlob(tweet).sentiment.polarity
            if polarity > 0:
                tweets_sentiments_list.append('Positive')
            elif polarity < 0:
                tweets_sentiments_list.append('Negative')
            else:
                tweets_sentiments_list.append('Neutral')

        return tweets_sentiments_list

if __name__ == '__main__':
    with open('twitter-tokens.txt', 'r') as credentials:
        CONSUMER_KEY = credentials.readline().strip('\n')
        CONSUMER_SECRET = credentials.readline().strip('\n')
        ACCESS_TOKEN = credentials.readline().strip('\n')
        ACCESS_TOKEN_SECRET = credentials.readline().strip('\n')


    TwitterExtractor = TweetAnalyzer(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    tweets_iter = TwitterExtractor.search_by_keyword(keyword=keyword)
    TwitterExtractor.prepare_tweets_list(tweets_iter=tweets_iter)





