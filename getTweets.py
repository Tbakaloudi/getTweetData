import os

import nltk
import json
import time
nltk.download('stopwords')
import tweepy
import re
import string
import pandas as pd
import mysql.connector
from mysql.connector import Error
from textblob import TextBlob
from textblob.sentiments import NaiveBayesAnalyzer
import sys
import plotly.express as px

consumer_key = 'Ah9MbHXIgU3edCgjCGvXqw0a4'
consumer_secret = 'pJhqqRAu1WoQjGA3CxoQHXZamlTy7btFNgghl8ynPherZd3T62'
access_token = '1345106360686567426-l2DakBZoKF0Xvq0pLcJjIyznvqbm8H'
access_token_secret = '68OphkpAKEeU82JObNqwkQNX26J0k4t8ef0AxcbrKXwnC'

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

placesUsa = api.geo_search(query="USA",granularity="country")
placeUsa_id = placesUsa[0].id

import datetime

create_unprocessed_table_fg = True
create_processed_table_fg = True

search_words = ["#Bridgerton", "#TheQueensGambit", "#CobraKai"]
stopword = nltk.corpus.stopwords.words('english')

# The size of each step in days
day_delta = datetime.timedelta(days=1)

start_date_start = datetime.datetime(2021, 2, 5)

end_date_start = start_date_start + 7*day_delta

create_unprocessed_table_sql = "CREATE TABLE unprocessed_tweets (id int(11) NOT NULL AUTO_INCREMENT,category varchar(128) NOT NULL,tweet_id int(11) NOT NULL, tweet_json JSON NOT NULL,text text DEFAULT NULL,screen_name varchar(128) DEFAULT NULL,followers int(11) DEFAULT NULL,account_tweets int(11) DEFAULT NULL,account_retweets int(11) DEFAULT NULL,created_at DATE NOT NULL,location varchar(100) DEFAULT NULL,hashtags varchar(10000) DEFAULT NULL,polarity varchar(15) DEFAULT NULL, PRIMARY KEY (id)"
create_processed_table_sql = "CREATE TABLE processed_tweets (id int(11) NOT NULL AUTO_INCREMENT,unprocessed_id int(11) NOT NULL, text_punct VARCHAR(1000) DEFAULT NULL,text_tokenized VARCHAR(1000) DEFAULT NULL,text_stop VARCHAR(1000) DEFAULT NULL,PRIMARY KEY (id))"

def clean_tweet_text (text):
    re.sub('@[^\s]+','',text)
    re.sub('http[^\s]+','',text)
    text.lower()
    text.strip()
    text.translate(str.maketrans('', '', string.punctuation))
    #deEmojify(text)
    return text

def deEmojify(text):
    regrex_pattern = re.compile(pattern = "["
                                          u"\U0001F600-\U0001F64F"  # emoticons
                                          u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                                          u"\U0001F680-\U0001F6FF"  # transport & map symbols
                                          u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                                          "]+", flags = re.UNICODE)
    return regrex_pattern.sub(r'',text)

def remove_punct(text):
    text  = "".join([char for char in text if char not in string.punctuation])
    text = re.sub('[0-9]+', '', text)
    return text

def tokenization(text):
    text = re.split('\W+', text)
    return text

def remove_stopwords(text):
    text = [word for word in text if word not in stopword]
    return text

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="tweets"
)

mycursor = mydb.cursor(buffered = True)


def create_table_fn (sql_script):
    create_sql = sql_script

    mycursor.execute(create_sql)
    mydb.commit()

mycursor.execute("SHOW TABLES")

for x in mycursor:
    print(x)
    if x: 'unprocessed_tweets'
    create_unprocessed_table_fg = False
    if x: 'processed_tweets'
    create_processed_table_fg = False

if create_unprocessed_table_fg:
    create_table_fn(create_unprocessed_table_sql)

if create_processed_table_fg:
    create_table_fn(create_processed_table_sql)

for i in range((end_date_start - start_date_start).days):
    search_date_start = (start_date_start).strptime(str(start_date_start+i*day_delta), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
    search_date_end = (start_date_start).strptime(str(start_date_start+i*day_delta+day_delta), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')


    for search_word in search_words:
        search_query = search_word + " lang:en profile_country:US"
        print(search_query)
        try:
            tweets = tweepy.Cursor(api.search, q=search_word, lang="en",
                                   since=search_date_start, until=search_date_end, tweet_mode='extended').items(250)

            for tweet in tweets:
                tweet_json = json.dumps(tweet._json)
                tweet_id = tweet.id
                text = tweet.full_text
                username = tweet.user.screen_name.encode('utf8')
                followers = int(tweet.user.followers_count)
                totaltweets = int(tweet.user.statuses_count)
                retweetcount = int(tweet.retweet_count)
                date = tweet.created_at
                location = tweet.user.location
                hashtags = str(tweet.entities['hashtags'])

                analysis = TextBlob(tweet.text)
                if analysis.sentiment > 0:
                    pol = "positive"
                elif analysis.sentiment < 0:
                    pol = "negative"
                else:
                    pol = "neural"


                sql = "INSERT INTO unprocessed_tweets (category,tweet_id, tweet_json, text,screen_name,followers,account_tweets,account_retweets,created_at,location,hashtags,polarity) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

                values = (search_word,tweet_id,tweet_json, text, username, followers, totaltweets, retweetcount, date, location, hashtags, pol)
                mycursor.execute(sql, values)

                mydb.commit()

                print(mycursor.rowcount, "record inserted.")
        except tweepy.TweepError:
            time.sleep(60 * 15)
            continue
        except StopIteration:
            break

