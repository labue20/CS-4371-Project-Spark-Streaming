import tweepy
import socket
import preprocessor
import re
import os
from geopy.geocoders import Nominatim
from elasticsearch import Elasticsearch
from textblob import TextBlob

os.environ["PYSPARK_PYTHON"] = "/Library/Frameworks/Python.framework/Versions/3.7/bin/python3.7"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/Library/Frameworks/Python.framework/Versions/3.7/bin/python3.7"



# Enter your Twitter keys here!!!
ACCESS_TOKEN = "2961986860-Nw26zlEUvzyyaXGfQeugwu8Imhi1HE4qAGoqsEI"
ACCESS_SECRET = "lCYngjmhWqmGMkz0ieqQRT4JvRywXygwdwzVHKkrQO0fN"
CONSUMER_KEY = "ohVk6m7kQ1FldfM6YFDpSRw5Z"
CONSUMER_SECRET = "5h4ja19xAu5P8DmeayOWLd5ZsCWe2ANsbrqanEmn3kl4jydUiE"


auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)


hashtag = '#covid19'

TCP_IP = 'localhost'
TCP_PORT = 8080


geolocator = Nominatim(user_agent="tweet")



def preprocessing(tweet):
    
    # Add here your code to preprocess the tweets and  
    # remove Emoji patterns, emoticons, symbols & pictographs, transport & map symbols, flags (iOS), etc
    regrex_pattern = re.compile(pattern = "["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           "]+", flags = re.UNICODE) 
    tweet = re.sub(r'[^x00-\x7F]+',' ',tweet)
    tweet = regrex_pattern.sub(r'', tweet)

    return regrex_pattern.sub('',tweet)
    #return preprocessor.clean(tweet)


def getTweet(status):
    
    # You can explore fields/data other than location and the tweet itself. 
    # Check what else you could explore in terms of data inside Status object

    tweet = ""
    location = ""

    location = status.user.location


    elastic_search = Elasticsearch([{'host':'localhost', 'port':9200}])
    
    if hasattr(status, "retweeted_status"):  # Check if Retweet
        try:
            tweet = status.retweeted_status.extended_tweet["full_text"]
        except AttributeError:
            tweet = status.retweeted_status.text
    else:
        try:
            tweet = status.extended_tweet["full_text"]
        except AttributeError:
            tweet = status.text
   

    

    return location, preprocessing(tweet)

'''
def processTweet(tweet):

    # Here, you should implement:
    # (i) Sentiment analysis,

    # (ii) Get data corresponding to place where the tweet was generate (using geopy or googlemaps)
    # (iii) Index the data using Elastic Search 

    elastic_search = Elasticsearch([{'host':'localhost', 'port':9200}])

    tweetData = tweet.split("::")

    if len(tweetData) > 1:
        
        text = tweetData[1]
        rawLocation = tweetData[0]

        # (i) Apply Sentiment analysis in "text"
        analysis = TextBlob(tweet)
        if analysis.sentiment.polarity > 0:
                 sentiment = "positive"
        elif analysis.sentiment.polarity == 0:
                sentiment = "positive"
        else:
                sentiment = "positive"
        

	# (ii) Get geolocation (state, country, lat, lon, etc...) from rawLocation
        try:
                location = geolocator.geocoders(tweetData[0],addressdetails=True)
                lat = location.rawLocation['lat']
                lon = location.rawLocation['lon']
                state = location.rawLocation['address']['state']
                country = location.rawLocation['address']['country']
        except:
                lat = lon = state = country=None


        print("\n\n=========================\ntweet: ", tweet)
        print("Raw location from tweet status: ", rawLocation)
        print("lat: ", lat)
        print("lon: ", lon)
        print("state: ", state)
        print("country: ", country)
        print("Text: ", text)
        print("Sentiment: ", sentiment)



        # (iii) Post the index on ElasticSearch or log your data in some other way (you are always free!!)

        if lat != None and lon != None and sentiment != None:
               eslastic_doc = {"lat": lat, "lon": lon, "state":state,"country":country,"sentiment":sentiment} 
               elastic_search.index(index='tweet-sentiment', doc_type='default', body= eslastic_doc)
               elastic_search.index

        

'''

# create sockets
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
conn, addr = s.accept()

class MyStreamListener(tweepy.StreamListener):

    def on_status(self, status):
        location, tweet = getTweet(status)

        if (location != None and tweet != None):
            tweetLocation = location + "::" + tweet+"\n"
            print(status.text)
            conn.send(tweetLocation.encode('utf-8'))

        return True


    def on_error(self, status_code):
        if status_code == 420:
            return False
        else:
            print(status_code)

myStream = tweepy.Stream(auth=auth, listener=MyStreamListener())
myStream.filter(track=[hashtag], languages=["en"])


