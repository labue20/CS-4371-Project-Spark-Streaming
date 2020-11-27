from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from geopy.geocoders import Nominatim
from textblob import TextBlob
import pickle
import requests
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch
from torrequest import TorRequest

import os
os.environ["PYSPARK_PYTHON"] = "/Library/Frameworks/Python.framework/Versions/3.7/bin/python3.7"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/Library/Frameworks/Python.framework/Versions/3.7/bin/python3.7"




TCP_IP = 'localhost'
TCP_PORT = 8080

geolocator = Nominatim(user_agent="tweet")



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

        


# Pyspark
# create spark configuration
conf = SparkConf()
conf.setAppName('TwitterApp')
conf.setMaster('local[8]')

# create spark context with the above configuration
sc = SparkContext(conf=conf)

# create the Streaming Context from spark context with interval size 4 seconds
ssc = StreamingContext(sc, 4)
ssc.checkpoint("checkpoint_TwitterApp")

# read data from port 9000
dataStream = ssc.socketTextStream(TCP_IP, TCP_PORT)


dataStream.foreachRDD(lambda rdd: rdd.foreach(processTweet))


ssc.start()
ssc.awaitTermination()
