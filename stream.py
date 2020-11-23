import tweepy
import socket
import preprocessor
import re



# Enter your Twitter keys here!!!
ACCESS_TOKEN = "2961986860-DnHgI5bJEwSWikKgI9lTsmT0y9zidjiM4xCES3S"
ACCESS_SECRET = "jBuXhX6ql7c8t9VjFuN5RycE97da4D7GvSzBDhmqEHDud"
CONSUMER_KEY = "ohVk6m7kQ1FldfM6YFDpSRw5Z"
CONSUMER_SECRET = "5h4ja19xAu5P8DmeayOWLd5ZsCWe2ANsbrqanEmn3kl4jydUiE"


auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)


hashtag = '#covid19'

TCP_IP = 'localhost'
TCP_PORT = 9001




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

    #return regrex_pattern.sub('',tweet)
    return preprocessor.clean(tweet)


def getTweet(status):
    
    # You can explore fields/data other than location and the tweet itself. 
    # Check what else you could explore in terms of data inside Status object

    tweet = ""
    location = ""

    location = status.user.location
    
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


