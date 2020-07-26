import os
import tweepy
import random
from collections import Counter
import sys
import time
import re

english_check = re.compile(r'^[a-zA-Z0-9]*$')

class MyStreamListener(tweepy.StreamListener):

    def __init__(self):
        super(MyStreamListener, self).__init__()
        self.len_of_tweets = 0
        self.tweet_list = []
        self.count_by_tag = {}


    def on_status(self, status):

        hashtags = status.entities.get('hashtags')
        tag_list = []
        for i in hashtags:
            if english_check.match(i['text'][0:]):
                tag_list.append(i['text'][0:])

        if len(tag_list) > 0:
            self.len_of_tweets += 1
            if self.len_of_tweets > 100:
                idx = random.randint(0,self.len_of_tweets)

                if idx < 100:
                    for t in self.tweet_list[idx]:
                        self.count_by_tag[t] -= 1


                    self.tweet_list[idx] = tag_list

                    for t in self.tweet_list[idx]:
                        if t in self.count_by_tag:
                            self.count_by_tag[t] += 1

                        else:
                            self.count_by_tag.setdefault(t,1)

                sorted_dict = sorted(self.count_by_tag.items(), key = lambda x: (-x[1],x[0]))

                with open(out_file,'a+') as f:
                    f.write('The number of tweets with tags from the begining: ' + str(self.len_of_tweets) + '\n')
                    count = 0
                    prev_val = -1
                    for item in sorted_dict:
                        if count > 3:
                            break
                        if item[1] != prev_val:
                            prev_val = item[1]
                            count += 1
                        if count < 4:
                            f.write(str(item[0]) + ' : ' + str(item[1]) + '\n')
                    f.write('\n')
            else:
                self.tweet_list.append(tag_list)
                for i in tag_list:
                    if i in self.count_by_tag:
                        self.count_by_tag[i] += 1

                    else:
                        self.count_by_tag.setdefault(i,1)

                sorted_dict =  sorted(self.count_by_tag.items(), key = lambda x: (-x[1],x[0]))

                with open(out_file,'a+') as f:
                    f.write('The number of tweets with tags from the begining: ' + str(self.len_of_tweets) + '\n')
                    count = 0
                    prev_val = -1
                    for item in sorted_dict:
                        if count > 3:
                            break
                        if item[1] != prev_val:
                            prev_val = item[1]
                            count += 1
                        if count < 4:
                            f.write(str(item[0]) + ' : ' + str(item[1]) + '\n')
                    f.write('\n')

    def on_error(self, status_code):
        if status_code == 420:
            #returning False in on_error disconnects the stream
            return False



API_key = 'Ejtce8FvtzOK9zOKgTUNHxM9V'
API_secret_key = 'jh52ql65oP6eMVCvBZJCPJbYOJxoDKiXkkp5VwHZh81cyR36af'
Access_token = '1252345204281049088-frsRe23vXVIj3MabjEOd5O50sSJtEr'
Access_token_secret = '0Px8pZdebPtjOW3EwLaoa9N6xEun879QFW4DY2qAaTNIW'

auth = tweepy.OAuthHandler(API_key, API_secret_key)
auth.set_access_token(Access_token, Access_token_secret)
twitter_api = tweepy.API(auth)

port_num = sys.argv[1]
out_file = sys.argv[2]
with open(out_file, "w") as f:
    1

myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth=twitter_api.auth, listener=myStreamListener)
myStream.filter(track=["#"],languages=["en"])
