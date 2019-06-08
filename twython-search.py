#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Sample script to write captured tweets to a CSV file
using Twython library for Python 3, which is available
for install using your package manager of choice:

$ sudo apt install python3-twython

               [or]

$ pip3 install twython

This code is aimed as an example of how to
get tweets using the Twitter REST API with
a Python interface like the Twython library.
'''

from csv import writer, QUOTE_MINIMAL
from datetime import datetime, timezone
from twython import Twython, TwythonError, TwythonRateLimitError
from time import time, sleep

# twitter data mining setup
# list of operators available at:
# https://dev.twitter.com/rest/public/search

QUERY = ""                   # keywords to search
APP_KEY = ""                 # your API key ID
APP_SECRET = ""              # your API key secret
OAUTH_TOKEN = ""             # your API token ID
OAUTH_TOKEN_SECRET = ""      # your API token secret

print('Authenticating...')    
twitter = Twython(APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)

count = 0
max_id = None
previous_results = None

with open('tweets.csv', 'w', newline='', encoding='utf8') as csvfile:

    header = ['text', 'in_reply_to_screen_name', 'user_screen_name', 'id_str', 'user_id_str', 'lang', 'source',
              'user_profile_image_url', 'geo_type', 'latitude', 'longitude', 'created_at', 'timestamp', 'type',
              'retweet_count', 'favorite_count', 'retweeted_id_str', 'in_reply_to_status_id_str', 'user_followers']
    
    file_writer = writer(csvfile, delimiter='|', quoting=QUOTE_MINIMAL)
    file_writer.writerow(header)
    
    print('Collecting...')
    
    while True: # keep searching
        
        try: # collecting

            search_results = twitter.search(q=QUERY,
                                            count=100,
                                            since=None,
                                            until=None,
                                            since_id=None,
                                            max_id=max_id)

            count += len(search_results['statuses'])
            
            if (count/1000).is_integer():
                print('Got', count, 'tweets.')

            if not search_results\
            or search_results == previous_results:
                print('Finished.')
                break

            for status in search_results['statuses']:
                max_id = int(status['id_str']) - 1 # for next search
                status_type = 'Retweet' if status['text'].startswith('RT @')\
                    else ('Reply' if status['text'].startswith('@') else 'Tweet')
                tweet = [status['text'].replace('\n',' ').replace('|',' '),
                         status['in_reply_to_screen_name'],
                         status['user']['screen_name'],
                         status['id_str'],
                         status['user']['id_str'],
                         status['lang'],
                         status['source'],
                         status['user']['profile_image_url'],
                         'Point' if status['coordinates'] else '',
                         status['coordinates']['coordinates'][1] if status['coordinates'] else '',
                         status['coordinates']['coordinates'][0] if status['coordinates'] else '',
                         status['created_at'],
                         int(datetime.strptime(status['created_at'], "%a %b %d %H:%M:%S +0000 %Y").replace(tzinfo=timezone.utc).timestamp()),
                         status_type,
                         status['retweet_count'],
                         status['favorite_count'],
                         status['retweeted_status']['id_str'] if status_type == 'Retweet' else '',
                         status['in_reply_to_status_id_str'] if status_type == 'Reply' else '',
                         status['user']['followers_count']]
                file_writer.writerow(tweet)

            previous_results = search_results

        except TwythonRateLimitError:
            rate_limit_status = twitter.get_application_rate_limit_status(resources='search')
            rate_limit_status = rate_limit_status['resources']['search']['/search/tweets']
            reset = int(rate_limit_status['reset'] - time() + 1)
            if reset > 0:
                print('Sleeping', reset, 'seconds.')
                sleep(reset)

        except KeyboardInterrupt:
            print('Finishing...')
            break

print('Got', count, 'tweets.')