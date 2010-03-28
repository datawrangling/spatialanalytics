#!/usr/bin/env python
# encoding: utf-8
"""
parse_stream.py

Parses Twitter streaming API JSON tweets into un-nested tab delimited format

TODO: reverse geocode iPhone: lat lon pairs in location field to nearest city

ÜT: -23.639481,-46.610946	1
ÜT: 18.494521,-69.936517	1
ÜT: 34.096677,-118.100306	1
iPhone: 36.866623,-76.176041	1
ÜT: 40.654919,-73.746026	1
iPhone: 39.949680,-75.143860	1

6.41 percent have explicit lat lon...
0.69 percent have bounding boxes

21 percent have no location at all
Around 7 percent have explicit geo latlon or bounding box
Of remaining 72 percent, how many are real locations we can standardize?

Try exact match of lower case on city, state, country combinations.... see how far that gets us.

Handle unicode
# export LC_ALL=en_US.UTF-8; cat tweets.2010-03-21 | python ./parse_stream.py > locations.txt
# export LC_ALL=en_US.UTF-8; cat locations.txt | sort | uniq -c | sort -nr > ranked_locations.txt
# export LC_ALL=en_US.UTF-8; cat ranked_locations.txt | head -101 | tail -100 > top_100_twitter_locations.txt 
# file -bi top_100_twitter_locations.txt 
text/plain; charset=utf-8

Fields:
user_screen_name, tweet_id, tweet_created_at, tweet_text,
  user_id, user_name, user_description, user_profile_image_url, user_url,
  user_followers_count, user_friends_count, user_statuses_count, 
  user_location, user_lang, user_time_zone, place_id, place_name,
  place_full_name, place_type, place_country_code, place_bounding_box_coordinates
  

Example:
steve_arnett	10850933009	Mon Mar 22 02:23:53 +0000 2010	Other than issues with tickets & a long line, Steamfest 2010 was one of the most relaxing days of my life. Snagged 5 of 6 geocaches too!	17638525	Steve Arnett	Hi. I'm Steve. I'm a photographer in the bay area.	http://a3.twimg.com/profile_images/402082941/Copy_of_sf-steve-rachel-0055_normal.jpg	http://www.SteveArnett.com	137	171437	Pleasant Hill, CA	en	Pacific Time (US & Canada)	d70cebab5f549266	Pleasant Hill	Pleasant Hill, CA	city	US	[[[-122.104417, 37.925260000000002], [-122.049491, 37.925260000000002], [-122.049491, 37.982315999999997], [-122.104417, 37.982315999999997]]]


Created by Peter Skomoroch on 2010-03-21.
Copyright (c) 2010 Data Wrangling LLC. All rights reserved.
"""

import sys
import os
import simplejson

def clean(x):
  if x is None:
    return "NULL"
  if type(x) == type(' '):    
    x = x.replace('\t', ' ').replace('\n',' ')
  elif type(x) == type(1):
    x = str(x)
  return x.strip()

def main():
  for line in sys.stdin: 
    try:
      # tweet level info
      tweet_id = "NULL" #d70cebab5f549266
      tweet_created_at = "NULL" #"Mon Mar 22 02:23:53 +0000 2010",
      tweet_text = "NULL" # "hello world!"
      # user information
      # tweet['user']
      user_id = "NULL" #17638525,    
      user_screen_name = "NULL" #steve_arnett",
      user_name = "NULL" #:"Steve Arnett",    
      user_description = "NULL" #"Hi. I'm Steve. I'm a photographer in the bay area.",
      user_profile_image_url = "NULL" #:"http://a3.twimg.com/profile_images/402082941/Copy_of_sf-steve-rachel-0055_normal.jpg",
      user_url = "NULL" #:"http://www.SteveArnett.com",
      user_followers_count = "0" #137,
      user_friends_count = "0" #172,    
      user_statuses_count = "0" #1437,    
      user_geo_enabled = "NULL" #true,
      user_location = "NULL" #"Pleasant Hill, CA",    
      user_lang = "NULL" #en,
      user_time_zone = "NULL" #:"Pacific Time (US & Canada)",
      # place information available infrequently
      # tweet['place']
      place_id = "NULL"
      place_name = "NULL"
      place_full_name = "NULL"
      place_type = "NULL"
      place_country_code = "NULL"
      # tweet['place']['bounding_box']    
      place_bounding_box_coordinates = "NULL"    

      # try:
      tweet = simplejson.loads(line.strip())   

      if tweet.has_key('id'):
        tweet_id = str(tweet['id']) #d70cebab5f549266
        tweet_created_at = tweet['created_at'] #"Mon Mar 22 02:23:53 +0000 2010",
        tweet_text = clean(tweet['text']) # "hello world!"

      if tweet.has_key('user'):
        user = tweet['user']
        user_screen_name = user['screen_name']
        user_id = str(user['id'])  
        user_name = clean(user['name'])  
        if user.has_key('description'):  
          user_description = clean(user['description']) 
        if user.has_key('profile_image_url'):  
          user_profile_image_url = clean(user['profile_image_url'])
        if user.has_key('url'):    
          user_url = clean(user['url'])
        user_followers_count = clean(user['followers_count'])
        user_friends_count = clean(user['friends_count']) 
        user_statuses_count = clean(user['statuses_count'])
        if user.has_key('geo_enabled'):           
          user_geo_enabled = user['geo_enabled']
        if user.has_key('url'):       
          user_location = clean(user['location'])
        if user.has_key('lang'):  
          user_lang = clean(user['lang'])
        if user.has_key('time_zone'): 
          user_time_zone = clean(user['time_zone'])

      if tweet.has_key('place'): 
        place = tweet['place'] 
        if place is not None:
          place_id = str(place['id'])
          place_name = clean(place['name'])
          place_full_name = clean(place['full_name'])
          place_type = str(place['place_type'])
          place_country_code = str(place['country_code'])
          # tweet['place']['bounding_box']    
          place_bounding_box_coordinates = str(place['bounding_box']['coordinates'])      

      data = [user_screen_name, tweet_id, tweet_created_at, tweet_text,
        user_id, user_name, user_description, user_profile_image_url, user_url,
        user_followers_count, user_friends_count, user_statuses_count, 
        user_location, user_lang, user_time_zone, place_id, place_name,
        place_full_name, place_type, place_country_code, place_bounding_box_coordinates]

      data = [x.replace('\t', ' ') for x in data] 

      if len(data) == 21:
        output = ("\t".join(data)).replace('\n',' ').encode('utf8')
        print output      
      
    except:
      pass  
         



if __name__ == '__main__':
  main()

