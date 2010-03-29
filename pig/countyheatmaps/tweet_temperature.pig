REGISTER s3://piggybank/0.6.0/piggybank.jar
DEFINE LOWER org.apache.pig.piggybank.evaluation.string.LOWER();

--%default INPUT s3://where20demo/sample-tweets/

-- $ pig -l /mnt -p INPUT=s3://where20demo/sample-tweets/ tweet_temperature.pig

tweets = LOAD '$INPUT' as (
  user_screen_name:chararray, 
  tweet_id:chararray,
  tweet_created_at:chararray, 
  tweet_text:chararray,
  user_id:chararray, 
  user_name:chararray, 
  user_description:chararray, 
  user_profile_image_url:chararray, 
  user_url:chararray,
  user_followers_count:int, 
  user_friends_count:int, 
  user_statuses_count:int, 
  user_location:chararray, 
  user_lang:chararray, 
  user_time_zone:chararray, 
  place_id:chararray, 
  place_name:chararray,
  place_full_name:chararray, 
  place_type:chararray, 
  place_country_code:chararray, 
  place_bounding_box_coordinates:chararray);

filtered_tweets = FILTER tweets BY user_location != 'NULL';
filtered_tweets = FILTER filtered_tweets 
  BY (user_time_zone == 'Central Time (US & Canada)')
  OR (user_time_zone == 'Pacific Time (US & Canada)')
  OR (user_time_zone == 'Eastern Time (US & Canada)')
  OR (user_time_zone == 'Mountain Time (US & Canada)')
  OR (user_time_zone == 'Hawaii')
  OR (user_time_zone == 'Alaska')
  OR (user_time_zone == 'Arizona')
  OR (user_time_zone == 'Indiana (East)');

filtered_tweets = FOREACH filtered_tweets GENERATE user_location, tweet_text;

SPLIT filtered_tweets INTO 
  cold_tweets IF (LOWER(tweet_text) matches '.*cold.*'), 
  warm_tweets IF (LOWER(tweet_text) matches '.*warm.*');

-- join to standardized locations
std_location = LOAD 's3://where20demo/standard_locations.txt' as (
  location:chararray, std_location:chararray, user_count:int, geonameid:int, population:int, fips:chararray);
std_location = FOREACH std_location GENERATE location, fips;

cold_tweets = JOIN std_location BY location, cold_tweets BY LOWER(user_location) using "replicated";
cold_tweets = FOREACH cold_tweets GENERATE $1 as fips, $3 as user_description;

warm_tweets = JOIN std_location BY location, warm_tweets BY user_location using "replicated";
warm_tweets = FOREACH warm_tweets GENERATE $1 as fips, $3 as user_description;

cold_counts = GROUP cold_tweets BY fips;
cold_counts = FOREACH cold_counts GENERATE $0 as fips, SIZE($1) as count;

warm_counts = GROUP warm_tweets BY fips;
warm_counts = FOREACH warm_counts GENERATE $0 as fips, SIZE($1) as count;

rmf warm_counts
STORE warm_counts INTO 'warm_counts';

rmf cold_counts
STORE cold_counts INTO 'cold_counts';
