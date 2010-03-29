REGISTER s3://piggybank/0.6.0/piggybank.jar
DEFINE LOWER org.apache.pig.piggybank.evaluation.string.LOWER();
DEFINE REPLACE org.apache.pig.piggybank.evaluation.string.REPLACE();

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
filtered_tweets = FILTER filtered_tweets BY 
  (user_time_zone == 'Central Time (US & Canada)')
  OR (user_time_zone == 'Pacific Time (US & Canada)')
  OR (user_time_zone == 'Eastern Time (US & Canada)')
  OR (user_time_zone == 'Mountain Time (US & Canada)')
  OR (user_time_zone == 'Hawaii')
  OR (user_time_zone == 'Alaska')
  OR (user_time_zone == 'Arizona')
  OR (user_time_zone == 'Indiana (East)');
  
filtered_tweets = FOREACH filtered_tweets
 GENERATE tweet_text, user_location, tweet_created_at;

-- Standardize locations in tweets
std_locations = LOAD 's3://where20demo/standard_locations.txt' as (
  location:chararray, 
  std_location:chararray, 
  user_count:int, 
  geonameid:int, 
  population:int, 
  fips:chararray);
  
std_locations = FOREACH std_location GENERATE location, fips, geonameid;

-- use replicated join, since geoid/ location relations are small enough to fit into main memory.
std_location_tweets = join filtered_tweets by 
  LOWER(user_location), std_locations by location using "replicated";

std_location_tweets = FOREACH std_location_tweets GENERATE
$4 as fips, 
$5 as geonameid,
$2 as tweet_created_at,
$0 as tweet_text;

DEFINE tweet_tokenizer `tweet_tokenizer.py`
  SHIP ('tweet_tokenizer.py', 'nltkandyaml.mod', 's3://where20demo/wikiphrases.pkl');
tweet_ngrams = STREAM std_location_tweets THROUGH tweet_tokenizer
  AS (ngram:chararray, fipscode:chararray, geonameid:int, date:int, hour:int);
   
rmf tweet_ngrams
store tweet_ngrams into 'tweet_ngrams';



