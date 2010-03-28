REGISTER s3://piggybank/0.6.0/piggybank.jar
DEFINE LOWER org.apache.pig.piggybank.evaluation.string.LOWER();
DEFINE REPLACE org.apache.pig.piggybank.evaluation.string.REPLACE();

-- You can replace '$INPUT' with 's3://where20/sample-tweets-*'
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
filtered_tweets = FILTER filtered_tweets BY (user_time_zone == 'Central Time (US & Canada)')
  OR (user_time_zone == 'Pacific Time (US & Canada)')
  OR (user_time_zone == 'Eastern Time (US & Canada)')
  OR (user_time_zone == 'Mountain Time (US & Canada)')
  OR (user_time_zone == 'Hawaii')
  OR (user_time_zone == 'Alaska')
  OR (user_time_zone == 'Arizona')
  OR (user_time_zone == 'Indiana (East)');

filtered_tweets = FOREACH filtered_tweets GENERATE user_location, user_id;
distinct_location_users = DISTINCT filtered_tweets PARALLEL 70;
lower_locations = FOREACH distinct_location_users GENERATE LOWER(user_location) as user_location, user_id;
grouped_tweets = GROUP lower_locations BY user_location PARALLEL 70;
location_counts = FOREACH grouped_tweets GENERATE $0 as location, SIZE($1) as user_count;
sorted_counts = ORDER location_counts BY user_count DESC;
rmf us_location_counts
STORE sorted_counts INTO 'us_location_counts';  