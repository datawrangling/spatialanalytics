DEFINE LOWER org.apache.pig.piggybank.evaluation.string.LOWER();
 
tweets = LOAD 's3://where20demo/sample-tweets' as (
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
 
tweets_with_location = FILTER tweets BY user_location != 'NULL';
 
normalized_locations = FOREACH tweets_with_location GENERATE LOWER(user_location) as user_location;

grouped_tweets = GROUP normalized_locations BY user_location PARALLEL 10;

location_counts = FOREACH grouped_tweets GENERATE $0 as location, SIZE($1) as user_count;

sorted_counts = ORDER location_counts BY user_count DESC;

STORE sorted_counts INTO 'global_location_tweets';


