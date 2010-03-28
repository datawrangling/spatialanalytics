-- generate 'locations_timezones': locations, top display string variant, and time zone, sorted by user count
REGISTER s3://piggybank/0.6.0/piggybank.jar
DEFINE LOWER org.apache.pig.piggybank.evaluation.string.LOWER();
DEFINE REPLACE org.apache.pig.piggybank.evaluation.string.REPLACE();

tweets = LOAD 's3://where20/parsed-tweets-feb' as (
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

filtered_tweets = FILTER tweets BY user_location != 'NULL' AND user_time_zone != 'NULL';   
location_time_zone = FOREACH filtered_tweets GENERATE LOWER(user_location) as location, user_time_zone, tweet_id;
grouped_loc_time = GROUP location_time_zone BY (location,user_time_zone);
loc_time = FOREACH grouped_loc_time GENERATE $0.location as location, $0.user_time_zone as user_time_zone, SIZE($1) as freq;
grouped_locs = GROUP loc_time by location;

top_time_zone = FOREACH grouped_locs {
      sorted = ORDER loc_time BY freq DESC;
      sorted = LIMIT sorted 1;
      GENERATE $0 as location, FLATTEN(sorted.user_time_zone) as user_time_zone;}

sorted_counts = LOAD 'global_location_counts' as (location:chararray, user_count:long);
joined_names_zones = JOIN sorted_counts BY location, top_time_zone by location;

locations_with_zones = FOREACH joined_names_zones GENERATE $0 as location, $3 as time_zone, $1 as user_count;
sorted_loc_zone = ORDER locations_with_zones BY user_count DESC;
rmf locations_with_zones
STORE sorted_loc_zone INTO 'locations_with_zones';

-- Find the top display string for each location
-- will help provide turker cotext for abbreviations like "LA"
filtered_tweets = FILTER tweets BY user_location != 'NULL';   
location_displaystring = FOREACH filtered_tweets GENERATE LOWER(user_location) as location, 
  user_location as display_string, tweet_id;
grouped_loc_disp = GROUP location_displaystring BY (location,display_string);
loc_disp = FOREACH grouped_loc_disp GENERATE $0.location as location, $0.display_string as display_string, SIZE($1) as freq;
grouped_locs = GROUP loc_disp by location;


top_display_string = FOREACH grouped_locs {
      sorted = ORDER loc_disp BY freq DESC;
      sorted = LIMIT sorted 1;
      GENERATE $0 as location, FLATTEN(sorted.display_string) as display_string;}
rmf top_display_string      
STORE top_display_string INTO 'top_display_string';

-- Join with time zones & counts
rmf sorted-locations
top_display_string = LOAD 'top_display_string' AS (location:chararray, display_string:chararray);
sorted_loc_zone = LOAD 'locations_with_zones' AS (location:chararray, time_zone:chararray, user_count:long);
joined_names_zones = JOIN top_display_string by location, sorted_loc_zone BY location;

locations_with_zones = FOREACH joined_names_zones 
GENERATE CONCAT(CONCAT('"',$0),'"') as location, 
  CONCAT(CONCAT('"',$1),'"') as display_string, 
  CONCAT(CONCAT('"',$3),'"') as time_zone, 
  $4 as user_count;
sorted_loc_zone = ORDER locations_with_zones BY user_count DESC;
rmf locations_timezones
STORE sorted_loc_zone INTO 'locations_timezones' USING PigStorage(',');