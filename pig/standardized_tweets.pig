-- standardization approach (data limited to US)
-- 1) mechanical turk mappings
-- 2) city, state abbrev. exact match
-- 3) exact match to most populated city name

--- merge location name mapping files into a single file

REGISTER s3://piggybank/0.6.0/piggybank.jar
DEFINE LOWER org.apache.pig.piggybank.evaluation.string.LOWER();
DEFINE REPLACE org.apache.pig.piggybank.evaluation.string.REPLACE();

city_counts = LOAD 'city_counts' as (location:chararray, std_location:chararray, user_count:int, geonameid:int, population:int, fips:chararray);
city_state_counts = LOAD 'city_state_counts' as (location:chararray, std_location:chararray, user_count:int, geonameid:int, population:int, fips:chararray);
-- Assign location geonameid based on US exact matches if available:
std_locations = UNION city_counts, city_state_counts;

--If turk location is not in std_locations, then merge
turk_counts = LOAD 'turk_counts' as (location:chararray, std_location:chararray, user_count:int, geonameid:int, population:int, fips:chararray);

cogrouped_locs = cogroup std_locations by location, turk_counts by location;
-- find locations where count of std_locations is 0
new_turk_locs = filter cogrouped_locs by COUNT(std_locations) == 0;
new_std_locations = foreach new_turk_locs generate FLATTEN(turk_counts);
std_locations = UNION std_locations, new_std_locations;

rmf standard_locations
sorted_standard_locations = ORDER std_locations BY population desc;
store sorted_standard_locations into 'standard_locations';


std_location = LOAD 'standard_locations' as (
  location:chararray, std_location:chararray, user_count:int, geonameid:int, population:int, fips:chararray);

grouped_loc = GROUP std_location BY fips;
fips_counts = FOREACH grouped_loc GENERATE $0 as fips, SUM($1.user_count) as user_count, SUM($1.population) as population;
sorted_fips = ORDER fips_counts BY user_count DESC;
rmf county_counts
STORE sorted_fips INTO 'county_counts';

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
  
filtered_tweets = FOREACH filtered_tweets
 GENERATE user_screen_name,
  tweet_id,
  tweet_created_at,
  tweet_text,
  user_name,
  user_description,
  user_profile_image_url,
  user_url,
  user_followers_count,
  user_friends_count,
  user_statuses_count,
  user_location);
  
-- TODO: we need to standardize tweet time & emit fields for date, hour

-- Standardize locations in tweets
std_location = LOAD 'standard_locations' as (
  location:chararray, std_location:chararray, user_count:int, geonameid:int, population:int, fips:chararray);
  
std_location = FOREACH std_location GENERATE location, std_location, geonameid, fips;    

-- use replicated join, since geoid/ location relations are small enough to fit into main memory.
standardized_tweets = join filtered_tweets by LOWER(user_location), std_locations by location using "replicated";
standardized_tweets = FOREACH standardized_tweets GENERATE 
   $0 as tweet_id,
   $1 as tweet_created_at,
   $2 as tweet_text,
   $3 as user_name,
   $4 as user_description,
   $5 as user_profile_image_url,
   $6 as user_url,
   $7 as user_followers_count,
   $8 as user_friends_count,
   $9 as user_statuses_count,
   $11 as location, 
   $12 as std_location, 
   $13 as geonameid, 
   $14 as fips);
   
rmf standardized_tweets
store standardized_tweets into 'standardized_tweets';
