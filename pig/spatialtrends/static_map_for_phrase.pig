-- accepts an input parameter phrase to use in regex
-- outputs # of tweets by county containing that phrase over sample period

-- $ pig -p PHRASE='glenn beck' static_map_for_phrase.pig

REGISTER s3://piggybank/0.6.0/piggybank.jar
DEFINE LOWER org.apache.pig.piggybank.evaluation.string.LOWER();
DEFINE REPLACE org.apache.pig.piggybank.evaluation.string.REPLACE();
DEFINE FILENAME REPLACE('$PHRASE', ' ', '_');

tweet_phrases = LOAD 's3://where20demo/tweet_phrases/'as (
  phrase:chararray, 
  date:chararray, 
  hour:int, 
  fipscode:chararray,
  count:int);

filtered_phrases = tweet_phrases BY (phrase matches '.*$PHRASE.*');

grouped_loc = GROUP filtered_phrases BY fipscode;
fips_counts = FOREACH grouped_loc GENERATE $0 as fipscode, SUM($1.count) as count;
sorted_fips = ORDER fips_counts BY count DESC;

STORE foo as 's3://where20demo/mapdata/$PHRASE.txt'  

-- we can also normalize by population by joining to geonames table:
standard_us_cities = LOAD 's3://where20demo/standard_us_cities.txt' as (
  geonameid:int,
  name:chararray, 
  latitude:float,
  longitude:float,
  country_code:chararray,
  cc2:chararray,
  fipscode:chararray,
  county:chararray,
  population:int,
  countyfips:chararray,
  std_location:chararray,
  std_full_location:chararray);