-- accepts an input parameter phrase to use in regex
-- outputs # of tweets by county containing that phrase over sample period

morning = filter tweet_phrases by 
  (phrase == 'good morning');

morning_group = GROUP morning BY (hour, fipscode);
morning_counts = FOREACH morning_group GENERATE
  $0.hour, $0.fipscode, SUM($1.count) as count;

tweet_phrases = LOAD 's3://where20demo/tweet_phrases/'as (
  phrase:chararray, 
  date:chararray, 
  hour:int, 
  fipscode:chararray,
  count:int);
  
STORE foo as 's3://where20demo/mapdata/foo.txt'  

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