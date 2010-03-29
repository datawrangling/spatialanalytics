REGISTER s3://piggybank/0.6.0/piggybank.jar
DEFINE LOWER org.apache.pig.piggybank.evaluation.string.LOWER();

-- Exact match of locations to geonameids as determined by mechanical turk
location_counts = LOAD 'us_location_counts' as (location:chararray, user_count:long);

-- this file was produced by location_standardization/parse_turk_responses.py
location_geo_mapping = LOAD 's3://where20demo/location_geo_mapping.txt' as (location:chararray, geonameid:int);

joined_names = JOIN location_counts BY location, location_geo_mapping BY location using "replicated";
-- location:chararray, user_count:long, location:chararray, geonameid:int
joined_names = FOREACH joined_names GENERATE 
  $0 as location, 
  $1 as user_count, 
  $3 as geonameid;

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
    
--join geo mapping to standard_us_cities to get name, geonameid, population, countyfips  

standard_us_cities = FOREACH standard_us_cities GENERATE 
  name, 
  std_location, 
  geonameid, 
  population, 
  countyfips;  
  
joined_turk_results = JOIN standard_us_cities BY geonameid, joined_names BY geonameid using "replicated";
-- geonameid, population, countyfips, location, user_count, geonameid;

--Desired output:  
--turk_counts = LOAD 'turk_counts' as (
--  location:chararray, 
--  std_location:chararray, 
--  user_count:int, 
--  geonameid:int, 
--  population:int, 
--  fips:chararray);  


turk_counts = FOREACH joined_turk_results GENERATE 
  $5 as location, 
  $1 as std_location, 
  $6 as user_count,
  $2 as geonameid,  
  $3 as population,
  $4 as fips;
  
rmf turk_counts
sorted_turk_counts = ORDER turk_counts BY population DESC;
store sorted_turk_counts into 'turk_counts';  

-- turk_counts output is in the following format:
-- [location, user_count, geonameid, population, fips]


