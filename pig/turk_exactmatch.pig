REGISTER s3://piggybank/0.6.0/piggybank.jar
DEFINE LOWER org.apache.pig.piggybank.evaluation.string.LOWER();

-- Exact match of locations to geonameids as determined by mechanical turk
location_counts = LOAD 'us_location_counts' as (location:chararray, user_count:long);

-- this file was produced by location_standardization/parse_turk_responses.py
location_geo_mapping = LOAD 's3://where20/location_geo_mapping.txt' as (location:chararray, geonameid:int);

standard_us_cities = LOAD 's3://where20/standard_us_cities.txt' as (
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
  standard_name:chararray);

standard_us_cities = FOREACH standard_us_cities 
  GENERATE LOWER(standard_name) as city_state, geonameid, population, countyfips;

joined_names = JOIN location_counts BY location, standard_us_cities BY city_state;