REGISTER s3://piggybank/0.6.0/piggybank.jar
DEFINE LOWER org.apache.pig.piggybank.evaluation.string.LOWER();

-- Exact match approach 1
-- try direct mapping of "city name, state abbrev." to lower case tweet lcoation string
location_counts = LOAD 'us_location_counts' as (location:chararray, user_count:long);

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
city_state_counts = FOREACH joined_names GENERATE $0 as location,
  $1 as user_count, $3 as geonameid, $4 as population, $5 as fips;

rmf city_state_counts
sorted_city_state_counts = ORDER city_state_counts BY population DESC;
store sorted_city_state_counts into 'city_state_counts';
  
