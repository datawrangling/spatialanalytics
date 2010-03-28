REGISTER s3://piggybank/0.6.0/piggybank.jar
DEFINE LOWER org.apache.pig.piggybank.evaluation.string.LOWER();
DEFINE REPLACE org.apache.pig.piggybank.evaluation.string.REPLACE();


-- Exact match approach 1
-- try direct mapping of "city name, state abbrev." to lower case tweet lcoation string
location_counts = LOAD 'us_location_counts' as 
  (location:chararray, user_count:long);

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
  std_location:chararray,
  std_full_location:chararray);
  
--Desired output:  
--city_state_counts = LOAD 'city_state_counts' as (
--  location:chararray, 
--  std_location:chararray, 
--  user_count:int, 
--  geonameid:int, 
--  population:int, 
--  fips:chararray);  
    
standard_us_cities = FOREACH standard_us_cities GENERATE 
  LOWER(std_full_location) as city_state, 
  std_location, 
  geonameid, 
  population, 
  countyfips;

standard_us_cities_wo_space = FOREACH standard_us_cities GENERATE 
  REPLACE(city_state, ', ', ',') as city_state, 
  std_location, 
  geonameid, 
  population, 
  countyfips;  
  
standard_us_cities_wo_comma = FOREACH standard_us_cities GENERATE 
  REPLACE(REPLACE(city_state, ',', ' '), '  ', ' ') as city_state,
  std_location, 
  geonameid, 
  population, 
  countyfips;  
  
standard_us_cities = UNION standard_us_cities, 
  standard_us_cities_wo_space, standard_us_cities_wo_comma;   

joined_names = JOIN location_counts BY location, standard_us_cities BY city_state;

city_state_counts = FOREACH joined_names GENERATE 
  $0 as location, 
  $3 as std_location,  
  $1 as user_count, 
  $4 as geonameid,
  $5 as population,
  $6 as fips;
  
--------------  
  
standard_us_cities_abbrev = FOREACH standard_us_cities GENERATE 
  LOWER(std_location) as city_state, 
  std_location, 
  geonameid, 
  population, 
  countyfips;

standard_us_cities_abbrev_wo_space = FOREACH standard_us_cities_abbrev GENERATE 
  REPLACE(city_state, ', ', ',') as city_state, 
  std_location, 
  geonameid, 
  population, 
  countyfips;  

standard_us_cities_abbrev_wo_comma = FOREACH standard_us_cities_abbrev GENERATE 
  REPLACE(REPLACE(city_state, ',', ' '), '  ', ' ') as city_state,
  std_location, 
  geonameid, 
  population, 
  countyfips;  

standard_us_cities_abbrev = UNION standard_us_cities_abbrev, 
  standard_us_cities_abbrev_wo_space, standard_us_cities_abbrev_wo_comma;  
  
joined_abbrev_names = JOIN location_counts BY location, standard_us_cities_abbrev BY city_state;

city_state_abbrev_counts = FOREACH joined_abbrev_names GENERATE 
  $0 as location,
  $3 as std_location,  
  $1 as user_count,
  $4 as geonameid, 
  $5 as population, 
  $6 as fips;
  
--------------------
  
all_city_state_counts = UNION city_state_abbrev_counts, city_state_counts;
final_city_state_counts = DISTINCT all_city_state_counts;

rmf city_state_counts
sorted_city_state_counts = ORDER final_city_state_counts BY population DESC;
store sorted_city_state_counts into 'city_state_counts';
  
