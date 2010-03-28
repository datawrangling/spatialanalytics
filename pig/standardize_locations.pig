-- standardization approach (data limited to US)
-- 1) mechanical turk mappings
-- 2) city, state abbrev. exact match
-- 3) exact match to most populated city name

--- merge location name mapping files into a single file

REGISTER s3://piggybank/0.6.0/piggybank.jar
DEFINE LOWER org.apache.pig.piggybank.evaluation.string.LOWER();
DEFINE REPLACE org.apache.pig.piggybank.evaluation.string.REPLACE();

city_counts = LOAD 'city_counts' as (
  location:chararray, 
  std_location:chararray, 
  user_count:int, 
  geonameid:int, 
  population:int, 
  fips:chararray);

city_state_counts = LOAD 'city_state_counts' as (
  location:chararray, 
  std_location:chararray, 
  user_count:int, 
  geonameid:int, 
  population:int, 
  fips:chararray);
  
-- Assign location geonameid based on US exact matches if available:
std_locations = UNION city_counts, city_state_counts;

--If turk location is not in std_locations, then merge
turk_counts = LOAD 'turk_counts' as (
  location:chararray, 
  std_location:chararray, 
  user_count:int, 
  geonameid:int, 
  population:int, 
  fips:chararray);

cogrouped_locs = cogroup std_locations by location, turk_counts by location;
-- find locations where count of std_locations is 0
new_turk_locs = filter cogrouped_locs by COUNT(std_locations) == 0;
new_std_locations = foreach new_turk_locs generate FLATTEN(turk_counts);
std_locations = UNION std_locations, new_std_locations;

--- Need to remove Country/State names as std city locations:
blacklist_states = LOAD 's3://where20/blacklist_states.txt' as (location:chararray);
cogrouped_final = cogroup std_locations by location, blacklist_states by location;
good_locs = filter cogrouped_final by COUNT(blacklist_states) == 0;
final_std_locations = foreach good_locs generate FLATTEN(std_locations);

rmf standard_locations
sorted_standard_locations = ORDER final_std_locations BY population desc;
store sorted_standard_locations into 'standard_locations';