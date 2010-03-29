REGISTER s3://piggybank/0.6.0/piggybank.jar
DEFINE LOWER org.apache.pig.piggybank.evaluation.string.LOWER();

-- Exact Match approach 2
--- build list cityname->geoid/fips mappings based on most populated variant in US.
--- group standard_us_cities by name, find top city variant by population
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
  
standard_us_cities = FOREACH standard_us_cities GENERATE 
  name, 
  std_location, 
  geonameid, 
  latitude, 
  longitude, 
  population, 
  countyfips;

grouped_cities = GROUP standard_us_cities BY name;

-- default mapping users to top version of city based on population
top_variants = FOREACH grouped_cities {
      sorted = ORDER standard_us_cities BY population DESC;
      sorted = LIMIT sorted 1;
      GENERATE FLATTEN(sorted);}
rmf top_city_variants      
STORE top_variants INTO 'top_city_variants';

--- now join to locations
location_counts = LOAD 'us_location_counts' as (location:chararray, user_count:long);

top_city_variants = LOAD 'top_city_variants' AS (
  name:chararray, 
  std_location:chararray, 
  geonameid:int, 
  latitude:float, 
  longitude:float, 
  population:int, 
  countyfips:chararray
  );

joined_names = JOIN location_counts BY location, top_city_variants BY LOWER(name);
city_counts = FOREACH joined_names GENERATE 
  $0 as location, 
  $3 as std_location, 
  $1 as user_count, 
  $4 as geonameid, 
  $7 as population, 
  $8 as fips;

rmf city_counts
sorted_city_counts = ORDER city_counts BY population DESC;
store sorted_city_counts into 'city_counts';

