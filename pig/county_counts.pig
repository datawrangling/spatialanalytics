REGISTER s3://piggybank/0.6.0/piggybank.jar
DEFINE LOWER org.apache.pig.piggybank.evaluation.string.LOWER();
DEFINE REPLACE org.apache.pig.piggybank.evaluation.string.REPLACE();

std_location = LOAD 'standard_locations' as (
  location:chararray, std_location:chararray, user_count:int, geonameid:int, population:int, fips:chararray);

grouped_loc = GROUP std_location BY fips;
fips_counts = FOREACH grouped_loc GENERATE $0 as fips, SUM($1.user_count) as user_count, SUM($1.population) as population;
sorted_fips = ORDER fips_counts BY user_count DESC;
rmf county_counts
STORE sorted_fips INTO 'county_counts';