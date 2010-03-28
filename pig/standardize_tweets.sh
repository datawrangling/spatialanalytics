#!/bin/sh
# Run as follows:
# $ bash standardize_tweets.sh s3://where20/sample-tweets-*
INPUTFILES=$1

echo Running US location counts
echo --------------------------
pig -p INPUT=$INPUTFILES -l /mnt us_location_counts.pig
echo Running Exact Match to Geonames "City, State"
echo --------------------------
pig -l /mnt city_state_exactmatch.pig
echo Running Exact Match to Geonames "City"
echo --------------------------
pig -l /mnt city_exactmatch.pig
echo Running Turk Match to Geonames location strings
echo --------------------------
pig -l /mnt turk_exactmatch.pig
echo Merging Standardized location strings
echo --------------------------
pig -l /mnt standardize_locations.pig
## result is 'standard_locations' 
# location:chararray, std_location:chararray, user_count:int, geonameid:int, population:int, fips:chararray
echo Generating list of unknown locations for turkers
echo --------------------------
pig -p INPUT=$INPUTFILES -l /mnt locations_timezones.pig
rm /mnt/locations_timezones.csv
hadoop fs -getmerge /locations_timezones /mnt/locations_timezones.csv
# need to remove UT iphone strings then limit to top 8k...
#
#

# echo Running tweet standardization...
# echo --------------------------
# pig -p INPUT=$INPUTFILES -l /mnt standardized_tweets.pig