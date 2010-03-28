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
echo Running location string merge & standardization...
echo --------------------------
pig -p INPUT=$INPUTFILES -l /mnt standardized_tweets.pig