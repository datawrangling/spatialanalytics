#!/bin/sh
echo Running US location counts
echo --------------------------
pig -l /mnt us_location_counts.pig
echo Running Exact Match to Geonames "City, State"
echo --------------------------
pig -l /mnt city_state_exactmatch.pig
echo Running Exact Match to Geonames "City"
echo --------------------------
pig -l /mnt city_exactmatch.pig
#pig -l /mnt turk_exactmatch.pig