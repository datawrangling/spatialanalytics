#!/bin/sh
pig -l /mnt us_location_counts.pig
pig -l /mnt city_state_exactmatch.pig
pig -l /mnt city_exactmatch.pig
#pig -l /mnt turk_exactmatch.pig