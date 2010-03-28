#!/usr/bin/env python
# encoding: utf-8
"""
parse_geonames.py

$ grep PPL allCountries.txt | grep US > data/allUSCities.txt
$ cat data/allUSCities.txt | ./parse_geonames.py > output/standard_us_cities.txt
$ wc -l standard_us_cities.txt 
   25451 standard_us_cities.txt


state.txt from http://www.census.gov/geo/www/ansi/state.txt
-----------------------
STATE|STUSAB|STATE_NAME|STATENS
01|AL|Alabama|01779775
02|AK|Alaska|01785533
04|AZ|Arizona|01779777

allCountries.zip from http://download.geonames.org/export/dump/
The main 'geoname' table has the following fields :
---------------------------------------------------
1 geonameid         : integer id of record in geonames database
2 name              : name of geographical point (utf8) varchar(200)
5 latitude          : latitude in decimal degrees (wgs84)
6 longitude         : longitude in decimal degrees (wgs84)
9 country code      : ISO-3166 2-letter country code, 2 characters
10 cc2               : alternate country codes, comma separated, ISO-3166 2-letter country code, 60 characters
11 admin1 code       : fipscode (subject to change to iso code), see exceptions below, see file admin1Codes.txt for 
12 admin2 code       : code for the second administrative division, a county in the US, see file admin2Codes.txt; 
15 population        : bigint (4 byte int)

Created by Peter Skomoroch on 2010-03-25.
Copyright (c) 2010 Data Wrangling LLC. All rights reserved.
"""

import sys
import os
import csv

infile = 'data/state.txt'
StateReader = csv.DictReader(open(infile, 'r'), delimiter='|')

state_fips ={}
state_name = {}
for data in StateReader:
  state_fips[data['STUSAB']]=data['STATE']
  state_name[data['STUSAB']]=data['STATE_NAME']

primary_key = {}

def main():
  for line in sys.stdin: 
    fields = line.strip().split('\t')
    for i, x in enumerate(fields):
      if len(x) == 0:
        fields[i] = ' '
    geonameid = fields[0]
    name = fields[1]
    latitude = fields[4]
    longitude = fields[5]
    country_code = fields[8]
    cc2 = fields[9]
    fipscode = fields[10]
    county = fields[11]
    population = fields[14]
    if int(population) > 0:
      # construct lower case "city,state abbrev."
      standard_name = name + ', ' + fipscode
      try:
        primary_key[standard_name]
      except:
        primary_key[standard_name] = 1  
        full_standard_name = name + ', ' + state_name[fipscode]
        # construct county fips code
        countyfips = state_fips[fipscode] + county
        print '\t'.join([geonameid,name, latitude,longitude,country_code,
          cc2,fipscode,county,population, countyfips, standard_name.lower(), full_standard_name.lower()])


if __name__ == '__main__':
  main()

