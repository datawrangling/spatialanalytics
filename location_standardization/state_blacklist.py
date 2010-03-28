#!/usr/bin/env python
# encoding: utf-8
"""
city_blacklist.py

Exclude known larger entities like countries and states
 with potentially overlapping names

http://download.geonames.org/export/dump/countryInfo.txt

Created by Peter Skomoroch on 2010-03-28.
Copyright (c) 2010 Data Wrangling LLC. All rights reserved.
"""

import sys
import os
import csv

statefile = 'data/state.txt'
countryfile = 'data/countryInfo.txt'
outfile = open('output/blacklist_states.txt', 'w') 
StateReader = csv.DictReader(open(statefile, 'r'), delimiter='|')
CountryReader = csv.DictReader(open(countryfile, 'r'), delimiter='\t')

for data in CountryReader:
  country = (data['Country']).lower()
  if country not in []:
    print >> outfile, country


for data in StateReader:
  state = (data['STATE_NAME']).lower()
  if state not in ['district of columbia', 'new york']:
    print >> outfile, state

outfile.close()  

 


