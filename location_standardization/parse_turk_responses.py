#!/usr/bin/env python
# encoding: utf-8
"""
parse_turk_responses.py

Created by Peter Skomoroch on 2010-03-26.
Copyright (c) 2010 __MyCompanyName__. All rights reserved.
"""

import sys
import getopt
import os
import csv
from collections import defaultdict

help_message = '''
Usage: $ ./parse_turk_responses.py -f data/Batch_213923_result.csv > output/location_geo_mapping.txt
'''

# manual overrides for a few bad turk geonameid responses
override_file = 'data/overrides.csv'
OverrideReader = csv.DictReader(open(override_file, 'rU'), delimiter=',')
overrides ={}
for data in OverrideReader:
  overrides[data['location']]=data['geonameid']


class Usage(Exception):
  def __init__(self, msg):
    self.msg = msg


def main(argv=None):
  if argv is None:
    argv = sys.argv
  try:
    try:
      opts, args = getopt.getopt(argv[1:], "hf:v", ["help", "file="])
    except getopt.error, msg:
      raise Usage(msg)
  
    # option processing
    for option, value in opts:
      if option == "-v":
        verbose = True
      if option in ("-h", "--help"):
        raise Usage(help_message)
      if option in ("-f", "--file"):
        infile = value
        
    # outfile = open(infile.replace('result', 'parsed'), 'w')
    reader = csv.DictReader(open(infile, 'r'), delimiter=',', quotechar='"')
    
    # construct hash of geoids for each location
    # return geoid with max frequency
    
    geonameids = {}
    locations = []
    
    for data in reader:
      location = data['Input.location']      
      category = data['Answer.Q1Category']
      geonameid = data['Answer.Q2GeonameID']
      display_string = data['Input.display_string']
      time_zone = data['Input.time_zone']
      user_count = data['Input.user_count'] 
      comment = data['Answer.comment']
      if category == 'city':
        try:
          geonameids[location]
        except:   
          geonameids[location] = defaultdict(int)
        geonameids[location][geonameid] += 1
        
    geonameid_mapping={}    
    for loc in geonameids.keys():
      d = geonameids[loc]
      try:
        idnum = int(max(d, key=d.get))
        try: 
          geonameid_mapping[loc] = overrides[loc]
        except:  
          geonameid_mapping[loc] = str(idnum)
        
        output = ("\t".join([loc, str(idnum)])).encode('utf8')
        print output
      except:
        pass            

  
  except Usage, err:
    print >> sys.stderr, sys.argv[0].split("/")[-1] + ": " + str(err.msg)
    print >> sys.stderr, "\t for help use --help"
    return 2


if __name__ == "__main__":
  sys.exit(main())
