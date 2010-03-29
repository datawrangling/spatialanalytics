#!/usr/bin/env python
# encoding: utf-8
"""
construct_wikiphrases.py

The file s3://wher20demo/pages-20100316.txt.gz was constructed from
trendingtopics.org data as follows:

s3cmd get --config=/root/.s3cfg -r s3://trendingtopics/archive/20100316/pages/ pages
cat pages/* | sed 's/\x01/\t/g' > pages-20100316.txt
gzip pages-20100316.txt

This script constructs a python dictionary for use in tokenizing tweets
using the Wikipedia page trend data

Created by Peter Skomoroch on 2010-03-11.
Copyright (c) 2010 Data Wrangling LLC. All rights reserved.
"""

import sys
import os
import csv
import cPickle as pickle

def main():
  wikiphrases = {}
  reader = csv.reader(open('pages-20100316.txt', "rb"), delimiter='\t', quoting=csv.QUOTE_NONE)
  
  for i, row in enumerate(reader):
    title, daily_trend = row[2], row[6]
    wikiphrases[title.lower()] = float(daily_trend)
    if i % 100000 == 0:
      print i
  
  print "Done, saving pickle"
  output = open('wikiphrases.pkl','wb')
  pickle.dump(wikiphrases, output, -1)
  output.close()

  print "test loading pickle"
  pkl_file = open('wikiphrases.pkl', 'rb')
  wikiphrases = pickle.load(pkl_file)
  
  print wikiphrases['gossip girl']
  print wikiphrases['barack obama']

if __name__ == '__main__':
  main()
