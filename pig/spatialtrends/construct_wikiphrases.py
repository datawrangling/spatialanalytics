#!/usr/bin/env python
# encoding: utf-8
"""
construct_wikiphrases.py

The file s3://wher20demo/pages-20100316.txt.gz was constructed from
trendingtopics.org data as follows:

s3cmd get --config=/root/.s3cfg -r s3://trendingtopics/archive/20100316/pages/ pages
cat pages/* | sed 's/\x01/\t/g' > pages-20100316.txt
gzip pages-20100316.txt

For the Where 2.0 workshop, instead of using this map side dictionary, we will join to the following table in Pig
cut -f 1 page_lookups.txt | sed 's/\_/\ /g' > wikipedia_dictionary.txt

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
  reader = csv.reader(open('page_lookups.txt', "rb"), delimiter='\t', quoting=csv.QUOTE_NONE)
  
  #phrase std_phrase  page_from page_to
  #Barack_Obama_"Progress"_poster  Barack Obama "Hope" poster      21129442        276142252
  for i, row in enumerate(reader):
    phrase, std_phrase = row[0], row[1]
    wikiphrases[phrase.lower().replace('_',' ')] = 1
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
  print wikiphrases['obama']

if __name__ == '__main__':
  main()
