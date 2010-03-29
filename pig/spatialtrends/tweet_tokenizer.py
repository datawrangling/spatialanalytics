#!/usr/bin/env python
# encoding: utf-8
"""
tweet_tokenizer.py

sample input format:
-- 31055 5074472 Wed Feb 10 04:59:42 +0000 2010  thanks for coming to pub quiz steph jess ali and stacey!
-- 06073 5391811 Wed Feb 10 04:50:26 +0000 2010  looooooooost!!

sample output format
-- i know	25025	4930956	2010-02-10	4
-- people	36061	5128581	2010-02-10	2
-- read	36061	5128581	2010-02-10	2


Created by Peter Skomoroch on 2010-03-29.
Copyright (c) 2010 Data Wrangling LLC. All rights reserved.
"""

import sys
import os
import zipimport
import cPickle as pickle
import rfc822
import time
import datetime
import re

# Pattern for fully-qualified URLs:
url_pattern = re.compile('''["']http://[^+]*?['"]''')

# load NLTK from distributed cache
importer = zipimport.zipimporter('nltkandyaml.mod')
yaml = importer.load_module('yaml')
nltk = importer.load_module('nltk')

# load stopword list
stopwords = open('stopwords.txt','r').readlines()
stopwords = [word.strip() for word in stopwords]

def gethour(timestamp):
  ''' convert timestamp of form: "Mon Mar 22 02:23:53 +0000 2010" '''
  rftime = rfc822.parsedate(timestamp)
  return str(rftime[3])

def getdate(timestamp):
  ''' convert timestamp of form: "Mon Mar 22 02:23:53 +0000 2010" '''
  rftime = rfc822.parsedate(timestamp)
  dateval = datetime.date(rftime[0], rftime[1], rftime[2])
  return dateval.isoformat()

def tokenize(text):
  tokenizer = nltk.tokenize.punkt.PunktWordTokenizer()
  tokens = tokenizer.tokenize(text)
  return tokens

def find_ngrams(seq, n):
  '''Use python list comprehension to generate ngrams'''
  ngram_list = [seq[i:i+n] for i in range(1+len(seq)-n)]
  return [' '.join(ngram) for ngram in ngram_list]
  
def emit_phrases(ngrams, fipscode, geonameid, date, hour):
  '''Validate ngrams against wikipedia phrases and emit to stdout'''
  for ngram in ngrams:
    try:
      #exclude numbers
      int(ngram)
    except:  
      #exclude special chars
      if len(ngram.strip()) > 1:
        print '\t'.join([ngram, fipscode, geonameid, date, hour])  

for line in sys.stdin:
  try:
    fipscode, geonameid, timestamp, tweet_text = line.strip().split('\t')
    date = getdate(timestamp)
    hour = gethour(timestamp)
    
    unigrams = tokenize(tweet_text)
    filtered_unigrams = list(set(unigrams) - set(stopwords))
    emit_phrases(filtered_unigrams, fipscode, geonameid, date, hour)  
     
    bigrams = find_ngrams(unigrams, 2)
    emit_phrases(bigrams, fipscode, geonameid, date, hour)  
    
    trigrams = find_ngrams(unigrams, 3)
    emit_phrases(trigrams, fipscode, geonameid, date, hour)
    
    fourgrams = find_ngrams(unigrams, 4)
    emit_phrases(fourgrams, fipscode, geonameid, date, hour)    
    
  except:
    pass

