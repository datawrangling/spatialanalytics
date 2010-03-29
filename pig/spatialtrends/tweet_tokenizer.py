#!/usr/bin/env python
# encoding: utf-8
"""
tweet_tokenizer.py

Created by Peter Skomoroch on 2010-03-29.
Copyright (c) 2010 Data Wrangling LLC. All rights reserved.
"""

import sys
import os
import zipimport
import cPickle as pickle

# load NLTK from distributed cache
importer = zipimport.zipimporter(’nltkandyaml.mod’)
yaml = importer.load_module(’yaml’)
nltk = importer.load_module(’nltk’)

# load Wikipedia page title hash
pkl_file = open('wikiphrases.pkl', 'rb')
wikiphrases = pickle.load(pkl_file)

def gethour(timestamp):
  pass

def getdate(timestamp):
  pass  

def tokenize(text):
  tokenizer = nltk.tokenize.punkt.PunktWordTokenizer()
  tokens = tokenizer.tokenize(text)
  return tokens

def find_ngrams(seq, n):
  '''Use python list comprehension to generate ngrams'''
  return [seq[i:i+n] for i in range(1+len(seq)-n)]
  
def emit_phrases(ngrams, date, hour, geonameid, fipscode):
  '''Validate ngrams against wikipedia phrases and emit to stdout'''
  for ngram in ngrams:
    if wikiphrases.has_key(ngram):
      print '\t'.join([ngram, date, hour, geonameid, fipscode, '1'])  

for line in sys.stdin:
  try:
    timestamp, geonameid, fipscode, tweet_text = line.strip().split('\t')
    date = getdate(timestamp)
    hour = gethour(timestamp)
    
    unigrams = tokenize(chunk)
    emit_phrases(unigrams, date, hour, geonameid, fipscode)  
     
    bigrams = find_ngrams(unigrams, 2)
    emit_phrases(bigrams, date, hour, geonameid, fipscode)  
    
    trigrams = find_ngrams(unigrams, 3)
    emit_phrases(trigrams, date, hour, geonameid, fipscode)
  except:
    pass

