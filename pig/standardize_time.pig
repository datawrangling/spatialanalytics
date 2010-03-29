-- Demonstrate use of python streaming with Pig
-- Standardize Timestamps and tokenize tweets....

DEFINE tweet_tokenizer `tweet_tokenizer.py`
  SHIP ('tweet_tokenizer.py');
tokenized_tweets = STREAM tweets THROUGH tweet_tokenizer
  AS (tweet_id:long, tokens:chararray, location:chararray, date:chararray, time:chararray);

-- send unemployment by county in January as side data


