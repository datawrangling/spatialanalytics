#!/bin/sh
# scp parse_stream.py parse_tweets.sh hadoop@ec2-184-73-60-236.compute-1.amazonaws.com:/mnt
# ssh hadoop@ec2-184-73-60-236.compute-1.amazonaws.com
# bash /mnt/parse_tweets.sh 2010-02-1 parsed_tweets_feb
# hadoop distcp /user/root/parsed_tweets_feb/ s3n://where20/parsed_tweets_feb

DATELIMIT=$1
OUTPUT=$2

hadoop jar /home/hadoop/contrib/streaming/hadoop-streaming.jar \
  -input s3n://trendingtopics/tweets/tweets.$DATELIMIT* \
  -output $OUTPUT \
  -mapper "parse_stream.py" \
  -file 'parse_stream.py' \
  -jobconf mapred.output.compress=true \
  -jobconf mapred.job.name=parse_tweets_$DATELIMIT