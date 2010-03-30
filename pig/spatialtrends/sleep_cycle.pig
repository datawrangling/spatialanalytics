-- genrate hourly heatmaps for 'good morning' and 'good night'

tweet_phrases = LOAD 's3://where20demo/tweet_phrases/'as (
  phrase:chararray, 
  date:chararray, 
  hour:int, 
  fipscode:chararray,
  count:int);

grouped_phrases = GROUP tweet_phrases by phrase;
phrase_counts = FOREACH grouped_phrases GENERATE $0 as phrase, SUM($1.count) as count;
sorted_phrase_counts = ORDER phrase_counts BY count desc PARALLEL 1;
DUMP sorted_phrase_counts LIMIT 100;
--STORE sorted_phrase_counts INTO 's3://where20demo/twitter_phrase_counts';


obama_tweets = FILTER tweet_phrases by phrase == 'obama';
grouped_obama = GROUP obama_tweets by date;
obama_counts = FOREACH grouped_obama GENERATE $0 as date, SUM($1.count) as count;
sorted_obama_counts = ORDER obama_counts BY date;
DUMP sorted_obama_counts;

morning = filter tweet_phrases by 
  (phrase == 'good morning');

morning_group = GROUP morning BY (hour, fipscode);
morning_counts = FOREACH morning_group GENERATE
  $0.hour, $0.fipscode, SUM($1.count) as count;

STORE morning_counts INTO 'morning_counts';  
  
night = filter tweet_phrases by 
  (phrase == 'goodnight') or (phrase == 'good night');
  
night_group = GROUP night BY (hour, fipscode);
night_counts = FOREACH night_group GENERATE
  $0.hour, $0.fipscode, SUM($1.count) as count;

STORE night_counts INTO 'night_counts';  


  


  

    