-- genrate hourly heatmaps for 'good morning' and 'good night'

tweet_phrases = LOAD 'tweet_phrases' as (
  phrase:chararray, 
  date:chararray, 
  hour:int, 
  fipscode:chararray,
  count:int);

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


  


  

    