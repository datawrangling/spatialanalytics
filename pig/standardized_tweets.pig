-- standardization approach (data limited to US)
-- 1) mechanical turk mappings
-- 2) city, state abbrev. exact match
-- 3) exact match to most populated city name

--- merge location name mapping files into a single file

-- Standardize locations in tweets
tweets = ?
-- filter by time zone
filtered_tweets = ?
-- filter by location
filtered_tweets = ?
-- restrict fields
filtered_tweets = ? 
-- use replicated join, since geoid/ location relations are small enough to fit into main memory.
standardized_tweets = join filtered_tweets by LOWER(user_location), std_locations by location using "replicated";

standardized_tweets = FOREACH standardized_tweets GENERATE ???
