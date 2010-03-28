sorted_counts = LOAD 'global_location_counts' as (location:chararray, user_count:long);
stub_counts = FOREACH sorted_counts GENERATE '1' as stub, location, user_count;
grouped_stubs = GROUP stub_counts by stub;
total_count = FOREACH grouped_stubs GENERATE $0 as stub, SUM($1.user_count) as total_users;
DUMP total_count;