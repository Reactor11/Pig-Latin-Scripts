-- Data Loading
users = LOAD './users.csv' 
        USING PigStorage(',') 
        AS (id:chararray,name:chararray,state:chararray); 

tweets = LOAD './tweets.csv' 
         USING PigStorage('`') 
         AS (tweet_id:chararray,tweet:chararray,id:chararray);

-- Write a Pig Latin query that outputs the login of all users in NY state
df = JOIN users BY id , tweets BY id;
filter_data = FILTER df BY state == 'NY';
dump filter_data;

--Write a Pig Latin query that returns all the tweets that include the word 'favorite', ordered by tweet id.

word_df = FILTER df BY (tweet matches '.*favorite.*');
ordered_df = ORDER word_df BY tweet_id ASC;
dump ordered_df;

-- Write a Pig Latin query that computes the natural join between the two collections

df = JOIN users BY id , tweets BY id;
dump df;

-- Write a Pig Latin query that returns the number of tweets for each user name (not login). 
-- You should output one user per line, in the following format:    user_name, number_of_tweets

grouped_data = GROUP tweets BY id;
count_t = FOREACH grouped_data GENERATE group as grp, COUNT(tweets.tweet);
dump count_t;

-- Write a Pig Latin query that returns the number of tweets for each user name (not login), 
-- ordered from most active to least active users. You should output one user per line, 
-- in the following format:    user_name, number_of_tweets

count_t_ordered = ORDER count_t BY $1 DESC;
dump count_t_ordered;

-- Write a Pig Latin query that returns the name of users that posted at least two tweets. 
-- You should output one user name per line.

greaterThanTwo = FILTER count_t_ordered BY $1 >= 2;
temp = FOREACH greaterThanTwo GENERATE $0;
dump temp;

-- Write a Pig Latin query that returns the name of users that posted no tweets.
-- You should output one user name per line.


left_Join = JOIN users by id LEFT OUTER , tweets BY id;
left_group = GROUP left_Join BY id;
group_count = FOREACH grouped_data GENERATE group as grp, COUNT($1);
zero_tweet = FILTER group_count BY $1 == 0;
temp = FOREACH zero_tweet GENERATE $0;
dump temp;