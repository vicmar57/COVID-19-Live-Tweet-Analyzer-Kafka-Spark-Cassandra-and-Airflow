CREATE KEYSPACE IF NOT EXISTS twitterAnalysisDB
  WITH REPLICATION = { 
   'class' : 'SimpleStrategy', 
   'replication_factor' : 1 
  };
  
USE twitterAnalysisDB;
Create table IF NOT EXISTS parsedTweetsTable
    (
        userID bigint,
        tweet_Text text,
        hashTags text,
        location_full_name text,
        location_country text,
        created_at text,
        Primary key(userID, location_country)
    );
	
--DROP TABLE parsedTweetsTable