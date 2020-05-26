# COVID-19 Live Tweet Analyzer
Pulling worldwide tweets and analyzing most popular words, hashtags, most tweeted locations and more. 
Ingesting data using Kafka, storing in Cassandra, analyzing with Spark, and scheduling with Airflow.

the system is comprised of 3 Apache Kafka microservices - 1 consumer that pulls tweets from twitter and push them to raw_tweet_data Kafka topic, 1 consumer-producer to get the raw tweets, parse them and publish to a raw_tweet_data Kafka topic, and the last consumer, that get the parsed tweets and publishes them to cassandra.

After this process is done, an Apache Spark service is spun-up, to pull the data from cassandra and analize it as written above (get most popular words, hashtags, most tweeted locations and more.). After the analysis, the results are written to an incremental resuts file, that can be use for dashbording etc.

All these services are scheduled by Apache Airflow

High level system architecture:
![Alt text](covid-19-twitter-analytics.png?raw=true "system architecture")

## Usage
(These steps apply for the state when Kafka, Airflow, Spark, and Cassandra are all setup and ready to go, and the user has a twitter API user, and credentials).
1. spin up zookeeper in order for Kafka services to work (can be done using Airflow - todo)
2. start the airflow DAG to start the process
