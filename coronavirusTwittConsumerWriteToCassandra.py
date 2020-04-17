# -*- coding: utf-8 -*-
"""
Created on Thu Apr 16 21:15:09 2020

@author: vicma
"""

import json
from time import sleep
import numpy as np
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from cassandra.query import BatchStatement

if __name__ == '__main__':
    parsed_topic_name = 'COVID_parsed_tweets'
    
    consumer = KafkaConsumer(parsed_topic_name, auto_offset_reset='earliest',
                             bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)
    
    fields = "userid, tweet_text, hashtags, location_full_name, location_country, created_at"
    table = "parsedtweetstable"
    
    cluster = Cluster(['127.0.0.1'], port = 9042)
    session = cluster.connect('twitteranalysisdb', wait_for_all_pools = True)
    session.execute('USE twitteranalysisdb')
    query = (f"INSERT INTO {table} "
            f"({fields}) "
            f"VALUES (?, ?, ?, ?, ?, ?);")
    insertTweet = session.prepare(query) 
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

    for msg in consumer:
        try:
            msgDict = json.loads(msg.value)
            if msgDict == {}:
                print(msg)
                continue
            
            record = (msgDict["userID"], 
                      msgDict["tweetText"],
                      ', '.join(msgDict["hashTags"]), 
                      msgDict["location_full_name"],
                      msgDict["location_coutry"],
                      msgDict["created_at"])
            batch.add(insertTweet, record)
            
        except Exception as e:
            print(e)
            #logger.error('The cassandra error: {}'.format(e))
    
    
    session.execute(batch)
    if consumer is not None:
        consumer.close()