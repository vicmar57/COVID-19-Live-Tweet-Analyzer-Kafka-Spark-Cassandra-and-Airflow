# -*- coding: utf-8 -*-
"""
Created on Thu Apr 16 21:15:09 2020

@author: vicma
"""

import json
from time import sleep
import numpy as np
from sys import getsizeof
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from cassandra.query import BatchStatement

parsed_topic_name = "2_COVID_parsed_tweets"

if __name__ == '__main__':
    ind = 1
    inc_count = 0
    batch_thresh =  10
    
    consumer = KafkaConsumer(parsed_topic_name, auto_offset_reset='earliest',
                             bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=10000)
    
    fields = "userid, tweet_text, hashtags, location_full_name, location_country, created_at"
    table = "parsedtweetstable"
    
    cluster = Cluster(['127.0.0.1'], port = 9042)
    session = cluster.connect('twitteranalysisdb', wait_for_all_pools = True)
    session.execute('USE twitteranalysisdb')
    query = (f"INSERT INTO {table} "
            f"({fields}) "
            f"VALUES (?, ?, ?, ?, ?, ?);")
    insertTweet = session.prepare(query) 
    batch = BatchStatement(consistency_level=ConsistencyLevel.ONE) #QUORUM
    
    batchSTMTs = []
    
    for msg in consumer:
        if (ind < batch_thresh):
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
                batchSTMTs.append(record)
                batch.add(insertTweet, record)
                #print("added record no. " + str(ind) + " to batch.")
                ind += 1
                if (ind == batch_thresh): 
                    #sleep(1)
                    print("size of batch in bytes: {}".format(getsizeof(batchSTMTs)))
                    
                    if (inc_count == 18):
                        print("got here")
                    session.execute(batch)
                    print("executed batch INSERT of " + str(ind) + " records to Cassandra. Exiting...")
                    ind = 0
                    inc_count += 1
                    batchSTMTs = []
                
            except Exception as e:
                print("Exception: {}".format(e))

    print("\nInserted {} records and failed, Bye Bye!!\n".format( batch_thresh*inc_count + ind))
    if consumer is not None:
        consumer.close()