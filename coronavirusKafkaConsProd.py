# -*- coding: utf-8 -*-
"""
Created on Thu Apr 16 12:36:17 2020

@author: vicma
"""

import json
from time import sleep
from kafka import KafkaConsumer, KafkaProducer

ind = 0

def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer


# Fields we care about:
# parsed["user"]["id"]
# parsed["text"]
# parsed["entities"]["hashtags"]
# parsed["place"]["full_name"]
# parsed["place"]["country"]
# parsed["created_at"] 
        
def parse(tweetData):
    userID = '-'
    tweetText = '-'
    hashTags = []
    location_full_name = '-'
    location_coutry = '-'
    created_at = '-'

    try:
        userID = tweetData["user"]["id"]
        if tweetData["truncated"] == True:
            tweetText = tweetData["extended_tweet"]["full_text"]
            hashTags = [ hTag["text"] for hTag in tweetData["extended_tweet"]["entities"]["hashtags"] ]
        else:
            tweetText = tweetData["text"]
            hashTags = [ hTag["text"] for hTag in tweetData["entities"]["hashtags"] ]
        
        if hashTags == []:
            global ind
            print("hashTags list empty " + str(ind))
            ind += 1
        if "place" in tweetData and tweetData["place"] != None:
            location_full_name = tweetData["place"]["full_name"]
            location_coutry = tweetData["place"]["country"]
        created_at = tweetData["created_at"]

        rec = {'userID': userID, 
               'tweetText': tweetText, 
               'hashTags': hashTags, 
               'location_full_name': location_full_name,
               'location_coutry': location_coutry,
               'created_at': created_at }
    
        # session.execute(
        # """
        # INSERT INTO users (name, credits, user_id)
        # VALUES (%s, %s, %s)
        # """,
        # ("John O'Reilly", 42, uuid.uuid1()))
        return json.dumps(rec)

    except Exception as ex:
        print('Exception while parsing')
        print(str(ex))
        

if __name__ == '__main__':
    print('Consuming coronavirus raw tweets and re-publishing parsed ones..')
    parsed_records = []
    topic_name = 'coronavirus_RAW_tweets'
    parsed_topic_name = 'COVID_parsed_tweets'

    consumer = KafkaConsumer(topic_name, auto_offset_reset='earliest',
                             bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)
    for msg in consumer:
        html = json.loads(msg.value)
        record = parse(html)
        parsed_records.append(record)

    consumer.close()
    sleep(5)

    if len(parsed_records) > 0:
        print('Publishing records..')
        producer = connect_kafka_producer()
        for rec in parsed_records:
            publish_message(producer, parsed_topic_name, 'parsed', rec)