# -*- coding: utf-8 -*-
"""
Created on Sun Apr 19 13:42:00 2020

@author: vicma
"""

from cassandra.cluster import Cluster
import pandas as pd
from pandas import DataFrame

cluster = Cluster(['127.0.0.1'], port = 9042)
session = cluster.connect('twitteranalysisdb', wait_for_all_pools = True)

pdDF = pd.DataFrame(session.execute('select * from parsedtweetstable;'))

from pyspark import SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import desc

# sc = SparkContext(appName='cassandra analysis').getOrCreate()
# rdd = sc.parallelize(pdDF)

spark = SparkSession.builder \
    .master("local") \
    .appName("cassandra Word Count") \
    .getOrCreate()
    
spark.sparkContext._conf.setAll([('spark.executor.memory', '4g'), \
                                 ('spark.executor.cores', '4'), \
                                 ('spark.cores.max', '4'), \
                                 ('spark.driver.memory','4g')])

# print(spark.sparkContext._conf.getAll())

spRDD = spark.createDataFrame(pdDF).cache()
# spRDD.printSchema()

# Counts people by age
countsByhashtags = spRDD.groupBy("hashtags").count().sort(desc("count"))
countsByhashtags.show(10)

onlyText = spRDD.select("tweet_text")
onlyText.show(5)

sparkRDD = onlyText.rdd

splitted = sparkRDD.flatMap(lambda line : str(line).split(' ')) 
    # .reduce(lambda word : (word , 1))

reduced = splitted.map(lambda word : (str(word), 1)) \
                    .reduceByKey(lambda a, b: a + b) \
                    .sortBy(lambda a: -a[1])
        
topWords = reduced.collect()[:20]

for i in range(len(topWords)):
    print(str(i+1) + ". " + str(topWords[i]))
