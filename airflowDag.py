# -*- coding: utf-8 -*-
"""
Created on Tue Apr 21 14:15:59 2020

@author: vicma
"""

from airflow import DAG
from airflow.operators import BashOperator,PythonOperator
from datetime import datetime, timedelta

thirty_sec_from_now = datetime.now() + timedelta(seconds=30)

default_args = {
    'owner': 'VicMar',
    'depends_on_past': False,
    'start_date': thirty_sec_from_now,
    'email': ['vicmar57@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': timedelta(minutes = 10)
    }

dag = DAG('simple', default_args=default_args)

dummyStart = BashOperator(task_id='dummpyOpStart', 
                  bash_command='echo "Hello from Airflow!"',
                  dag=dag)

task2 = BashOperator(task_id='runKafkaTwitterProducer', 
                  bash_command='python /home/airflow/airflow/airflow_dags/pyScripts\TwittAnalysis/KafkaTwitterProducer.py',
                  dag=dag)

task3 = BashOperator(task_id='runKafkaTwitterConsProd', 
                  bash_command='python /home/airflow/airflow/airflow_dags/pyScripts\TwittAnalysis/KafkaConsumerProducer.py',
                  dag=dag)

task4 = BashOperator(task_id='runKafkaTwitterPushToCassandra', 
                  bash_command='python /home/airflow/airflow/airflow_dags/pyScripts\TwittAnalysis/KafkaConsumerWriteToCassandra.py',
                  dag=dag)

sparkAnalysisTask = BashOperator(task_id='sparkCassandraAnalysis', 
                  bash_command='python /home/airflow/airflow/airflow_dags/pyScripts\TwittAnalysis/SparkCassandraAnalysis.py',
                  dag=dag)

dummyStart >> task2
dummyStart >> task3
dummyStart >> task4
