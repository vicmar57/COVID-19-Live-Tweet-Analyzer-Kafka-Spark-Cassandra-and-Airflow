# -*- coding: utf-8 -*-
"""
Created on Tue Apr 14 18:41:21 2020

@author: vicma
"""

from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement


# fields = "field1, field2, field3, field4"
# table = "table"
# conditions = "condition1=1 AND condition2=2"

# sql = (f"SELECT {fields} "
#        f"FROM {table} "
#        f"WHERE {conditions};")


if __name__ == "__main__":
    cluster = Cluster(['127.0.0.1'], port = 9042)
    session = cluster.connect('system', wait_for_all_pools = True)
    session.execute('USE system')
    query = SimpleStatement("SELECT * FROM schema_keyspaces;",
                            consistency_level = ConsistencyLevel.ALL)
    
    future = session.execute_async(query)
    
    def log_error(exc):
        print("Operation failed: %s", exc)

    def log_results(results):
        for row in results:
            print(row.keyspace_name, row.strategy_options)

    future.add_callbacks(log_results, log_error)