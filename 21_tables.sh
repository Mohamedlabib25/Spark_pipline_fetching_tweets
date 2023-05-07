#!/bin/bash

# Run the Hive script to create tables
/opt/hive/bin/hive  -f 20_tables.hql

# Run the Spark job
/opt/spark3/bin/spark-submit 20_fact.py 
