#!/bin/bash

# Run the Hive script to create tables
hive -f 20_tables.sql

# Run the Spark job
/opt/spark3/bin/spark-submit 20_fact.py 