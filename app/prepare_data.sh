#!/bin/bash

source .venv/bin/activate

# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python)

unset PYSPARK_PYTHON

# Upload local txt files to HDFS and create tab-separated format
hdfs dfs -rm -r -f /input/data && \
    echo "Putting data to HDFS" && \
    hdfs dfs -put -f data / && \
    spark-submit prepare_data.py && \
    hdfs dfs -ls /data && \
    hdfs dfs -ls /input/data && \
    echo "done data preparation!"
