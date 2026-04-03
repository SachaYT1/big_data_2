#!/bin/bash
echo "Storing index data in Cassandra"

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python)
export PYSPARK_PYTHON=./.venv/bin/python

spark-submit --master yarn --archives /app/.venv.tar.gz#.venv store_index.py

if [ $? -ne 0 ]; then
    echo "ERROR: Failed to store index in Cassandra"
    exit 1
fi

echo "Index stored in Cassandra successfully"
