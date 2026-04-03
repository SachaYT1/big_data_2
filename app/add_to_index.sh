#!/bin/bash
echo "Adding new documents to the index"

if [ -z "$1" ]; then
    echo "Usage: add_to_index.sh <path_to_new_documents>"
    echo "  path_to_new_documents: local directory containing .txt files to add"
    exit 1
fi

NEW_DOCS_PATH=$1

if [ ! -d "$NEW_DOCS_PATH" ]; then
    echo "ERROR: Directory $NEW_DOCS_PATH not found"
    exit 1
fi

# Step 1: Upload new documents to HDFS /data/
echo "Uploading new documents to HDFS..."
hdfs dfs -put -f "$NEW_DOCS_PATH"/* /data/

# Step 2: Regenerate tab-separated input from all documents
echo "Regenerating tab-separated input..."
source .venv/bin/activate
export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON

hdfs dfs -rm -r -f /input/data
spark-submit prepare_data.py

# Step 3: Re-run full indexing pipeline
echo "Re-running full indexing pipeline..."
bash index.sh

echo "Incremental indexing complete"
