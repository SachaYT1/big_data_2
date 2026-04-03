#!/bin/bash
echo "Running full indexing pipeline"

INPUT_PATH=${1:-/input/data}

# Step 1: Create index via MapReduce
bash create_index.sh "$INPUT_PATH"
if [ $? -ne 0 ]; then
    echo "ERROR: Index creation failed"
    exit 1
fi

# Step 2: Store index in Cassandra
bash store_index.sh
if [ $? -ne 0 ]; then
    echo "ERROR: Index storage failed"
    exit 1
fi

echo "Full indexing pipeline complete"
