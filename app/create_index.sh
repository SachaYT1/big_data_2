#!/bin/bash
echo "Creating index using MapReduce pipelines"

INPUT_PATH=${1:-/input/data}
echo "Input path: $INPUT_PATH"

STREAMING_JAR=$(find $HADOOP_HOME -name "hadoop-streaming*.jar" 2>/dev/null | head -1)
if [ -z "$STREAMING_JAR" ]; then
    echo "ERROR: hadoop-streaming jar not found"
    exit 1
fi
echo "Using streaming jar: $STREAMING_JAR"

# Clean previous outputs
hdfs dfs -rm -r -f /indexer/index
hdfs dfs -rm -r -f /indexer/doc_stats

# Job 1: Inverted Index (term -> doc_id, tf, df)
echo "=== Running Job 1: Inverted Index ==="
hadoop jar $STREAMING_JAR \
    -input $INPUT_PATH \
    -output /indexer/index \
    -mapper "python3 mapper1.py" \
    -reducer "python3 reducer1.py" \
    -file /app/mapreduce/mapper1.py \
    -file /app/mapreduce/reducer1.py

if [ $? -ne 0 ]; then
    echo "ERROR: Job 1 (Inverted Index) failed"
    exit 1
fi

# Job 2: Document Statistics (single reducer for global stats)
echo "=== Running Job 2: Document Statistics ==="
hadoop jar $STREAMING_JAR \
    -D mapreduce.job.reduces=1 \
    -input $INPUT_PATH \
    -output /indexer/doc_stats \
    -mapper "python3 mapper2.py" \
    -reducer "python3 reducer2.py" \
    -file /app/mapreduce/mapper2.py \
    -file /app/mapreduce/reducer2.py

if [ $? -ne 0 ]; then
    echo "ERROR: Job 2 (Document Statistics) failed"
    exit 1
fi

echo "=== MapReduce indexing complete ==="
hdfs dfs -ls /indexer/index
hdfs dfs -ls /indexer/doc_stats
