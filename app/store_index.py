#!/usr/bin/env python3
"""Read MapReduce index output from HDFS and store in Cassandra."""
import time
from pyspark import SparkContext, SparkConf
from cassandra.cluster import Cluster


def connect_cassandra(retries=10, delay=5):
    """Connect to Cassandra with retry logic for startup timing."""
    for attempt in range(retries):
        try:
            cluster = Cluster(['cassandra-server'])
            session = cluster.connect()
            print(f"Connected to Cassandra (attempt {attempt + 1})")
            return cluster, session
        except Exception as e:
            print(f"Cassandra connection attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                time.sleep(delay)
    raise RuntimeError("Could not connect to Cassandra")


def create_schema(session):
    """Create keyspace and tables."""
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS search_engine
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    session.set_keyspace('search_engine')

    session.execute("""
        CREATE TABLE IF NOT EXISTS inverted_index (
            term text,
            doc_id text,
            tf int,
            df int,
            PRIMARY KEY (term, doc_id)
        )
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS doc_stats (
            doc_id text PRIMARY KEY,
            title text,
            doc_length int
        )
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS corpus_stats (
            id int PRIMARY KEY,
            num_docs int,
            avg_doc_length float
        )
    """)
    print("Schema created successfully")


def main():
    conf = SparkConf().setAppName("StoreIndex")
    sc = SparkContext(conf=conf)

    cluster, session = connect_cassandra()
    create_schema(session)

    # Prepare statements
    insert_index = session.prepare(
        "INSERT INTO search_engine.inverted_index (term, doc_id, tf, df) VALUES (?, ?, ?, ?)"
    )
    insert_doc = session.prepare(
        "INSERT INTO search_engine.doc_stats (doc_id, title, doc_length) VALUES (?, ?, ?)"
    )
    insert_corpus = session.prepare(
        "INSERT INTO search_engine.corpus_stats (id, num_docs, avg_doc_length) VALUES (?, ?, ?)"
    )

    # Load inverted index from HDFS
    print("Loading inverted index from HDFS...")
    index_rdd = sc.textFile("hdfs:///indexer/index/part-*")
    index_rows = index_rdd.collect()

    count = 0
    for row in index_rows:
        parts = row.split('\t')
        if len(parts) == 4:
            term, doc_id, tf, df = parts
            session.execute(insert_index, (term, doc_id, int(tf), int(df)))
            count += 1
    print(f"Inserted {count} inverted index entries")

    # Load document stats from HDFS
    print("Loading document stats from HDFS...")
    doc_rdd = sc.textFile("hdfs:///indexer/doc_stats/part-*")
    doc_rows = doc_rdd.collect()

    doc_count = 0
    for row in doc_rows:
        parts = row.split('\t')
        if len(parts) == 3:
            if parts[0] == '__CORPUS__':
                num_docs = int(parts[1])
                avg_doc_length = float(parts[2])
                session.execute(insert_corpus, (1, num_docs, avg_doc_length))
                print(f"Corpus stats: N={num_docs}, avgdl={avg_doc_length:.2f}")
            else:
                doc_id, title, doc_length = parts
                session.execute(insert_doc, (doc_id, title, int(doc_length)))
                doc_count += 1
    print(f"Inserted {doc_count} document stats entries")

    cluster.shutdown()
    sc.stop()
    print("Index storage complete")


if __name__ == "__main__":
    main()
