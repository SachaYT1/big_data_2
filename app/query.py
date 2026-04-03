#!/usr/bin/env python3
"""BM25 search engine: queries Cassandra index and ranks documents."""
import sys
import re
import math
from pyspark import SparkContext, SparkConf
from cassandra.cluster import Cluster


# BM25 parameters
K1 = 1.0
B = 0.75


def tokenize(text):
    return re.sub(r'[^a-z0-9\s]', ' ', text.lower()).split()


def compute_bm25(doc_info, query_terms_data, N, avgdl):
    """Compute BM25 score for a single document.

    doc_info: (doc_id, title, doc_length)
    query_terms_data: {term: (tf, df)} for terms present in this doc
    """
    doc_id, title, dl = doc_info
    score = 0.0
    for term, (tf, df) in query_terms_data.items():
        idf = math.log(N / df) if df > 0 else 0
        tf_norm = ((K1 + 1) * tf) / (K1 * ((1 - B) + B * (dl / avgdl)) + tf)
        score += idf * tf_norm
    return (doc_id, title, score)


def main():
    if len(sys.argv) < 2:
        print("Usage: query.py <search_query>")
        sys.exit(1)

    query = sys.argv[1]
    query_terms = tokenize(query)

    if not query_terms:
        print("No valid query terms found")
        sys.exit(0)

    # Connect to Cassandra
    cluster = Cluster(['cassandra-server'])
    session = cluster.connect('search_engine')

    # Get corpus stats
    row = session.execute("SELECT num_docs, avg_doc_length FROM corpus_stats WHERE id=1").one()
    if row is None:
        print("ERROR: No corpus stats found. Run indexing first.")
        sys.exit(1)
    N = row.num_docs
    avgdl = row.avg_doc_length

    # For each query term, fetch postings from inverted index
    # Build: {doc_id: {term: (tf, df)}}
    candidates = {}
    for term in set(query_terms):
        rows = session.execute(
            "SELECT doc_id, tf, df FROM inverted_index WHERE term=%s", (term,)
        )
        for r in rows:
            if r.doc_id not in candidates:
                candidates[r.doc_id] = {}
            candidates[r.doc_id][term] = (r.tf, r.df)

    if not candidates:
        print("No documents found for the given query")
        cluster.shutdown()
        sys.exit(0)

    # Fetch doc stats for all candidate documents
    doc_infos = {}
    for doc_id in candidates:
        r = session.execute(
            "SELECT title, doc_length FROM doc_stats WHERE doc_id=%s", (doc_id,)
        ).one()
        if r:
            doc_infos[doc_id] = (doc_id, r.title, r.doc_length)

    cluster.shutdown()

    # Use PySpark RDD to compute BM25 scores
    conf = SparkConf().setAppName("BM25Search")
    sc = SparkContext(conf=conf)

    # Create RDD of candidate documents and compute scores
    candidates_list = [
        (doc_infos[doc_id], candidates[doc_id])
        for doc_id in candidates
        if doc_id in doc_infos
    ]

    # Broadcast corpus stats
    N_bc = sc.broadcast(N)
    avgdl_bc = sc.broadcast(avgdl)

    results_rdd = sc.parallelize(candidates_list).map(
        lambda x: compute_bm25(x[0], x[1], N_bc.value, avgdl_bc.value)
    )

    # Get top 10 results
    top_results = results_rdd.takeOrdered(10, key=lambda x: -x[2])

    # Print results
    print("\n=== Search Results ===")
    print(f"Query: {query}")
    print(f"Found {len(top_results)} results:\n")
    for doc_id, title, score in top_results:
        print(f"{doc_id}\t{title}")

    sc.stop()


if __name__ == "__main__":
    main()
