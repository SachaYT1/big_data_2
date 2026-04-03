#!/usr/bin/env python3
"""Reducer for inverted index: groups by term, computes tf per doc and df."""
import sys
from collections import defaultdict


current_term = None
doc_counts = defaultdict(int)


def flush(term, doc_counts):
    df = len(doc_counts)
    for doc_id, tf in doc_counts.items():
        print(f"{term}\t{doc_id}\t{tf}\t{df}")


for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split('\t', 1)
    if len(parts) < 2:
        continue
    term, doc_id = parts

    if term != current_term:
        if current_term is not None:
            flush(current_term, doc_counts)
        current_term = term
        doc_counts = defaultdict(int)

    doc_counts[doc_id] += 1

if current_term is not None:
    flush(current_term, doc_counts)
