#!/usr/bin/env python3
"""Reducer for document statistics: passes through doc stats, computes corpus-level N and avgdl."""
import sys

total_docs = 0
total_length = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split('\t', 2)
    if len(parts) < 3:
        continue
    doc_id, title, doc_length = parts
    doc_length = int(doc_length)

    print(f"{doc_id}\t{title}\t{doc_length}")
    total_docs += 1
    total_length += doc_length

if total_docs > 0:
    avg_doc_length = total_length / total_docs
    print(f"__CORPUS__\t{total_docs}\t{avg_doc_length:.2f}")
