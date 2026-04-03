#!/usr/bin/env python3
"""Mapper for document statistics: reads doc_id\ttitle\ttext, emits doc_id\ttitle\tdoc_length."""
import sys
import re


def tokenize(text):
    return re.sub(r'[^a-z0-9\s]', ' ', text.lower()).split()


for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split('\t', 2)
    if len(parts) < 3:
        continue
    doc_id, title, text = parts
    doc_length = len(tokenize(text))
    print(f"{doc_id}\t{title}\t{doc_length}")
