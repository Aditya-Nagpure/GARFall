"""
RAG Ingestion — loads scraped JSONL chunks into a ChromaDB vector store.

Usage:
    python ingest.py output/cars_tatamotors_com_*.jsonl
    python ingest.py output/*.jsonl --collection my_collection
    python ingest.py output/site.jsonl --reset   # wipe collection before ingesting
"""

import argparse
import glob
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

import chromadb
from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction

DB_DIR = "chroma_db"
DEFAULT_COLLECTION = "rag_chunks"
EMBED_MODEL = "all-MiniLM-L6-v2"
BATCH_SIZE = 64


def load_jsonl(path: str) -> list[dict]:
    records = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def make_doc_id(chunk: dict, idx: int) -> str:
    """Stable ID: url + heading path + position index."""
    url = chunk.get("url", "")
    heading = chunk.get("heading_path", "")
    return f"{url}::{heading}::{idx}"


def ingest(paths: list[str], collection_name: str, reset: bool):
    # Load all chunks from all files
    all_chunks: list[dict] = []
    for path in paths:
        expanded = glob.glob(path)
        if not expanded:
            print(f"  [WARN] No files matched: {path}")
            continue
        for p in expanded:
            chunks = load_jsonl(p)
            print(f"  Loaded {len(chunks):>5} chunks from {p}")
            all_chunks.extend(chunks)

    if not all_chunks:
        print("No chunks loaded. Exiting.")
        sys.exit(1)

    print(f"\n  Total chunks to ingest: {len(all_chunks)}\n")

    # Connect to ChromaDB
    client = chromadb.PersistentClient(path=DB_DIR)
    embed_fn = SentenceTransformerEmbeddingFunction(model_name=EMBED_MODEL)

    if reset:
        try:
            client.delete_collection(collection_name)
            print(f"  Deleted existing collection '{collection_name}'.")
        except Exception:
            pass

    collection = client.get_or_create_collection(
        name=collection_name,
        embedding_function=embed_fn,
        metadata={"hnsw:space": "cosine"},
    )

    # Ingest in batches
    total = len(all_chunks)
    ingested = 0

    for batch_start in range(0, total, BATCH_SIZE):
        batch = all_chunks[batch_start : batch_start + BATCH_SIZE]

        ids = [make_doc_id(c, batch_start + i) for i, c in enumerate(batch)]
        documents = [c.get("text", "") for c in batch]
        metadatas = [
            {
                "url": c.get("url", ""),
                "title": c.get("title", "")[:200],
                "heading_path": c.get("heading_path", ""),
                "source_domain": c.get("source_domain", ""),
                "crawled_at": c.get("crawled_at", ""),
            }
            for c in batch
        ]

        # Skip empty documents (Chroma rejects them)
        filtered = [
            (id_, doc, meta)
            for id_, doc, meta in zip(ids, documents, metadatas)
            if doc.strip()
        ]
        if not filtered:
            continue

        ids_f, docs_f, metas_f = zip(*filtered)

        collection.upsert(
            ids=list(ids_f),
            documents=list(docs_f),
            metadatas=list(metas_f),
        )

        ingested += len(docs_f)
        print(f"  [{ingested:>5}/{total}] ingested...")

    print(f"\n  Done. {ingested} chunks stored in '{DB_DIR}' / collection '{collection_name}'.")
    print(f"  Total in collection: {collection.count()}")


def parse_args():
    parser = argparse.ArgumentParser(description="Ingest scraped JSONL into ChromaDB.")
    parser.add_argument("files", nargs="+", help="JSONL file(s) or glob patterns")
    parser.add_argument("--collection", default=DEFAULT_COLLECTION,
                        help=f"ChromaDB collection name (default: {DEFAULT_COLLECTION})")
    parser.add_argument("--reset", action="store_true",
                        help="Wipe the collection before ingesting")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    print(f"\n{'='*60}")
    print(f"  RAG Ingestion")
    print(f"  DB      : {os.path.abspath(DB_DIR)}")
    print(f"  Collection: {args.collection}")
    print(f"  Model   : {EMBED_MODEL}")
    print(f"{'='*60}\n")
    ingest(args.files, args.collection, args.reset)