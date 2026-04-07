"""
RAG Query — retrieves relevant chunks from ChromaDB and answers with Groq.

Usage:
    python query.py "What are Tata Motors' service offerings?"
    python query.py "What is Value Care AMC?" --top-k 8
    python query.py "Explain nexon features" --collection tatamotors --no-generate
"""

import argparse
import os
import sys

from dotenv import load_dotenv
load_dotenv()

from groq import Groq
import chromadb
from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction

DB_DIR = "chroma_db"
DEFAULT_COLLECTION = "rag_chunks"
EMBED_MODEL = "all-MiniLM-L6-v2"
DEFAULT_TOP_K = 5
GROQ_MODEL = "llama-3.3-70b-versatile"


def retrieve(collection, query: str, top_k: int) -> list[dict]:
    results = collection.query(
        query_texts=[query],
        n_results=top_k,
        include=["documents", "metadatas", "distances"],
    )

    chunks = []
    for doc, meta, dist in zip(
        results["documents"][0],
        results["metadatas"][0],
        results["distances"][0],
    ):
        chunks.append({
            "text": doc,
            "url": meta.get("url", ""),
            "title": meta.get("title", ""),
            "heading_path": meta.get("heading_path", ""),
            "source_domain": meta.get("source_domain", ""),
            "score": round(1 - dist, 4),
        })

    return chunks


def build_context(chunks: list[dict]) -> str:
    parts = []
    for i, c in enumerate(chunks, 1):
        heading = f" > {c['heading_path']}" if c["heading_path"] else ""
        parts.append(f"[{i}] Source: {c['url']}{heading}\n{c['text']}")
    return "\n\n---\n\n".join(parts)


def generate(query: str, chunks: list[dict]) -> None:
    context = build_context(chunks)

    system = (
        "You are a helpful assistant that answers questions based on provided source documents. "
        "Answer using only the information in the context. "
        "If the context does not contain enough information, say so clearly. "
        "Cite sources using the [N] reference numbers when relevant."
    )

    client = Groq()

    print("\nAnswer:\n")
    stream = client.chat.completions.create(
        model=GROQ_MODEL,
        max_tokens=2048,
        stream=True,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": f"Context:\n{context}\n\nQuestion: {query}"},
        ],
    )
    for chunk in stream:
        text = chunk.choices[0].delta.content or ""
        print(text, end="", flush=True)

    print("\n")


def parse_args():
    parser = argparse.ArgumentParser(description="Query the RAG vector store.")
    parser.add_argument("query", help="Question to ask")
    parser.add_argument("--top-k", type=int, default=DEFAULT_TOP_K,
                        help=f"Number of chunks to retrieve (default: {DEFAULT_TOP_K})")
    parser.add_argument("--collection", default=DEFAULT_COLLECTION,
                        help=f"ChromaDB collection name (default: {DEFAULT_COLLECTION})")
    parser.add_argument("--no-generate", action="store_true",
                        help="Only retrieve chunks, skip LLM generation")
    return parser.parse_args()


def main():
    args = parse_args()

    if not os.path.exists(DB_DIR):
        print(f"No database found at '{DB_DIR}'. Run ingest.py first.")
        sys.exit(1)

    client = chromadb.PersistentClient(path=DB_DIR)
    embed_fn = SentenceTransformerEmbeddingFunction(model_name=EMBED_MODEL)

    try:
        collection = client.get_collection(name=args.collection, embedding_function=embed_fn)
    except Exception:
        print(f"Collection '{args.collection}' not found. Run ingest.py first.")
        sys.exit(1)

    print(f"\nQuery: {args.query}")
    print(f"Retrieving top {args.top_k} chunks from '{args.collection}'...\n")

    chunks = retrieve(collection, args.query, args.top_k)

    print("Retrieved chunks:")
    for i, c in enumerate(chunks, 1):
        heading = f" > {c['heading_path']}" if c["heading_path"] else ""
        print(f"  [{i}] (score={c['score']:.3f}) {c['url'][:70]}{heading}")
        print(f"       {c['text'][:120].strip()}...")

    if args.no_generate:
        return

    if not os.environ.get("GROQ_API_KEY"):
        print("\n[WARN] GROQ_API_KEY not set — skipping generation.")
        print("       Add it to .env or use --no-generate to suppress this warning.")
        return

    generate(args.query, chunks)


if __name__ == "__main__":
    main()