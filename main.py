"""
RagForAll scraper — CLI entrypoint.

Usage:
    python main.py <url> [options]

Examples:
    python main.py https://ai.pydantic.dev/
    python main.py https://docs.python.org/3/ --max-concurrent 5
    python main.py https://example.com --no-js --output my_output.jsonl
"""

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scraper.url_discovery import discover_urls
from scraper.crawler import crawl_urls, PageResult
from scraper.filters import filter_results
from scraper.chunker import chunk_pages


def parse_args():
    parser = argparse.ArgumentParser(description="Scrape a website into RAG-ready chunks.")
    parser.add_argument("url", help="Starting URL to scrape")
    parser.add_argument("--max-concurrent", type=int, default=10,
                        help="Max parallel browser sessions (default: 10)")
    parser.add_argument("--memory-threshold", type=float, default=80.0,
                        help="Pause crawling if RAM %% exceeds this (default: 80)")
    parser.add_argument("--no-js", action="store_true",
                        help="Disable JavaScript rendering (faster for static sites)")
    parser.add_argument("--output", default=None,
                        help="Output .jsonl file path (default: output/<domain>_<timestamp>.jsonl)")
    parser.add_argument("--no-chunk", action="store_true",
                        help="Save full pages instead of RAG chunks")
    parser.add_argument("--limit", type=int, default=0,
                        help="Max URLs to crawl (0 = no limit)")
    parser.add_argument("--single", action="store_true",
                        help="Skip URL discovery and crawl only the given URL")
    return parser.parse_args()


def default_output_path(url: str) -> str:
    from urllib.parse import urlparse
    domain = urlparse(url).netloc.replace(".", "_")
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    os.makedirs("output", exist_ok=True)
    return os.path.join("output", f"{domain}_{ts}.jsonl")


def save_jsonl(records: list[dict], path: str):
    with open(path, "w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


async def main():
    args = parse_args()
    output_path = args.output or default_output_path(args.url)

    print(f"\n{'='*60}")
    print(f"  RagForAll Scraper")
    print(f"  Target : {args.url}")
    print(f"  Output : {output_path}")
    print(f"{'='*60}\n")

    # Step 1: Discover URLs
    if args.single:
        print("[1/4] Single-URL mode — skipping discovery.")
        urls = [args.url]
    else:
        print("[1/4] Discovering URLs...")
        urls = discover_urls(args.url)
        if args.limit > 0:
            urls = urls[: args.limit]
            print(f"  Limited to {args.limit} URLs.")

    print(f"  → {len(urls)} URLs to crawl.\n")

    if not urls:
        print("No URLs found. Exiting.")
        return

    # Step 2: Crawl
    print(f"[2/4] Crawling ({args.max_concurrent} parallel sessions, JS={'off' if args.no_js else 'on'})...")

    completed = 0
    failed = 0

    async def on_result(page: PageResult):
        nonlocal completed, failed
        if page.success:
            completed += 1
            print(f"  [OK {completed:>4}] {page.url[:80]}  ({len(page.content)} chars)")
        else:
            failed += 1
            print(f"  [FAIL   ] {page.url[:80]}  {page.error[:60]}")

    pages = await crawl_urls(
        urls,
        js_enabled=not args.no_js,
        max_concurrent=args.max_concurrent,
        memory_threshold=args.memory_threshold,
        on_result=on_result,
    )

    print(f"\n  → {completed} succeeded, {failed} failed.\n")

    # Step 3: Filter
    print("[3/4] Filtering low-quality pages...")
    kept, dropped = filter_results(pages)
    print(f"  → Kept {len(kept)}, dropped {len(dropped)} (stubs/errors).\n")

    # Step 4: Chunk or save full pages
    if args.no_chunk:
        print("[4/4] Saving full pages...")
        records = [
            {
                "url": p.url,
                "title": p.title,
                "content": p.content,
                "source_domain": p.source_domain,
                "crawled_at": p.crawled_at,
            }
            for p in kept
        ]
    else:
        print("[4/4] Chunking into RAG segments...")
        records = chunk_pages(kept)
        print(f"  → {len(records)} chunks generated.")

    save_jsonl(records, output_path)
    print(f"\n  Saved to: {output_path}")
    print(f"\n{'='*60}")
    print(f"  Done. {len(records)} records ready for RAG ingestion.")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    asyncio.run(main())