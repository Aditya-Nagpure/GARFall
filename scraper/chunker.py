"""
Splits cleaned markdown pages into RAG-ready chunks at heading boundaries.
Target: 400-600 tokens per chunk (approximated as ~400-600 words × 1.3).
"""

import re
from .crawler import PageResult


TARGET_CHARS = 1800     # ~450 tokens
OVERLAP_CHARS = 200     # carry last ~50 tokens into next chunk


def _split_by_headings(text: str) -> list[tuple[str, str]]:
    """
    Split markdown into (heading_path, content) sections.
    Returns a list where each item is a heading context + its content block.
    """
    heading_re = re.compile(r'^(#{1,3})\s+(.+)$', re.MULTILINE)
    sections = []
    last_end = 0
    heading_stack: list[str] = []

    for match in heading_re.finditer(text):
        # Save content before this heading
        if last_end < match.start():
            block = text[last_end:match.start()].strip()
            if block:
                sections.append((" > ".join(heading_stack), block))

        level = len(match.group(1))
        title = match.group(2).strip()

        # Update heading stack
        heading_stack = heading_stack[: level - 1]
        heading_stack.append(title)
        last_end = match.end()

    # Trailing content after last heading
    tail = text[last_end:].strip()
    if tail:
        sections.append((" > ".join(heading_stack), tail))

    return sections


def _chunk_text(heading_path: str, text: str) -> list[dict]:
    """Break a single section into fixed-size chunks with overlap."""
    chunks = []
    start = 0
    while start < len(text):
        end = start + TARGET_CHARS
        chunk_text = text[start:end].strip()
        if chunk_text:
            chunks.append({"heading_path": heading_path, "text": chunk_text})
        start = end - OVERLAP_CHARS  # overlap
        if start >= len(text):
            break
    return chunks


def chunk_page(page: PageResult) -> list[dict]:
    """
    Convert a PageResult into a list of RAG chunk dicts, each with:
      url, title, heading_path, text, source_domain, crawled_at
    """
    sections = _split_by_headings(page.content)

    # If no headings found, treat entire content as one section
    if not sections:
        sections = [("", page.content)]

    chunks = []
    for heading_path, text in sections:
        for chunk in _chunk_text(heading_path, text):
            chunks.append({
                "url": page.url,
                "title": page.title,
                "heading_path": chunk["heading_path"],
                "text": chunk["text"],
                "source_domain": page.source_domain,
                "crawled_at": page.crawled_at,
            })

    return chunks


def chunk_pages(pages: list[PageResult]) -> list[dict]:
    """Chunk all pages and return a flat list of chunk dicts."""
    all_chunks = []
    for page in pages:
        all_chunks.extend(chunk_page(page))
    return all_chunks