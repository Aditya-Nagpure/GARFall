"""
Core crawler: uses arun_many with MemoryAdaptiveDispatcher for parallel crawling.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import urlparse

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.async_dispatcher import MemoryAdaptiveDispatcher, RateLimiter
from crawl4ai.content_filter_strategy import PruningContentFilter
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator


@dataclass
class PageResult:
    url: str
    title: str
    content: str          # fit_markdown (pruned)
    raw_content: str      # raw_markdown (unpruned)
    crawled_at: str
    source_domain: str
    success: bool
    error: str = ""


def _extract_title(markdown: str) -> str:
    """Pull the first H1 from markdown, fallback to first non-empty line."""
    for line in markdown.splitlines():
        line = line.strip()
        if line.startswith("# "):
            return line[2:].strip()
        if line and not line.startswith("#"):
            return line[:120]
    return ""


def build_configs(js_enabled: bool = True) -> tuple[BrowserConfig, CrawlerRunConfig]:
    browser_config = BrowserConfig(
        headless=True,
        java_script_enabled=js_enabled,
        extra_args=["--disable-gpu", "--disable-dev-shm-usage", "--no-sandbox", "--disable-quic", "--disable-http2"],
    )

    md_generator = DefaultMarkdownGenerator(
        content_filter=PruningContentFilter(
            threshold=0.2,
            threshold_type="fixed",
            min_word_threshold=3,
        )
    )

    run_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        markdown_generator=md_generator,
        page_timeout=60000,       # 60s per page
        wait_until="domcontentloaded",
        stream=True,              # stream results as they complete
    )

    return browser_config, run_config


async def crawl_urls(
    urls: list[str],
    js_enabled: bool = True,
    max_concurrent: int = 10,
    memory_threshold: float = 80.0,
    on_result=None,
) -> list[PageResult]:
    """
    Crawl a list of URLs in parallel using MemoryAdaptiveDispatcher.

    Args:
        urls:             List of URLs to crawl.
        js_enabled:       Whether to enable JavaScript rendering.
        max_concurrent:   Max simultaneous browser sessions.
        memory_threshold: Pause new sessions if RAM usage exceeds this %.
        on_result:        Optional async callback(PageResult) called for each completed page.

    Returns:
        List of PageResult objects (success and failure both included).
    """
    if not urls:
        return []

    browser_config, run_config = build_configs(js_enabled)

    dispatcher = MemoryAdaptiveDispatcher(
        memory_threshold_percent=memory_threshold,
        max_session_permit=max_concurrent,
        rate_limiter=RateLimiter(
            base_delay=(0.5, 1.5),  # random delay between requests
            max_delay=30.0,
            max_retries=2,
        ),
    )

    results: list[PageResult] = []

    async with AsyncWebCrawler(config=browser_config) as crawler:
        stream = await crawler.arun_many(urls, config=run_config, dispatcher=dispatcher)

        async for raw in stream:
            domain = urlparse(raw.url).netloc
            now = datetime.now(timezone.utc).isoformat()

            if raw.success and raw.markdown:
                fit = raw.markdown.fit_markdown or ""
                raw_md = raw.markdown.raw_markdown or ""
                title = _extract_title(fit or raw_md)

                page = PageResult(
                    url=raw.url,
                    title=title,
                    content=fit,
                    raw_content=raw_md,
                    crawled_at=now,
                    source_domain=domain,
                    success=True,
                )
            else:
                page = PageResult(
                    url=raw.url,
                    title="",
                    content="",
                    raw_content="",
                    crawled_at=now,
                    source_domain=domain,
                    success=False,
                    error=raw.error_message or "Unknown error",
                )

            results.append(page)

            if on_result:
                await on_result(page)

    return results
