"""
URL discovery: tries sitemap.xml first, falls back to crawling the root page for links.
"""

import re
import requests
from urllib.parse import urljoin, urlparse
from xml.etree import ElementTree


# Matches path segments that are known non-English language codes,
# e.g. /de/, /fr/, /es/, /zh-cn/, etc.
NON_ENGLISH_PATH_RE = re.compile(
    r'(?:^|/)'
    r'(?:af|ar|az|be|bg|bn|bs|ca|cs|cy|da|de|el|eo|es|et|eu|fa|fi|fr|ga|gl'
    r'|gu|he|hi|hr|ht|hu|hy|id|is|it|ja|ka|kk|km|ko|ku|ky|lo|lt|lv|mk|ml'
    r'|mn|mr|ms|my|nb|ne|nl|no|pa|pl|ps|pt|ro|ru|si|sk|sl|sq|sr|sv|sw|ta'
    r'|te|tg|th|tk|tl|tr|tt|ug|uk|ur|uz|vi|xh|yi|zh|zu)'
    r'(?:/|$|-[a-z]{2,8}(?:/|$))',
    re.IGNORECASE,
)


def is_non_english_url(url: str) -> bool:
    """Return True if the URL path contains a known non-English language segment."""
    path = urlparse(url).path
    return bool(NON_ENGLISH_PATH_RE.search(path))


def _same_domain(base: str, url: str) -> bool:
    return urlparse(url).netloc == urlparse(base).netloc


def _clean_url(url: str) -> str:
    """Strip fragments and trailing slashes for deduplication."""
    parsed = urlparse(url)
    return parsed._replace(fragment="").geturl().rstrip("/")


def _is_content_url(url: str) -> bool:
    """Filter out asset URLs and non-content paths."""
    skip_exts = {".png", ".jpg", ".jpeg", ".gif", ".svg", ".ico", ".pdf",
                 ".zip", ".css", ".js", ".woff", ".woff2", ".ttf", ".mp4"}
    skip_patterns = ["/tag/", "/tags/", "/author/", "/authors/", "/page/",
                     "/feed", "?replytocom=", "wp-login", "wp-admin",
                     "/category/", "/categories/",
                     "/wp-json", "xmlrpc.php", "?rsd", "?oembed",
                     "/embed?url=", "wp-content/", "wp-includes/"]
    path = urlparse(url).path.lower()
    if any(path.endswith(ext) for ext in skip_exts):
        return False
    if any(p in url for p in skip_patterns):
        return False
    return True


def discover_from_sitemap(sitemap_url: str) -> list[str]:
    """Fetch and parse a sitemap (including sitemap index files)."""
    try:
        resp = requests.get(sitemap_url, timeout=(30, 30), headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"})
        resp.raise_for_status()
    except Exception as e:
        print(f"  [sitemap] Could not fetch {sitemap_url}: {e}")
        return []

    try:
        root = ElementTree.fromstring(resp.content)
    except ElementTree.ParseError as e:
        print(f"  [sitemap] XML parse error: {e}")
        return []

    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

    # Sitemap index — recurse into child sitemaps
    child_sitemaps = [el.text for el in root.findall(".//sm:sitemap/sm:loc", ns) if el.text]
    if child_sitemaps:
        urls = []
        for child in child_sitemaps:
            urls.extend(discover_from_sitemap(child))
        return urls

    # Regular sitemap
    urls = [el.text for el in root.findall(".//sm:url/sm:loc", ns) if el.text]
    return urls


def discover_from_page(root_url: str, max_links: int = 500) -> list[str]:
    """
    Fallback: fetch the root page and extract all same-domain links.
    Uses regex on raw HTML to avoid a full crawl4ai call for discovery.
    """
    try:
        resp = requests.get(root_url, timeout=(30, 30), headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"})
        resp.raise_for_status()
    except Exception as e:
        print(f"  [page discovery] Could not fetch {root_url}: {e}")
        return [root_url]

    raw_links = re.findall(r'href=["\']([^"\'#>]+)["\']', resp.text)
    seen = set()
    urls = []
    for href in raw_links:
        full = _clean_url(urljoin(root_url, href))
        if full not in seen and _same_domain(root_url, full) and _is_content_url(full):
            seen.add(full)
            urls.append(full)
        if len(urls) >= max_links:
            break
    return urls


def discover_urls(start_url: str) -> list[str]:
    """
    Main entry point. Tries sitemap.xml first, then supplements with page link
    extraction from the root URL to catch pages missing from the sitemap.
    Returns a deduplicated, filtered list of URLs to crawl.
    """
    parsed = urlparse(start_url)
    base = f"{parsed.scheme}://{parsed.netloc}"

    # Try common sitemap locations
    candidates = [
        urljoin(base, "/sitemap.xml"),
        urljoin(base, "/sitemap_index.xml"),
        urljoin(base, "/sitemap/sitemap.xml"),
    ]

    sitemap_urls = []
    for sitemap_url in candidates:
        print(f"  Trying sitemap: {sitemap_url}")
        sitemap_urls = discover_from_sitemap(sitemap_url)
        if sitemap_urls:
            print(f"  Found {len(sitemap_urls)} URLs via sitemap.")
            break

    if not sitemap_urls:
        print(f"  No sitemap found, using page link extraction only.")

    # Always supplement with link extraction from the root page to catch
    # pages that are linked but not listed in the sitemap.
    print(f"  Supplementing with link extraction from {base} ...")
    page_urls = discover_from_page(base)
    print(f"  Found {len(page_urls)} URLs via page links.")

    # Merge and deduplicate with filtering
    seen = set()
    result = []
    for url in sitemap_urls + page_urls:
        clean = _clean_url(url)
        if clean not in seen and _is_content_url(clean) and not is_non_english_url(clean):
            seen.add(clean)
            result.append(clean)

    return result