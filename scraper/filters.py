"""
Post-crawl filters: drop low-quality pages before saving.
"""

import unicodedata

from .crawler import PageResult
from .url_discovery import is_non_english_url

try:
    from langdetect import detect, LangDetectException
    _LANGDETECT_AVAILABLE = True
except ImportError:
    _LANGDETECT_AVAILABLE = False


MIN_CONTENT_CHARS = 200   # pages shorter than this are stubs/errors
MIN_WORD_COUNT = 40       # minimum words in fit_markdown

# Ratio of non-Latin/non-ASCII characters above which we reject the page
_NON_LATIN_THRESHOLD = 0.15


def _word_count(text: str) -> int:
    return len(text.split())


def _non_latin_ratio(text: str) -> float:
    """Return fraction of alphabetic chars that are outside the Latin/ASCII range."""
    alpha_chars = [c for c in text if c.isalpha()]
    if not alpha_chars:
        return 0.0
    non_latin = sum(
        1 for c in alpha_chars
        if unicodedata.category(c) == 'Lo'           # CJK, Arabic, Hebrew, etc.
        or '\u0400' <= c <= '\u04FF'                  # Cyrillic
        or '\u0370' <= c <= '\u03FF'                  # Greek
    )
    return non_latin / len(alpha_chars)


def is_english(text: str) -> bool:
    """
    Return True only if the text is English.

    Fast path: reject pages where >15% of alphabetic characters are from
    non-Latin scripts (CJK, Cyrillic, Arabic, Greek, etc.).

    Slow path: if langdetect is installed, use it to confirm the language
    is English ('en'), catching European Latin-script languages.
    """
    if _non_latin_ratio(text) > _NON_LATIN_THRESHOLD:
        return False

    if _LANGDETECT_AVAILABLE:
        sample = text[:3000]  # langdetect only needs a sample
        try:
            return detect(sample) == 'en'
        except LangDetectException:
            pass  # too short or ambiguous — fall through and accept

    return True


def is_useful(page: PageResult) -> bool:
    """Return True if the page has enough content to be worth indexing."""
    if not page.success:
        return False
    if len(page.content) < MIN_CONTENT_CHARS:
        return False
    if _word_count(page.content) < MIN_WORD_COUNT:
        return False
    if is_non_english_url(page.url):
        return False
    if not is_english(page.content):
        return False
    return True


def filter_results(pages: list[PageResult]) -> tuple[list[PageResult], list[PageResult]]:
    """
    Split results into (kept, dropped).
    Dropped pages are ones that failed or have too little content.
    """
    kept, dropped = [], []
    for page in pages:
        (kept if is_useful(page) else dropped).append(page)
    return kept, dropped
