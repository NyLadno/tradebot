"""Relevance filters for Tatneft / oil-sector news."""

from __future__ import annotations

import re

from app.config import RELEVANT_KEYWORDS

FILTER_PATTERN = re.compile("|".join(RELEVANT_KEYWORDS), re.IGNORECASE)


def is_relevant_to_tatneft(title: str, description: str) -> bool:
    """
    Return True if title or description mentions Tatneft, competitors,
    or oil-market factors that may affect stock quotes.
    """
    text_to_check = f"{title or ''} {description or ''}"
    return bool(FILTER_PATTERN.search(text_to_check))
