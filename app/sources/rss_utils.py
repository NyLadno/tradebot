"""Shared helpers for RSS/XML parsing."""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional

_HTML_TAG_RE = re.compile(r"<.*?>", re.DOTALL)
_IMG_SRC_RE = re.compile(r'<img[^>]+src=["\']([^"\']+)["\']')
_CATEGORY_SPLIT_RE = re.compile(r"\s*/\s*|\s*:\s*")


def clean_html(raw_html: str) -> str:
    """Strip HTML tags and collapse whitespace."""
    if not raw_html:
        return ""
    text = re.sub(_HTML_TAG_RE, "", raw_html)
    return re.sub(r"\s+", " ", text).strip()


def extract_image_url_from_html(html: str) -> Optional[str]:
    """Return the first img src found in HTML, if any."""
    if not html:
        return None
    match = _IMG_SRC_RE.search(html)
    return match.group(1) if match else None


def find_ns_text(
    item: ET.Element,
    tag_name: str,
    namespaces: Optional[Dict[str, str]] = None,
) -> Optional[str]:
    """Find element text by local name, with optional namespace fallbacks."""
    val = item.findtext(tag_name)
    if val is not None:
        return val.strip()

    if namespaces:
        for ns_uri in namespaces.values():
            val = item.findtext(f"{{{ns_uri}}}{tag_name}")
            if val is not None:
                return val.strip()

    for child in item:
        local_name = child.tag.split("}")[-1]
        if local_name == tag_name:
            return child.text.strip() if child.text else None
    return None


def normalize_categories(raw_categories: List[str]) -> List[str]:
    """Split and deduplicate category strings."""
    normalized: List[str] = []
    for cat in raw_categories:
        if not cat:
            continue
        for part in _CATEGORY_SPLIT_RE.split(cat):
            part_cleaned = part.strip()
            if part_cleaned and part_cleaned not in normalized:
                normalized.append(part_cleaned)
    return normalized
