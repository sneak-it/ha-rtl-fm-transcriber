"""Whisper hallucination filtering."""

import re

# Matched on word boundaries: plain substring matching discarded legitimate
# traffic, because "subscribe" is inside "subscriber".
_PHRASES = [
        "thank you for watching",
        "thanks for watching",
        "subs by",
        "subscribe",
        "amara.org",
        "copyright",
        "please subscribe",
        "like and subscribe",
        "transcribed by",
        "subtitles by",
]

# Hallucinations only when they are the entire transcript
_EXACT = frozenset({"you"})

_PATTERNS = [re.compile(rf"\b{re.escape(p)}\b") for p in _PHRASES]


def is_hallucination(text):
    """Check for common Whisper hallucinations on silence."""
    text_lower = text.lower().strip()

    # Empty or very short
    if len(text_lower) < 2:
        return True

    if any(p.search(text_lower) for p in _PATTERNS):
        return True

    return text_lower in _EXACT
