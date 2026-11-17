"""Whisper hallucination filtering."""

import re

# Matched on word boundaries, so "subscribe" does not catch "subscriber",
# which is routine radio traffic.
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
    # Whisper punctuates its output, so "You." must match the same as "you".
    bare = text_lower.strip(" .,!?")

    # Empty or very short
    if len(bare) < 2:
        return True

    if any(p.search(text_lower) for p in _PATTERNS):
        return True

    return bare in _EXACT
