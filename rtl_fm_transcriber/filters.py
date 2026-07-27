"""Whisper hallucination filtering."""


def is_hallucination(text):
    """Check for common Whisper hallucinations on silence."""
    # Phrase-based hallucinations
    phrase_hallucinations = [
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
    # Single-word hallucinations (only if they are the entire text)
    single_word_hallucinations = [
        "you",
    ]
    
    text_lower = text.lower().strip()

    # Empty or very short
    if len(text_lower) < 2:
        return True

    # Check for phrase-based hallucinations
    if any(h in text_lower for h in phrase_hallucinations):
        return True
        
    # Check for single-word hallucinations (exact match)
    return text_lower in single_word_hallucinations
