"""WAV recording of transmissions and retention cleanup."""

import logging
import os
import time

from .audio import create_wav_header

logger = logging.getLogger(__name__)


AUDIO_SAVE_DIR: str = "/config/www/radio-audio"


def save_audio_recording(
    audio_data: bytes,
    frequency: str,
    timestamp_str: str,
) -> str | None:
    """Save audio recording as a WAV file.
    
    Args:
        audio_data: Raw PCM16 audio bytes.
        frequency: The radio frequency as a string (e.g., "155.1075").
        timestamp_str: ISO-format timestamp string for the filename.
        
    Returns:
        Relative path to the saved file, or None on failure.
    """
    try:
        os.makedirs(AUDIO_SAVE_DIR, exist_ok=True)
    except OSError as e:
        logger.error(f"[Audio] Failed to create audio directory {AUDIO_SAVE_DIR}: {e}")
        return None
    
    # Generate filename: YYYYMMDD-HHMMSS-XXXX.XX.wav
    # Use the timestamp string to derive a sortable name
    ts_clean = ""
    freq_safe = str(frequency).replace(".", "_")
    try:
        # Parse ISO timestamp to get a clean filename component
        # Handle formats like "2026-01-25T18:15:00+00:00" or "2026-01-25T18:15:00Z"
        ts_clean = timestamp_str.replace(":", "").replace("-", "").replace("Z", "").split("+")[0]
        # Remove trailing fractional digits beyond 6 (microseconds)
        if "." in ts_clean:
            ts_clean = ts_clean.split(".")[0]
        filename = f"{ts_clean}-{freq_safe}.wav"
    except Exception:
        # Fallback to epoch-based naming
        ts_clean = str(int(time.time()))
        filename = f"{ts_clean}-{freq_safe}.wav"
    
    filepath = os.path.join(AUDIO_SAVE_DIR, filename)
    
    # Handle potential filename collisions
    counter = 0
    while os.path.exists(filepath):
        counter += 1
        filepath = os.path.join(AUDIO_SAVE_DIR, f"{ts_clean}-{freq_safe}_{counter}.wav")
    
    try:
        wav_header = create_wav_header(len(audio_data))
        with open(filepath, "wb") as f:
            f.write(wav_header)
            f.write(audio_data)
        logger.info(f"[Audio] Saved recording: {filepath} ({len(audio_data)} bytes)")
        return filepath
    except OSError as e:
        logger.error(f"[Audio] Failed to save audio file {filepath}: {e}")
        return None


def cleanup_old_recordings(retention_days: int, max_files: int) -> None:
    """Remove old audio recordings based on retention policy.
    
    Args:
        retention_days: Number of days to keep recordings.
        max_files: Maximum number of files to keep (0 = unlimited).
    """
    if not os.path.isdir(AUDIO_SAVE_DIR):
        return
    
    now = time.time()
    retention_seconds = retention_days * 86400  # seconds per day
    files: list[tuple[str, float]] = []
    
    try:
        for filename in os.listdir(AUDIO_SAVE_DIR):
            if not filename.endswith(".wav"):
                continue
            filepath = os.path.join(AUDIO_SAVE_DIR, filename)
            try:
                mtime = os.path.getmtime(filepath)
                files.append((filepath, mtime))
            except OSError:
                continue
    except OSError as e:
        logger.error(f"[Audio] Failed to list audio directory {AUDIO_SAVE_DIR}: {e}")
        return
    
    if not files:
        return
    
    # Remove files older than retention period
    removed = 0
    for filepath, mtime in files:
        if now - mtime > retention_seconds:
            try:
                os.remove(filepath)
                logger.debug(f"[Audio] Removed old recording: {filepath}")
                removed += 1
            except OSError as e:
                logger.warning(f"[Audio] Failed to remove {filepath}: {e}")
    
    if removed:
        logger.info(f"[Audio] Cleaned up {removed} old recording(s)")
    
    # Enforce max files limit if set
    if max_files <= 0:
        return
    
    # Sort by modification time (oldest first)
    files.sort(key=lambda x: x[1])
    
    while len(files) > max_files:
        oldest_path, _ = files.pop(0)
        try:
            os.remove(oldest_path)
            logger.debug(f"[Audio] Removed excess recording: {oldest_path}")
        except OSError as e:
            logger.warning(f"[Audio] Failed to remove {oldest_path}: {e}")
