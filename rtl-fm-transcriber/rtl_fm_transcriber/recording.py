"""WAV recording of transmissions and retention cleanup."""

import logging
import os
import time
from datetime import datetime

from .audio import create_wav_header

logger = logging.getLogger(__name__)


SUBDIR = "radio-audio"

# Default. /media is served only through Home Assistant's authenticated
# endpoints and is what the media-source:// URI in the MQTT payload resolves to.
MEDIA_DIR = f"/media/{SUBDIR}"
# Opt-in. This is Home Assistant's own config directory, mounted at
# /homeassistant by the homeassistant_config map (the legacy config map, which
# put it at /config, was deprecated in Supervisor 2023.11). Its www folder is
# served at /local/ with NO authentication, so anyone who can reach Home
# Assistant can enumerate and download recordings. Only useful because it allows
# inline <audio> playback in a dashboard card.
WWW_DIR = f"/homeassistant/www/{SUBDIR}"


def audio_dir(config) -> str:
    """Directory recordings are written to, per the audio_public_www option."""
    return WWW_DIR if config.get("audio_public_www", False) else MEDIA_DIR


def audio_url(config, filename: str) -> str:
    """URL for a saved recording, matching whichever directory it went to."""
    if config.get("audio_public_www", False):
        return f"/local/{SUBDIR}/{filename}"
    return f"media-source://media_source/local/{SUBDIR}/{filename}"


def recording_filename(stamp: datetime, frequency: str, counter: int = 0) -> str:
    """Build a sortable WAV filename: YYYYMMDD-HHMMSS-<freq>.wav.

    Formatted straight from the datetime. Scrubbing an ISO string instead used to
    strip the minus sign from a negative UTC offset and fuse the offset digits
    into the time, so every US-timezone user got names like
    20260727T1215000400-155_1075.wav.
    """
    freq_safe = str(frequency).replace(".", "_")
    suffix = f"_{counter}" if counter else ""
    return f"{stamp.strftime('%Y%m%d-%H%M%S')}-{freq_safe}{suffix}.wav"


def save_audio_recording(
    audio_data: bytes,
    frequency: str,
    stamp: datetime,
    save_dir: str = MEDIA_DIR,
) -> str | None:
    """Save audio recording as a WAV file.

    Args:
        audio_data: Raw PCM16 audio bytes.
        frequency: The radio frequency as a string (e.g., "155.1075").
        stamp: Local time of the transmission, used for the filename.
        save_dir: Directory to write into.

    Returns:
        Path to the saved file, or None on failure.
    """
    try:
        os.makedirs(save_dir, exist_ok=True)
    except OSError as e:
        logger.error(f"[Audio] Failed to create audio directory {save_dir}: {e}")
        return None

    counter = 0
    filepath = os.path.join(save_dir, recording_filename(stamp, frequency))
    while os.path.exists(filepath):
        counter += 1
        filepath = os.path.join(
            save_dir, recording_filename(stamp, frequency, counter)
        )
    
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


def cleanup_old_recordings(
    retention_days: int, max_files: int, save_dir: str = MEDIA_DIR
) -> None:
    """Remove old audio recordings based on retention policy.
    
    Args:
        retention_days: Number of days to keep recordings.
        max_files: Maximum number of files to keep (0 = unlimited).
        save_dir: Directory to prune.
    """
    if not os.path.isdir(save_dir):
        return
    
    now = time.time()
    retention_seconds = retention_days * 86400  # seconds per day
    files: list[tuple[str, float]] = []
    
    try:
        for filename in os.listdir(save_dir):
            if not filename.endswith(".wav"):
                continue
            filepath = os.path.join(save_dir, filename)
            try:
                mtime = os.path.getmtime(filepath)
                files.append((filepath, mtime))
            except OSError:
                continue
    except OSError as e:
        logger.error(f"[Audio] Failed to list audio directory {save_dir}: {e}")
        return
    
    if not files:
        return
    
    # Remove files older than the retention period, keeping the survivors so the
    # max_files pass below does not try to delete them again.
    remaining = []
    removed = 0
    for filepath, mtime in files:
        if now - mtime > retention_seconds:
            try:
                os.remove(filepath)
                logger.debug(f"[Audio] Removed old recording: {filepath}")
                removed += 1
            except OSError as e:
                logger.warning(f"[Audio] Failed to remove {filepath}: {e}")
        else:
            remaining.append((filepath, mtime))

    if removed:
        logger.info(f"[Audio] Cleaned up {removed} old recording(s)")

    if max_files <= 0:
        return

    # Oldest first
    remaining.sort(key=lambda x: x[1])
    excess = 0
    while len(remaining) > max_files:
        oldest_path, _ = remaining.pop(0)
        try:
            os.remove(oldest_path)
            excess += 1
        except OSError as e:
            logger.warning(f"[Audio] Failed to remove {oldest_path}: {e}")

    if excess:
        logger.info(f"[Audio] Removed {excess} recording(s) over the max_files limit")
