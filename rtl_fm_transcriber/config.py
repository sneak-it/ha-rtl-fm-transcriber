"""Add-on options loading and Wyoming URL parsing."""

import json
import logging
import os
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

DEFAULT_WYOMING_PORT = 10300


def load_config():
    """Load configuration from Home Assistant add-on options."""
    options_path = "/data/options.json"

    if os.path.exists(options_path):
        with open(options_path) as f:
            return json.load(f)

    # Defaults
    return {
        "frequency": 155.1075,
        "squelch": 50,
        "whisper_url": "http://youriphere:10300",
        "mqtt_host": "core-mosquitto",
        "mqtt_port": 1883,
        "mqtt_topic": "radio/transcription",
        "mqtt_username": "",
        "mqtt_password": "",
        "gain": "auto",
        "ppm": 0,
        "bandpass_filter": True,
        "bandpass_low": 300,
        "bandpass_high": 3000,
        "timezone": "UTC",
        # Streaming segmentation options
        "silence_timeout": 2.0,
        "vad_warmup_ms": 150,
        "min_transmission_duration": 0.3,
        "max_transmission_duration": 120.0,
        # Audio recording options
        "audio_recording": False,
        "audio_retention_days": 7,
        "audio_max_files": 0,
    }


def parse_wyoming_url(url: str) -> tuple[str, int]:
    """Split a Wyoming server URL into (host, port)."""
    parsed = urlparse(url)
    return parsed.hostname or "localhost", parsed.port or DEFAULT_WYOMING_PORT
