"""Add-on options loading, validation, and Wyoming URL parsing."""

import json
import logging
import os
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

DEFAULT_WYOMING_PORT = 10300

# rtl-sdr tuning range, in MHz (R820T/R828D front ends)
FREQ_MIN_MHZ = 24.0
FREQ_MAX_MHZ = 1766.0


class ConfigError(Exception):
    """Raised for an option value the add-on cannot run with."""


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
        "whisper_url": "tcp://homeassistant.local:10300",
        "mqtt_host": "core-mosquitto",
        "mqtt_port": 1883,
        "mqtt_topic": "radio/transcription",
        "mqtt_username": "",
        "mqtt_password": "",
        "mqtt_tls": False,
        "mqtt_tls_ca": "",
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
        # Wyoming connection settings
        "wyoming_connection_timeout": 10.0,
        "wyoming_read_timeout": 30.0,
        "wyoming_reconnect_max_attempts": 3,
        "wyoming_reconnect_delay": 1.0,
        # Audio recording options
        "audio_recording": False,
        "audio_public_www": False,
        "audio_retention_days": 7,
        "audio_max_files": 0,
    }


def parse_wyoming_url(url: str) -> tuple[str, int]:
    """Split a Wyoming server URL into (host, port).

    Accepts a bare "host:port" as well as a full URL. A scheme-less value used
    to parse with hostname None and silently fall back to localhost.

    Raises:
        ConfigError: if no host can be determined.
    """
    raw = (url or "").strip()
    if not raw:
        raise ConfigError("whisper_url is empty")
    if "//" not in raw:
        raw = f"tcp://{raw}"

    parsed = urlparse(raw)
    if not parsed.hostname:
        raise ConfigError(
            f"Could not determine a host from whisper_url {url!r}. "
            f"Use host:port, for example 10.0.0.5:10300."
        )
    return parsed.hostname, parsed.port or DEFAULT_WYOMING_PORT


def validate_config(config: dict) -> None:
    """Check option values at startup, failing with actionable messages.

    Raises:
        ConfigError: on the first invalid value found.
    """
    host, port = parse_wyoming_url(config.get("whisper_url", ""))
    logger.info(f"[Config] Wyoming server: {host}:{port}")

    freq = config.get("frequency")
    if not isinstance(freq, (int, float)) or not FREQ_MIN_MHZ <= freq <= FREQ_MAX_MHZ:
        raise ConfigError(
            f"frequency {freq!r} is outside the tunable range "
            f"{FREQ_MIN_MHZ}-{FREQ_MAX_MHZ} MHz"
        )

    gain = config.get("gain", "auto")
    if str(gain).strip().lower() != "auto":
        try:
            float(gain)
        except (TypeError, ValueError) as e:
            raise ConfigError(
                f"gain must be a number or \"auto\", got {gain!r}"
            ) from e

    low = config.get("bandpass_low", 300)
    high = config.get("bandpass_high", 3000)
    if config.get("bandpass_filter", True) and low >= high:
        raise ConfigError(
            f"bandpass_low ({low}) must be below bandpass_high ({high})"
        )

    min_dur = config.get("min_transmission_duration", 0.3)
    max_dur = config.get("max_transmission_duration", 120.0)
    if min_dur >= max_dur:
        raise ConfigError(
            f"min_transmission_duration ({min_dur}) must be below "
            f"max_transmission_duration ({max_dur})"
        )

    if config.get("audio_public_www", False):
        logger.warning(
            "[Config] audio_public_www is on: recordings go to /config/www and "
            "are served at /local/ with no authentication. Anyone who can reach "
            "Home Assistant can download them."
        )
