"""Entry point: startup checks, discovery, and the capture loop."""

import asyncio
import logging
import os
import subprocess
import time

from .capture import capture_loop, check_wyoming_connection
from .config import load_config
from .mqtt import create_mqtt_client, publish_audio_discovery, publish_discovery

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    logger.info("RTL-FM Transcriber starting (Wyoming Protocol)...")
    config = load_config()

    # Single source of truth for the timezone: log timestamps track the option
    # rather than run.sh exporting TZ separately.
    os.environ["TZ"] = config.get("timezone", "UTC")
    time.tzset()

    # Check RTL-SDR
    try:
        subprocess.run(["rtl_test", "-t"], capture_output=True, timeout=10, check=False)
        logger.info("RTL-SDR device check passed")
    except Exception as e:
        logger.error(f"RTL-SDR check failed: {e}")
        # Continue anyway, let rtl_fm fail if must

    mqtt_client = create_mqtt_client(config)

    # Publish HA Discovery
    publish_discovery(config, mqtt_client)
    
    # Publish audio sensor discovery if recording is enabled
    if config.get("audio_recording", False):
        publish_audio_discovery(config, mqtt_client)

    # Check Wyoming server connectivity on startup
    wyoming_available = asyncio.run(check_wyoming_connection(config))
    if not wyoming_available:
        logger.warning(
            "[Wyoming] Server not reachable at startup. "
            "Will retry connection on next transmission."
        )

    try:
        asyncio.run(capture_loop(config, mqtt_client))
    except KeyboardInterrupt:
        pass
    finally:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()


if __name__ == "__main__":
    main()
