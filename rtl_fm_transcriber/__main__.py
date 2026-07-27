"""Entry point: startup checks, discovery, and the capture loop."""

import asyncio
import contextlib
import logging
import os
import signal
import subprocess
import time

from .capture import capture_loop, check_wyoming_connection
from .config import ConfigError, load_config, validate_config
from .mqtt import (
    AVAILABILITY_OFFLINE,
    create_mqtt_client,
    publish_audio_discovery,
    publish_availability,
    publish_discovery,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def _request_stop(task: asyncio.Task, sig: signal.Signals) -> None:
    logger.info(f"Received {sig.name}, shutting down")
    task.cancel()


async def _run(config, mqtt_client) -> None:
    """Run the capture loop until it finishes or a stop signal arrives."""
    loop = asyncio.get_running_loop()
    capture = asyncio.create_task(capture_loop(config, mqtt_client))

    # Python runs as PID 1 here (init: false), where the default SIGTERM
    # disposition is to ignore it. Without these handlers every add-on stop or
    # update waited out Docker's kill timeout and then SIGKILLed, so nothing was
    # cleaned up and the USB device was sometimes left in a bad state.
    for sig in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, lambda s=sig: _request_stop(capture, s))

    with contextlib.suppress(asyncio.CancelledError):
        await capture


def main():
    logger.info("RTL-FM Transcriber starting (Wyoming Protocol)...")
    config = load_config()

    # Single source of truth for the timezone: log timestamps track the option
    # rather than run.sh exporting TZ separately.
    os.environ["TZ"] = config.get("timezone", "UTC")
    time.tzset()

    try:
        validate_config(config)
    except ConfigError as e:
        logger.error(f"Invalid configuration: {e}")
        raise SystemExit(1) from e

    # Check RTL-SDR
    try:
        subprocess.run(
            # Resolved via PATH, which is fixed inside the add-on image.
            ["rtl_test", "-t"],  # noqa: S607
            capture_output=True,
            timeout=10,
            check=False,
        )
        logger.info("RTL-SDR device check passed")
    except Exception as e:  # noqa: BLE001 - probe only; rtl_fm reports real faults
        logger.error(f"RTL-SDR check failed: {e}")
        # Continue anyway, let rtl_fm fail if must

    mqtt_client = create_mqtt_client(config)

    publish_discovery(config, mqtt_client)
    if config.get("audio_recording", False):
        publish_audio_discovery(config, mqtt_client)

    # Check Wyoming server connectivity on startup
    if not asyncio.run(check_wyoming_connection(config)):
        logger.warning(
            "[Wyoming] Server not reachable at startup. "
            "Will retry connection on next transmission."
        )

    try:
        asyncio.run(_run(config, mqtt_client))
    finally:
        publish_availability(mqtt_client, config, AVAILABILITY_OFFLINE)
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        logger.info("RTL-FM Transcriber stopped")


if __name__ == "__main__":
    main()
