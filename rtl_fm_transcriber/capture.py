"""Capture loop: VAD segmentation, Wyoming streaming, transcript publication."""

import asyncio
import contextlib
import json
import logging
import os
import time

from .audio import compute_rms
from .config import parse_wyoming_url
from .filters import is_hallucination
from .mqtt import safe_publish
from .pipeline import cleanup_pipeline, read_stderr_pipeline, start_pipeline
from .recording import cleanup_old_recordings, save_audio_recording
from .timeutil import format_timestamp
from .transmission import (
    ACTIVE_STATES,
    BUFFER,
    END,
    HOLD,
    IDLE,
    IGNORE,
    PROMOTE,
    START,
    STREAMING,
    WAITING_FOR_END,
    TransmissionState,
)
from .vad import is_voice
from .wyoming_client import WyomingStreamingClient

logger = logging.getLogger(__name__)

# Pipeline restart with exponential backoff
MAX_PIPELINE_RETRIES = 5
INITIAL_RETRY_DELAY = 1.0  # seconds
MAX_RETRY_DELAY = 30.0  # seconds

READ_CHUNK_BYTES = 4096
READ_TIMEOUT = 0.05


async def capture_loop(config, mqtt_client):
    """Capture audio continuously, segment it into transmissions, transcribe each.

    One rtl_fm -> sox pipeline runs for as long as it stays healthy. Every chunk
    read from it goes through the transition table in `transmission`, which
    yields exactly one action, so each chunk is dispatched exactly once.

    Restarts the pipeline with exponential backoff on failure.
    """
    frequency_hz = int(config["frequency"] * 1_000_000)
    sample_rate = 16000
    # 12000 Hz sample rate for narrowband FM (12.5 kHz public safety channels)
    capture_rate = 12000

    bytes_per_sec = sample_rate * 2  # 16-bit mono
    warmup_bytes = int(config.get("vad_warmup_ms", 150) / 1000.0 * bytes_per_sec)

    logger.info("Starting persistent capture with streaming transcription")
    logger.info(f"  Frequency: {config['frequency']} MHz")
    logger.info(f"  Squelch: {config.get('squelch', 50)}")
    logger.info(f"  Silence timeout: {config.get('silence_timeout', 2.0)}s")
    logger.info(f"  Min transmission: {config.get('min_transmission_duration', 0.3)}s")
    logger.info(f"  Max transmission: {config.get('max_transmission_duration', 120.0)}s")

    retry_count = 0

    while True:
        processes = await start_pipeline(
            config, frequency_hz, sample_rate, capture_rate
        )
        if processes is None:
            retry_count += 1
            delay = min(INITIAL_RETRY_DELAY * (2 ** (retry_count - 1)), MAX_RETRY_DELAY)
            logger.error(f"Failed to start audio pipeline, retrying in {delay:.1f}s")
            await asyncio.sleep(delay)
            continue

        rtl_proc, sox_proc = processes
        retry_count = 0  # Reset on successful start

        logger.info("Pipeline started. Waiting for audio...")

        transmission = TransmissionState(config, warmup_bytes)
        pipeline_running = True
        stderr_reader_task = None

        try:
            stderr_reader_task = asyncio.create_task(
                read_stderr_pipeline(rtl_proc, sox_proc)
            )

            while pipeline_running:
                try:
                    chunk = await asyncio.wait_for(
                        sox_proc.stdout.read(READ_CHUNK_BYTES), timeout=READ_TIMEOUT
                    )
                except TimeoutError:
                    # No data available. The same transition table decides, but
                    # only END is actionable with no chunk to dispatch.
                    if (
                        transmission.state in ACTIVE_STATES
                        and transmission.next_action(False, time.time()) == END
                    ):
                        await _finish(transmission, config, mqtt_client)
                    continue
                except asyncio.CancelledError:
                    break

                if not chunk:
                    # EOF from sox - pipeline broke
                    logger.error("Audio pipeline died (EOF from sox)")
                    pipeline_running = False
                    break

                now = time.time()
                voice = is_voice(compute_rms(chunk))
                action = transmission.next_action(voice, now, incoming=len(chunk))

                if action == IGNORE:
                    continue

                if action == END:
                    await _finish(transmission, config, mqtt_client)
                    # A max-duration split lands mid-carrier, so the chunk that
                    # tripped it opens the next transmission rather than being
                    # dropped.
                    if voice:
                        transmission.begin(chunk, now)
                    continue

                if action == START:
                    logger.info("[VAD] Transmission started (squelch open)")
                    transmission.begin(chunk, now)
                    continue

                if action == BUFFER:
                    transmission.warmup_buffer.extend(chunk)
                    continue

                if action == PROMOTE:
                    transmission.warmup_buffer.extend(chunk)
                    if not await _open_session(transmission, config):
                        transmission.reset()
                    continue

                if action == HOLD:
                    if transmission.silence_start is None:
                        transmission.silence_start = now
                        transmission.state = WAITING_FOR_END
                        logger.debug(
                            "[VAD] Silence detected, ending in "
                            f"{transmission.silence_timeout}s unless voice resumes"
                        )
                    continue

                # STREAM: voice, with a session already open
                if transmission.state == WAITING_FOR_END:
                    logger.debug("[VAD] Voice resumed inside the silence window")
                transmission.state = STREAMING
                transmission.silence_start = None
                if config.get("audio_recording", False):
                    transmission.recording_buffer.extend(chunk)
                if not await _send_chunk(transmission, config, chunk):
                    transmission.reset()

        except Exception as e:
            logger.error(f"Capture loop error: {e}")
        finally:
            pipeline_running = False
            # Collect any remaining stderr before cleanup
            if stderr_reader_task:
                stderr_reader_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await stderr_reader_task

            # Dump any remaining stderr for diagnostics
            try:
                if rtl_proc.returncode is not None or rtl_proc.stderr:
                    remaining = await rtl_proc.stderr.read()
                    if remaining:
                        logger.error(
                            f"RTL_FM final stderr: {remaining.decode('utf-8', errors='replace').strip()}"
                        )
            except (Exception, AttributeError):
                pass

            try:
                if sox_proc.returncode is not None or sox_proc.stderr:
                    remaining = await sox_proc.stderr.read()
                    if remaining:
                        logger.error(
                            f"SOX final stderr: {remaining.decode('utf-8', errors='replace').strip()}"
                        )
            except (Exception, AttributeError):
                pass

            # Check return codes for diagnostics
            rtl_ret = rtl_proc.returncode
            sox_ret = sox_proc.returncode
            logger.error(
                f"Pipeline status - RTL_FM: {'exited' if rtl_ret is not None else 'running'}({rtl_ret}), SOX: {'exited' if sox_ret is not None else 'running'}({sox_ret})"
            )

            await transmission.wyoming.disconnect()
            await cleanup_pipeline(rtl_proc, sox_proc)

        # Pipeline failed - decide whether to restart
        if retry_count >= MAX_PIPELINE_RETRIES:
            logger.error(
                f"Max pipeline retries ({MAX_PIPELINE_RETRIES}) reached. Stopping."
            )
            break

        retry_count += 1
        delay = min(INITIAL_RETRY_DELAY * (2 ** (retry_count - 1)), MAX_RETRY_DELAY)
        logger.warning(
            f"Pipeline failed. Restarting in {delay:.1f}s (attempt {retry_count}/{MAX_PIPELINE_RETRIES})..."
        )
        await asyncio.sleep(delay)


async def _open_session(transmission: TransmissionState, config: dict) -> bool:
    """Connect to Wyoming, open a session, and flush the warmup buffer.

    Runs on every exit from WARMUP, so STREAMING is never entered without a
    session to stream into.
    """
    host, port = parse_wyoming_url(config["whisper_url"])
    conn_timeout = config.get("wyoming_connection_timeout", 10.0)

    if not transmission.wyoming.is_connected():
        logger.info(f"[Wyoming] Connecting to {host}:{port}...")
        connected = await transmission.wyoming.connect(
            host, port, timeout=conn_timeout
        )
        if not connected and not await transmission.wyoming.reconnect_with_backoff(
            config, host, port
        ):
            logger.warning(
                "[Wyoming] Unable to connect to server. Transmission will be skipped."
            )
            return False

    try:
        await transmission.wyoming.start_session(transmission.wyoming.client)
        await transmission.wyoming.send_chunk(bytes(transmission.warmup_buffer))
    except Exception as e:
        error_type = transmission.wyoming.classify_error(e)
        logger.error(f"[Wyoming] Failed to start session ({error_type}): {e}")
        return False

    transmission.state = STREAMING
    logger.debug(
        f"[VAD] Warmup complete ({len(transmission.warmup_buffer)} bytes), streaming"
    )
    return True


async def _send_chunk(
    transmission: TransmissionState, config: dict, chunk: bytes
) -> bool:
    """Stream one chunk, reconnecting once if the connection dropped."""
    try:
        await transmission.wyoming.send_chunk(chunk)
        return True
    except Exception as e:
        error_type = transmission.wyoming.classify_error(e)
        logger.error(f"[Wyoming] Failed to send chunk ({error_type}): {e}")

    if error_type not in ("connection_reset", "connection_lost"):
        return False

    host, port = parse_wyoming_url(config["whisper_url"])
    logger.info("[Wyoming] Connection lost mid-stream, attempting reconnect...")
    if not await transmission.wyoming.reconnect_with_backoff(config, host, port):
        logger.error("[Wyoming] Reconnection failed, skipping transmission")
        return False

    try:
        await transmission.wyoming.start_session(transmission.wyoming.client)
    except Exception as e:
        logger.error(f"[Wyoming] Failed to restart session after reconnect: {e}")
        return False

    logger.info("[Wyoming] Reconnected and session restarted")
    return True


async def _finish(
    transmission: TransmissionState, config: dict, mqtt_client
) -> None:
    """Close out a transmission and return to IDLE."""
    transmission.end_time = time.time()
    if transmission.duration >= transmission.max_duration:
        reason = f"max duration ({transmission.max_duration}s)"
    else:
        reason = f"silence timeout ({transmission.silence_timeout}s)"
    logger.info(
        f"[VAD] Transmission ended on {reason}, duration {transmission.duration:.1f}s"
    )
    await _end_transmission(transmission, config, mqtt_client)
    transmission.reset()


async def _end_transmission(transmission: TransmissionState, config: dict,
                           mqtt_client) -> None:
    """Send AudioStop, collect the transcript, publish it to MQTT.

    Args:
        transmission: The TransmissionState to finalize.
        config: Configuration dictionary.
        mqtt_client: MQTT client instance.
    """
    if transmission.state not in ACTIVE_STATES:
        return

    # Check minimum duration
    if transmission.duration < transmission.min_duration:
        logger.info(
            f"[VAD] Transmission too short ({transmission.duration:.2f}s < "
            f"{transmission.min_duration}s), discarding"
        )
        transmission.state = IDLE
        return

    # Send AudioStop and get transcript
    try:
        transcript = await transmission.wyoming.stop_session(timeout=30.0)
    except Exception as e:
        logger.error(f"[Wyoming] Error ending transmission: {e}")
        transmission.state = IDLE
        return

    if not transcript:
        logger.warning("[VAD] No transcript received for transmission")
        transmission.state = IDLE
        return

    clean_text = transcript.strip()

    # Audio recording and MQTT publication
    audio_file_path: str | None = None
    audio_file_name: str | None = None

    # Check for hallucinations
    if is_hallucination(clean_text):
        logger.info(f"Filtered hallucination: '{clean_text}'")
    else:
        logger.info(f"Transcription [{transmission.duration:.1f}s]: {clean_text}")

        # Save audio recording if enabled
        if config.get("audio_recording", False) and transmission.recording_buffer:
            timestamp_str = format_timestamp(config.get("timezone", "UTC"))

            # Combine warmup buffer with recording buffer for complete audio
            full_audio = bytes(transmission.warmup_buffer) + bytes(transmission.recording_buffer)

            audio_file_path = save_audio_recording(
                full_audio,
                str(config["frequency"]),
                timestamp_str,
            )

            if audio_file_path:
                audio_file_name = os.path.basename(audio_file_path)

                # Run retention cleanup
                cleanup_old_recordings(
                    config.get("audio_retention_days", 7),
                    config.get("audio_max_files", 0),
                )

        # Publish to MQTT
        message = {
            "text": clean_text,
            "frequency": str(config["frequency"]),
            "timestamp": format_timestamp(config.get("timezone", "UTC")),
        }

        # Add audio fields if recording is enabled and available
        if config.get("audio_recording", False) and audio_file_name:
            message["audio_file"] = f"radio-audio/{audio_file_name}"
            message["audio_url"] = f"media-source://media_source/local/radio-audio/{audio_file_name}"

        safe_publish(
            mqtt_client, config["mqtt_topic"], json.dumps(message),
            qos=1, label="transcription",
        )

    transmission.state = IDLE


async def check_wyoming_connection(config: dict) -> bool:
    """Check Wyoming server connectivity on startup with reconnection retry.

    Args:
        config: Configuration dictionary.

    Returns:
        True if connected, False after max retries.
    """
    wyoming_host, wyoming_port = parse_wyoming_url(config["whisper_url"])
    conn_timeout = config.get("wyoming_connection_timeout", 10.0)
    max_attempts = config.get("wyoming_reconnect_max_attempts", 3)
    base_delay = config.get("wyoming_reconnect_delay", 1.0)

    client = WyomingStreamingClient()

    for attempt in range(1, max_attempts + 1):
        delay = base_delay * (2 ** (attempt - 1))  # 1s, 2s, 4s...
        logger.info(
            f"[Wyoming] Startup connection attempt {attempt}/{max_attempts} "
            f"in {delay:.1f}s to {wyoming_host}:{wyoming_port}"
        )
        await asyncio.sleep(delay)

        connected = await client.connect(wyoming_host, wyoming_port, timeout=conn_timeout)
        if connected:
            logger.info(
                f"[Wyoming] Startup connection successful on attempt {attempt}"
            )
            await client.disconnect()
            return True
        else:
            logger.warning(
                f"[Wyoming] Startup connection attempt {attempt}/{max_attempts} failed"
            )

    logger.error(
        f"[Wyoming] All {max_attempts} startup connection attempts failed. "
        f"The add-on will continue but transcription will not work until "
        f"the Wyoming server is available."
    )
    return False
