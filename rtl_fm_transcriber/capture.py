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
from .recording import audio_dir, audio_url, cleanup_old_recordings, save_audio_recording
from .timeutil import format_timestamp
from .transmission import (
    ACTIVE_STATES,
    BUFFER,
    END,
    HOLD,
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
# How long to let in-flight transcriptions finish during pipeline teardown.
FINALIZER_DRAIN_TIMEOUT = 35.0
# A pipeline must run at least this long to count as healthy for backoff.
HEALTHY_RUNTIME = 30.0


async def capture_loop(config, mqtt_client):
    """Supervise the capture pipeline, restarting it with backoff on failure."""
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
            ran_for = 0.0
        else:
            rtl_proc, sox_proc = processes
            logger.info("Pipeline started. Waiting for audio...")
            started = time.monotonic()
            try:
                await _run_pipeline(
                    rtl_proc, sox_proc, config, mqtt_client, warmup_bytes
                )
            except asyncio.CancelledError:
                await cleanup_pipeline(rtl_proc, sox_proc)
                raise
            except Exception as e:
                logger.error(f"Capture loop error: {e}")
            finally:
                ran_for = time.monotonic() - started
            logger.error(
                f"Pipeline status - RTL_FM: {_proc_status(rtl_proc)}, "
                f"SOX: {_proc_status(sox_proc)}"
            )
            await cleanup_pipeline(rtl_proc, sox_proc)

        # Only a pipeline that actually ran counts as healthy. Resetting on a
        # successful spawn instead meant a missing or claimed dongle, where
        # rtl_fm launches fine and exits immediately, looped at 1s forever and
        # never reached the backoff.
        if ran_for >= HEALTHY_RUNTIME:
            logger.info(f"Pipeline ran {ran_for:.0f}s before failing, resetting backoff")
            retry_count = 0

        retry_count += 1
        delay = min(INITIAL_RETRY_DELAY * (2 ** (retry_count - 1)), MAX_RETRY_DELAY)

        # Keep retrying rather than exiting: this add-on has no listening port,
        # so it cannot use the Supervisor watchdog, and exiting would leave it
        # stopped until someone noticed. Past the retry threshold the log goes
        # from warning to a loud, actionable error on every attempt.
        if retry_count > MAX_PIPELINE_RETRIES:
            logger.error(
                f"Audio pipeline has failed {retry_count} times without running "
                f"for {HEALTHY_RUNTIME:.0f}s. Check that the RTL-SDR dongle is "
                f"connected and not claimed by another process. "
                f"Retrying in {delay:.0f}s."
            )
        else:
            logger.warning(
                f"Restarting pipeline in {delay:.1f}s "
                f"(attempt {retry_count}/{MAX_PIPELINE_RETRIES})"
            )
        await asyncio.sleep(delay)


def _proc_status(proc) -> str:
    """Short description of a subprocess's exit state, for diagnostics."""
    rc = proc.returncode
    return f"exited({rc})" if rc is not None else "running"


async def _run_pipeline(rtl_proc, sox_proc, config, mqtt_client, warmup_bytes):
    """Read and segment audio until the pipeline dies.

    Every chunk goes through the transition table in `transmission`, which yields
    exactly one action, so no chunk is dropped or dispatched twice.
    """
    transmission = TransmissionState(config, warmup_bytes)
    # Owned here rather than by TransmissionState: at end of transmission the
    # client is handed to a background finalizer, which closes it.
    wyoming = WyomingStreamingClient()
    finalizers: set[asyncio.Task] = set()
    stderr_task = asyncio.create_task(read_stderr_pipeline(rtl_proc, sox_proc))

    try:
        while True:
            try:
                chunk = await asyncio.wait_for(
                    sox_proc.stdout.read(READ_CHUNK_BYTES), timeout=READ_TIMEOUT
                )
            except TimeoutError:
                # No data available. The same transition table decides, but only
                # END is actionable with no chunk to dispatch.
                if (
                    transmission.state in ACTIVE_STATES
                    and transmission.next_action(False, time.time()) == END
                ):
                    wyoming = await _finish(
                        transmission, wyoming, config, mqtt_client, finalizers
                    )
                continue

            if not chunk:
                logger.error("Audio pipeline died (EOF from sox)")
                return

            now = time.time()
            voice = is_voice(compute_rms(chunk))
            action = transmission.next_action(voice, now, incoming=len(chunk))

            if action == IGNORE:
                continue

            if action == END:
                wyoming = await _finish(
                    transmission, wyoming, config, mqtt_client, finalizers
                )
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
                if not await _open_session(transmission, wyoming, config):
                    await wyoming.disconnect()
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
            if not await _send_chunk(wyoming, config, chunk):
                await wyoming.disconnect()
                transmission.reset()
    finally:
        stderr_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await stderr_task

        # Let in-flight transcriptions finish rather than orphaning them.
        if finalizers:
            logger.info(f"Waiting for {len(finalizers)} pending transcription(s)")
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    asyncio.gather(*finalizers, return_exceptions=True),
                    timeout=FINALIZER_DRAIN_TIMEOUT,
                )
        await wyoming.disconnect()


async def _open_session(
    transmission: TransmissionState, wyoming: WyomingStreamingClient, config: dict
) -> bool:
    """Connect to Wyoming, open a session, and flush the warmup buffer.

    Runs on every exit from WARMUP, so STREAMING is never entered without a
    session to stream into.
    """
    host, port = parse_wyoming_url(config["whisper_url"])

    if not wyoming.is_connected() and not await wyoming.reconnect_with_backoff(
        config, host, port
    ):
        logger.warning(
            "[Wyoming] Unable to connect to server. Transmission will be skipped."
        )
        return False

    try:
        await wyoming.start_session(wyoming.client)
        await wyoming.send_chunk(bytes(transmission.warmup_buffer))
    except Exception as e:
        logger.error(
            f"[Wyoming] Failed to start session ({wyoming.classify_error(e)}): {e}"
        )
        return False

    transmission.state = STREAMING
    logger.debug(
        f"[VAD] Warmup complete ({len(transmission.warmup_buffer)} bytes), streaming"
    )
    return True


async def _send_chunk(
    wyoming: WyomingStreamingClient, config: dict, chunk: bytes
) -> bool:
    """Stream one chunk, reconnecting once if the connection dropped."""
    try:
        await wyoming.send_chunk(chunk)
        return True
    except Exception as e:
        error_type = wyoming.classify_error(e)
        logger.error(f"[Wyoming] Failed to send chunk ({error_type}): {e}")

    if error_type not in ("connection_reset", "connection_lost"):
        return False

    host, port = parse_wyoming_url(config["whisper_url"])
    logger.info("[Wyoming] Connection lost mid-stream, attempting reconnect...")
    if not await wyoming.reconnect_with_backoff(config, host, port):
        logger.error("[Wyoming] Reconnection failed, skipping transmission")
        return False

    try:
        await wyoming.start_session(wyoming.client)
    except Exception as e:
        logger.error(f"[Wyoming] Failed to restart session after reconnect: {e}")
        return False

    logger.info("[Wyoming] Reconnected and session restarted")
    return True


async def _finish(
    transmission: TransmissionState,
    wyoming: WyomingStreamingClient,
    config: dict,
    mqtt_client,
    finalizers: set,
) -> WyomingStreamingClient:
    """Detach a finished transmission for transcription and return to IDLE.

    Waiting for a transcript here would stall audio capture for up to
    wyoming_read_timeout seconds, and the stale chunks read afterwards would be
    stamped with the wrong wall-clock time, skewing the next transmission's
    timers. So the transcript wait runs as a background task, which takes
    ownership of the connection and closes it. Returns a fresh client for the
    next transmission.
    """
    transmission.end_time = time.time()
    duration = transmission.duration
    if duration >= transmission.max_duration:
        reason = f"max duration ({transmission.max_duration}s)"
    else:
        reason = f"silence timeout ({transmission.silence_timeout}s)"
    logger.info(f"[VAD] Transmission ended on {reason}, duration {duration:.1f}s")

    audio = bytes(transmission.warmup_buffer) + bytes(transmission.recording_buffer)
    session_open = wyoming.is_active
    transmission.reset()

    if duration < transmission.min_duration:
        logger.info(
            f"[VAD] Transmission too short ({duration:.2f}s < "
            f"{transmission.min_duration}s), discarding"
        )
        # Always close: discarding while the session is open leaves the server
        # holding orphaned audio mid-stream.
        await wyoming.disconnect()
        return WyomingStreamingClient()

    if not session_open:
        logger.warning("[VAD] Transmission had no Wyoming session, nothing to transcribe")
        await wyoming.disconnect()
        return WyomingStreamingClient()

    task = asyncio.create_task(
        _transcribe_and_publish(wyoming, config, mqtt_client, duration, audio)
    )
    finalizers.add(task)
    task.add_done_callback(finalizers.discard)
    return WyomingStreamingClient()


async def _transcribe_and_publish(
    wyoming: WyomingStreamingClient,
    config: dict,
    mqtt_client,
    duration: float,
    audio: bytes,
) -> None:
    """Collect the transcript for a finished transmission and publish it.

    Owns `wyoming` and closes it on every path.
    """
    try:
        read_timeout = config.get("wyoming_read_timeout", 30.0)
        try:
            transcript = await wyoming.stop_session(timeout=read_timeout)
        except Exception as e:
            logger.error(f"[Wyoming] Error ending transmission: {e}")
            return

        if not transcript:
            logger.warning("[VAD] No transcript received for transmission")
            return

        clean_text = transcript.strip()
        if is_hallucination(clean_text):
            logger.info(f"Filtered hallucination: '{clean_text}'")
            return

        logger.info(f"Transcription [{duration:.1f}s]: {clean_text}")
        await _publish_transcript(config, mqtt_client, clean_text, audio)
    finally:
        await wyoming.disconnect()


async def _publish_transcript(
    config: dict, mqtt_client, clean_text: str, audio: bytes
) -> None:
    """Save the recording if enabled and publish the transcript to MQTT."""
    audio_file_name: str | None = None

    if config.get("audio_recording", False) and audio:
        save_dir = audio_dir(config)
        timestamp_str = format_timestamp(config.get("timezone", "UTC"))
        audio_file_path = await asyncio.to_thread(
            save_audio_recording,
            audio, str(config["frequency"]), timestamp_str, save_dir,
        )
        if audio_file_path:
            audio_file_name = os.path.basename(audio_file_path)
            await asyncio.to_thread(
                cleanup_old_recordings,
                config.get("audio_retention_days", 7),
                config.get("audio_max_files", 0),
                save_dir,
            )

    message = {
        "text": clean_text,
        "frequency": str(config["frequency"]),
        "timestamp": format_timestamp(config.get("timezone", "UTC")),
    }
    if audio_file_name:
        message["audio_file"] = f"radio-audio/{audio_file_name}"
        message["audio_url"] = audio_url(config, audio_file_name)

    safe_publish(
        mqtt_client, config["mqtt_topic"], json.dumps(message),
        qos=1, label="transcription",
    )


async def check_wyoming_connection(config: dict) -> bool:
    """Probe the Wyoming server at startup.

    Returns:
        True if reachable, False after the configured attempts.
    """
    host, port = parse_wyoming_url(config["whisper_url"])
    client = WyomingStreamingClient()
    if await client.reconnect_with_backoff(config, host, port):
        await client.disconnect()
        return True

    logger.error(
        "[Wyoming] Server unreachable at startup. The add-on will continue but "
        "transcription will not work until the server is available."
    )
    return False
