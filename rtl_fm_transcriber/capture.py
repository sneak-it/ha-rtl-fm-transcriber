"""Capture loop: VAD segmentation, Wyoming streaming, transcript publication."""

import asyncio
import contextlib
import json
import logging
import os
import time

from wyoming.client import AsyncTcpClient

from .audio import compute_rms
from .config import parse_wyoming_url
from .filters import is_hallucination
from .mqtt import safe_publish
from .pipeline import cleanup_pipeline, read_stderr_pipeline, start_pipeline
from .recording import cleanup_old_recordings, save_audio_recording
from .timeutil import format_timestamp
from .transmission import TransmissionState
from .vad import RmsBaselineTracker
from .wyoming_client import WyomingStreamingClient

logger = logging.getLogger(__name__)

# Pipeline restart with exponential backoff
MAX_PIPELINE_RETRIES = 5
INITIAL_RETRY_DELAY = 1.0  # seconds
MAX_RETRY_DELAY = 30.0  # seconds


async def capture_loop(config, mqtt_client):
    """Async capture loop with persistent pipeline and streaming transcription.

    Implements a state machine for radio transmission segmentation:
    IDLE -> WARMUP -> STREAMING -> WAITING_FOR_END -> IDLE
    
    Audio is streamed directly to Wyoming as it arrives, with VAD driving
    real-time segmentation based on RMS amplitude drops below baseline.

    Implements exponential backoff restart on pipeline failure.
    """
    frequency_hz = int(config["frequency"] * 1_000_000)
    sample_rate = 16000
    # 12000 Hz sample rate for narrowband FM (12.5 kHz public safety channels)
    capture_rate = 12000

    logger.info("Starting persistent capture with streaming transcription")
    logger.info(f"  Frequency: {config['frequency']} MHz")
    logger.info(f"  Silence timeout: {config.get('silence_timeout', 2.0)}s")
    logger.info(f"  VAD recovery: {config.get('vad_recovery_seconds', 1.0)}s")
    logger.info(f"  Min transmission: {config.get('min_transmission_duration', 0.3)}s")
    logger.info(f"  Max transmission: {config.get('max_transmission_duration', 120.0)}s")

    retry_count = 0
    stderr_reader_task = None
    
    # Create shared VAD baseline tracker
    vad_baseline_window = config.get("vad_baseline_window", 30)
    baseline_tracker = RmsBaselineTracker(vad_baseline_window)
    logger.info(f"VAD baseline tracking enabled (window: {vad_baseline_window}s)")

    while True:
        # Build pipeline
        processes = await start_pipeline(
            config, frequency_hz, sample_rate, capture_rate
        )
        if processes is None:
            logger.error("Failed to start audio pipeline, retrying...")
            retry_count += 1
            delay = min(INITIAL_RETRY_DELAY * (2 ** (retry_count - 1)), MAX_RETRY_DELAY)
            await asyncio.sleep(delay)
            continue

        rtl_proc, sox_proc = processes
        retry_count = 0  # Reset on successful start

        logger.info("Pipeline started. Waiting for audio...")

        # Transmission state machine
        transmission = TransmissionState(config, baseline_tracker)
        pipeline_running = True
        stderr_reader_task = None
        # Track bytes per second for warmup buffer sizing
        bytes_per_sec = sample_rate * 2  # 16-bit mono
        warmup_bytes = int(config.get("vad_warmup_ms", 150) / 1000.0 * bytes_per_sec)

        # Wyoming connection pool - one connection per pipeline lifetime
        wyoming_client: AsyncTcpClient | None = None

        try:
            # Start concurrent stderr reader
            stderr_reader_task = asyncio.create_task(
                read_stderr_pipeline(rtl_proc, sox_proc)
            )

            # Main data reading loop
            while pipeline_running:
                # Use asyncio.wait_for with a timeout to periodically check pipeline health
                try:
                    chunk = await asyncio.wait_for(
                        sox_proc.stdout.read(4096), timeout=0.05
                    )
                except TimeoutError:
                    # No data available - check pipeline health
                    if (transmission.state in ("STREAMING", "WAITING_FOR_END")
                            and transmission.silence_start > 0):
                            elapsed = time.time() - transmission.silence_start
                            if elapsed >= transmission.silence_recovery:
                                logger.info(
                                    f"[VAD] Silence timeout ({transmission.silence_recovery}s) reached, "
                                    f"ending transmission (duration: {transmission.duration:.1f}s)"
                                )
                                await _end_transmission(transmission, config, mqtt_client)
                                wyoming_client = None
                                transmission.reset()
                    await asyncio.sleep(0.01)
                    continue
                except asyncio.CancelledError:
                    break

                if not chunk:
                    # EOF from sox - pipeline broke
                    logger.error("Audio pipeline died (EOF from sox)")
                    pipeline_running = False
                    break

                current_time = time.time()
                rms = compute_rms(chunk)

                # Get current baseline for VAD decision
                baseline = baseline_tracker.get_baseline(current_time)
                vad_threshold = config.get("vad_threshold", 0.03)

                if baseline is not None and rms < (baseline - vad_threshold):
                    # Voice activity detected (RMS drops below baseline)
                    if transmission.state == "IDLE":
                        logger.info(
                            f"[VAD] Transmission started — RMS: {rms:.4f} "
                            f"(baseline: {baseline:.4f}, threshold: {vad_threshold})"
                        )
                        transmission.state = "WARMUP"
                        transmission.start_time = current_time
                        transmission.warmup_buffer = bytearray(chunk)
                        baseline_tracker.add_sample(baseline, current_time)
                    elif transmission.state in ("STREAMING", "WAITING_FOR_END"):
                        # Voice resumed during recovery period
                        transmission.state = "STREAMING"
                        transmission.silence_start = 0.0
                        logger.debug("[VAD] Voice resumed during recovery period")
                else:
                    # No voice activity (idle noise or silence)
                    if transmission.state == "IDLE":
                        # Track baseline
                        baseline_tracker.add_sample(rms, current_time)
                    elif transmission.state == "WARMUP":
                        # Still buffering during warmup
                        transmission.warmup_buffer.extend(chunk)
                        
                        # Check warmup timeout (safety: don't buffer forever)
                        if current_time - transmission.start_time > 2.0:
                            logger.warning("[VAD] Warmup timeout, starting streaming")
                            transmission.state = "STREAMING"
                        
                        # Check if warmup buffer is large enough
                        if len(transmission.warmup_buffer) >= warmup_bytes:
                            logger.debug("[VAD] Warmup period complete, starting streaming")
                            transmission.state = "STREAMING"
                            
                            # Connect to Wyoming and start session
                            wyoming_host, wyoming_port = parse_wyoming_url(config["whisper_url"])
                            wyoming_conn_timeout = config.get("wyoming_connection_timeout", 10.0)
                            
                            if not transmission.wyoming.is_connected():
                                logger.info(
                                    f"[Wyoming] Connection not available, attempting to connect "
                                    f"to {wyoming_host}:{wyoming_port}..."
                                )
                                connected = await transmission.wyoming.connect(
                                    wyoming_host, wyoming_port, timeout=wyoming_conn_timeout
                                )
                                if not connected:
                                    # Try reconnecting with backoff
                                    reconnected = await transmission.wyoming._reconnect_with_backoff(
                                        config, wyoming_host, wyoming_port
                                    )
                                    if not reconnected:
                                        logger.warning(
                                            "[Wyoming] Unable to connect to server. "
                                            "Transmission will be skipped."
                                        )
                                        transmission.state = "IDLE"
                                        wyoming_client = None
                                        continue
                            
                            wyoming_client = transmission.wyoming.client
                            try:
                                await transmission.wyoming.start_session(wyoming_client)
                                # Send warmup buffer
                                await transmission.wyoming.send_chunk(
                                    bytes(transmission.warmup_buffer)
                                )
                            except Exception as e:
                                error_type = transmission.wyoming._classify_error(e)
                                logger.error(
                                    f"[Wyoming] Failed to start session ({error_type}): {e}"
                                )
                                transmission.state = "IDLE"
                                wyoming_client = None
                    elif transmission.state in ("STREAMING", "WAITING_FOR_END"):
                        # Voice stopped - start silence timer
                        if transmission.silence_start == 0.0:
                            transmission.silence_start = current_time
                            logger.debug(
                                f"[VAD] Silence detected, recovery timeout: "
                                f"{transmission.silence_recovery}s"
                            )
                        
                        # Check if we've exceeded max transmission duration
                        if transmission.duration >= transmission.max_duration:
                            logger.warning(
                                f"[VAD] Max transmission duration ({transmission.max_duration}s) "
                                f"reached, ending transmission"
                            )
                            await _end_transmission(transmission, config, mqtt_client)
                            wyoming_client = None
                            transmission.reset()

                # In WAITING_FOR_END state, check if silence timeout elapsed
                if (transmission.state == "WAITING_FOR_END"
                        and current_time - transmission.silence_start >= transmission.silence_recovery):
                        logger.info(
                            f"[VAD] Silence timeout ({transmission.silence_recovery}s) reached, "
                            f"ending transmission (duration: {transmission.duration:.1f}s)"
                        )
                        await _end_transmission(transmission, config, mqtt_client)
                        wyoming_client = None
                        transmission.reset()

                # Stream audio to Wyoming and record if actively speaking
                if transmission.state == "STREAMING" and wyoming_client is not None:
                    # Record audio for playback (if enabled)
                    if config.get("audio_recording", False):
                        transmission.recording_buffer.extend(chunk)
                    
                    try:
                        await transmission.wyoming.send_chunk(chunk)
                    except Exception as e:
                        error_type = transmission.wyoming._classify_error(e)
                        logger.error(
                            f"[Wyoming] Failed to send chunk ({error_type}): {e}"
                        )
                        
                        # Try to reconnect for transient errors
                        if error_type in ("connection_reset", "connection_lost"):
                            wyoming_host, wyoming_port = parse_wyoming_url(config["whisper_url"])
                            logger.info(
                                "[Wyoming] Connection lost mid-stream, attempting reconnect..."
                            )
                            reconnected = await transmission.wyoming._reconnect_with_backoff(
                                config, wyoming_host, wyoming_port
                            )
                            if reconnected:
                                # Restart session on new connection
                                wyoming_client = transmission.wyoming.client
                                try:
                                    await transmission.wyoming.start_session(wyoming_client)
                                    logger.info(
                                        "[Wyoming] Reconnected and session restarted"
                                    )
                                except Exception as reconnect_err:
                                    logger.error(
                                        f"[Wyoming] Failed to restart session after reconnect: {reconnect_err}"
                                    )
                                    transmission.state = "IDLE"
                                    wyoming_client = None
                            else:
                                logger.error(
                                    "[Wyoming] Reconnection failed, skipping transmission"
                                )
                                transmission.state = "IDLE"
                                wyoming_client = None
                        else:
                            transmission.state = "IDLE"
                            wyoming_client = None

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

            # Cleanup any in-progress Wyoming connection
            if transmission.wyoming.is_connected():
                try:
                    await transmission.wyoming.disconnect()
                    logger.info("[Wyoming] Connection closed gracefully")
                except Exception as e:
                    logger.warning(f"[Wyoming] Error closing connection: {e}")
            transmission.wyoming._connection_state = transmission.wyoming.STATE_DISCONNECTED

            # Cleanup processes
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


async def _end_transmission(transmission: TransmissionState, config: dict,
                           mqtt_client) -> None:
    """End a transmission: send AudioStop, get transcript, publish to MQTT.
    
    Args:
        transmission: The TransmissionState to finalize.
        config: Configuration dictionary.
        mqtt_client: MQTT client instance.
    """
    if transmission.state not in ("WARMUP", "STREAMING", "WAITING_FOR_END"):
        return
    
    # Check minimum duration
    if transmission.duration < transmission.min_duration:
        logger.info(
            f"[VAD] Transmission too short ({transmission.duration:.2f}s < "
            f"{transmission.min_duration}s), discarding"
        )
        transmission.state = "IDLE"
        return
    
    # Send AudioStop and get transcript
    try:
        transcript = await transmission.wyoming.stop_session(timeout=30.0)
    except Exception as e:
        logger.error(f"[Wyoming] Error ending transmission: {e}")
        transmission.state = "IDLE"
        return
    
    if not transcript:
        logger.warning("[VAD] No transcript received for transmission")
        transmission.state = "IDLE"
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
    
    transmission.state = "IDLE"


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
