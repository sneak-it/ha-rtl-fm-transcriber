#!/usr/bin/env python3
"""
RTL-FM Transcriber for Home Assistant
Captures FM audio via RTL-SDR, transcribes with Wyoming (faster-whisper), publishes to MQTT.
"""

import asyncio
import contextlib
import json
import logging
import os
import re
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse

import paho.mqtt.client as mqtt
from wyoming.asr import Transcribe, Transcript
from wyoming.audio import AudioChunk, AudioStart, AudioStop
from wyoming.client import AsyncTcpClient

# Constants for pipeline restart with exponential backoff
MAX_PIPELINE_RETRIES = 5
INITIAL_RETRY_DELAY = 1.0  # seconds
MAX_RETRY_DELAY = 30.0  # seconds

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


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
        "chunk_duration": 15,
        "whisper_url": "http://youriphere:10300",
        "mqtt_host": "core-mosquitto",
        "mqtt_port": 1883,
        "mqtt_topic": "radio/transcription",
        "mqtt_username": "",
        "mqtt_password": "",
        "vad_threshold": 0.01,
        "vad_baseline_window": 30,
        "gain": "auto",
        "debug_audio": False,
        "ppm": 0,
        "bandpass_filter": True,
        "bandpass_low": 300,
        "bandpass_high": 3000,
    }


def create_mqtt_client(config):
    """Create and connect MQTT client."""
    logger.info("[MQTT] Initializing MQTT client...")
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id="rtl-fm-transcriber",
    )

    if config.get("mqtt_username"):
        client.username_pw_set(config["mqtt_username"], config.get("mqtt_password", ""))
        logger.info(f"[MQTT] Username configured for broker at {config['mqtt_host']}")
    else:
        logger.info("[MQTT] No username configured (anonymous connection)")

    logger.info(
        f"[MQTT] Connecting to broker at {config['mqtt_host']}:{config['mqtt_port']} (timeout: 60s)..."
    )

    try:
        client.connect(config["mqtt_host"], config["mqtt_port"], 60)
        logger.info("[MQTT] Connect packet sent, starting network loop...")
        client.loop_start()
        logger.info("[MQTT] Network loop started (background thread)")

        # Verify connection state after starting the loop
        import time as _time
        _time.sleep(0.5)  # Give network thread time to establish connection
        if client.is_connected():
            logger.info(
                f"[MQTT] Successfully connected to MQTT broker at {config['mqtt_host']}:{config['mqtt_port']}"
            )
        else:
            logger.warning(
                f"[MQTT] loop_start() returned but client.is_connected() is False — "
                f"connection may not be established yet"
            )
        return client
    except Exception as e:
        logger.error(f"[MQTT] Failed to connect to MQTT broker at {config['mqtt_host']}:{config['mqtt_port']}: {e}")
        raise


def is_mqtt_connected(client):
    """Check if MQTT client is still connected and reconnect if needed.
    
    Args:
        client: MQTT client instance
        
    Returns:
        True if connected (or reconnected), False if reconnection failed
    """
    try:
        if client.is_connected():
            return True
        
        logger.warning("[MQTT] Client reports disconnected — attempting reconnection...")
        reconnect_delay = 1.0
        max_retries = 3
        for attempt in range(1, max_retries + 1):
            logger.info(
                f"[MQTT] Reconnection attempt {attempt}/{max_retries}..."
            )
            try:
                client.reconnect()
                if client.is_connected():
                    logger.info(f"[MQTT] Reconnected successfully on attempt {attempt}")
                    return True
                else:
                    logger.warning(f"[MQTT] Reconnect returned but not connected (attempt {attempt})")
            except Exception as reconnect_err:
                logger.error(
                    f"[MQTT] Reconnection attempt {attempt} failed: {reconnect_err}"
                )
            
            if attempt < max_retries:
                delay = reconnect_delay * attempt
                logger.info(f"[MQTT] Waiting {delay:.1f}s before next reconnection attempt...")
                import time as _time
                _time.sleep(delay)
        
        logger.error("[MQTT] All reconnection attempts failed — MQTT publishing will be skipped")
        return False
    except Exception as e:
        logger.error(f"[MQTT] Error checking connection state: {e}")
        return False


async def transcribe_wyoming(audio_path, whisper_url, connection_timeout=30):
    """Send audio file to Wyoming server for transcription.

    Args:
        audio_path: Path to WAV audio file
        whisper_url: Wyoming server URL (e.g. tcp://host:10300)
        connection_timeout: Timeout in seconds for connection and read operations
    """
    # Parse host/port from URL
    try:
        parsed = urlparse(whisper_url)
        host = parsed.hostname or "localhost"
        port = parsed.port or 10300
    except Exception as e:
        logger.error(f"Failed to parse Wyoming URL {whisper_url}: {e}")
        return None

    logger.info(f"Connecting to Wyoming server at {host}:{port}")

    try:
        async with asyncio.timeout(connection_timeout):
            async with AsyncTcpClient(host, port) as client:
                # Read audio file
                with open(audio_path, "rb") as f:
                    audio_data = f.read()

                # 1. Send AudioStart (for 16kHz, 16-bit mono, usually expected by Whisper)
                await client.write_event(
                    AudioStart(rate=16000, width=2, channels=1).event()
                )

                # 2. Send AudioChunk(s)
                chunk_size = 1024
                for i in range(0, len(audio_data), chunk_size):
                    chunk = audio_data[i : i + chunk_size]
                    await client.write_event(
                        AudioChunk(rate=16000, width=2, channels=1, audio=chunk).event()
                    )

                # 3. Send AudioStop
                await client.write_event(AudioStop().event())

                # 4. Send Transcribe event to trigger processing
                await client.write_event(Transcribe().event())

                # 5. Wait for Transcript
                logger.info("Waiting for transcript...")
                while True:
                    try:
                        event = await asyncio.wait_for(
                            client.read_event(), timeout=connection_timeout
                        )
                    except asyncio.TimeoutError:
                        logger.warning(
                            f"No transcript received within {connection_timeout}s timeout"
                        )
                        break

                    if event is None:
                        logger.warning("Connection closed by server")
                        break

                    if Transcript.is_type(event.type):
                        transcript = Transcript.from_event(event)
                        return transcript.text

    except asyncio.TimeoutError:
        logger.error(
            f"Timed out after {connection_timeout}s connecting to or communicating with Wyoming server at {host}:{port}"
        )
    except ConnectionRefusedError:
        logger.error(f"Connection refused to Wyoming server at {host}:{port}")
    except Exception as e:
        logger.error(f"Wyoming transcription error: {e}")

    return None


class _RmsBaselineTracker:
    """Tracks RMS baseline for RTL-SDR VAD with inverted RMS behavior.
    
    In RTL-SDR setups with AGC, idle noise often has HIGHER RMS than
    active transmissions (strong signal causes AGC to reduce gain).
    This tracker maintains a rolling baseline of idle RMS values and
    detects voice when RMS drops significantly below the baseline.
    """
    
    def __init__(self, window_seconds: int = 30) -> None:
        """Initialize the baseline tracker.
        
        Args:
            window_seconds: Seconds of idle samples to keep for baseline calculation.
        """
        self._samples: list[tuple[float, float]] = []
        self._window_seconds: int = window_seconds
        self._initial_baseline: Optional[float] = None
    
    def add_sample(self, rms, timestamp=None):
        """Add an RMS sample to the baseline.
        
        Args:
            rms: The RMS amplitude value.
            timestamp: Optional timestamp (defaults to current time).
        """
        ts = timestamp or time.time()
        self._samples.append((ts, rms))
        self._prune(ts)
        
        # Use first sample as initial baseline estimate
        if self._initial_baseline is None:
            self._initial_baseline = rms
    
    def _prune(self, current_time):
        """Remove samples older than the window."""
        cutoff = current_time - self._window_seconds
        while self._samples and self._samples[0][0] < cutoff:
            self._samples.pop(0)
    
    def get_baseline(self, current_time=None):
        """Get the current baseline RMS value.
        
        Returns the median of recent samples if >= 2 exist, otherwise
        falls back to the first sample (initial baseline).
        
        Args:
            current_time: Optional timestamp (defaults to current time).
            
        Returns:
            The baseline RMS value, or None if no samples yet.
        """
        if not self._samples and self._initial_baseline is None:
            return None
        
        current_time = current_time or time.time()
        self._prune(current_time)
        
        if len(self._samples) >= 2:
            rms_values = [s[1] for s in self._samples]
            rms_values.sort()
            mid = len(rms_values) // 2
            
            if len(rms_values) % 2 == 0:
                return (rms_values[mid - 1] + rms_values[mid]) / 2
            return rms_values[mid]
        
        # Fall back to initial baseline (first sample)
        return self._initial_baseline
    
    @property
    def sample_count(self):
        """Number of samples currently in the baseline."""
        return len(self._samples)


def check_audio_has_voice(wav_path: str, threshold: float = 0.02,
                          baseline_tracker: Optional[_RmsBaselineTracker] = None,
                          baseline_window: int = 30) -> bool:
    """
    Voice activity detection using audio amplitude with baseline tracking.
    
    In RTL-SDR setups with AGC, idle noise often has HIGHER RMS than
    active transmissions. When a strong signal arrives, AGC reduces gain,
    causing the RMS to drop. This function detects voice when RMS drops
    significantly below a rolling baseline of idle noise.

    Args:
        wav_path: Path to WAV file to analyze
        threshold: RMS drop below baseline to detect voice
        baseline_tracker: Optional _RmsBaselineTracker instance for tracking idle RMS
        baseline_window: Seconds of idle samples to keep for baseline

    Returns:
        True if audio likely contains voice, False otherwise.
        Returns False on failure (safe default to avoid transcribing silence).
    """
    try:
        # Use sox to get audio statistics
        result = subprocess.run(
            ["sox", wav_path, "-n", "stat"], capture_output=True, text=True, timeout=10
        )

        if result.returncode != 0:
            logger.error(
                f"sox VAD check failed with return code {result.returncode}: {result.stderr.strip()}"
            )
            return False

        # Parse RMS amplitude from sox stat output
        # sox stat outputs RMS amplitude in multiple formats across versions
        # We try multiple parsing strategies for robustness
        rms_found = False
        combined_output = result.stderr + "\n" + result.stdout

        for line in combined_output.split("\n"):
            line = line.strip()
            # Match lines like: "RMS amplitude: 0.1234567"
            if "RMS" in line and "amplitude" in line:
                # Try to find a float value in the line
                numbers = re.findall(r"[\d.]+", line)
                if numbers:
                    try:
                        rms = float(numbers[-1])
                        rms_found = True
                        return _check_voice(rms, threshold, baseline_tracker, baseline_window)
                    except ValueError:
                        logger.warning(
                            f"Could not parse RMS value from sox output: {line}"
                        )

        # If we can't parse, assume silence to be safe
        if not rms_found:
            logger.warning(
                "Could not parse VAD stats from sox output. "
                "Audio will be treated as silence to avoid transcribing noise. "
                "Consider adjusting vad_threshold or checking sox installation."
            )
        return False

    except subprocess.TimeoutExpired:
        logger.error("VAD check timed out after 10s")
        return False
    except FileNotFoundError:
        logger.error(
            "sox command not found - VAD check skipped. Install sox to enable voice activity detection."
        )
        return False
    except Exception as e:
        logger.warning(f"VAD check failed unexpectedly: {e}")
        return False


def _check_voice(rms: float, threshold: float,
                 baseline_tracker: Optional[_RmsBaselineTracker],
                 baseline_window: int) -> bool:
    """Check if RMS indicates voice using baseline tracking.
    
    In RTL-SDR setups with AGC, idle noise often has HIGHER RMS than
    active transmissions. When a strong signal arrives, AGC reduces gain,
    causing the RMS to drop.

    Args:
        rms: Measured RMS amplitude
        threshold: RMS drop margin below baseline for voice detection
        baseline_tracker: _RmsBaselineTracker instance for tracking idle RMS
        baseline_window: Seconds of idle samples to keep for baseline

    Returns:
        True if RMS drops significantly below baseline (voice detected).
    """
    if baseline_tracker is None:
        baseline_tracker = _RmsBaselineTracker(baseline_window)

    current_time = time.time()
    baseline = baseline_tracker.get_baseline(current_time)

    if baseline is None:
        # First sample - store as initial baseline
        baseline_tracker.add_sample(rms, current_time)
        logger.info(f"Audio RMS: {rms} (Initial baseline set)")
        return rms < 0.005  # Near-silence threshold

    logger.info(f"Audio RMS: {rms} (Baseline: {baseline:.4f}, Threshold drop: {threshold})")

    # Voice detected when RMS drops below baseline by the threshold amount
    voice_detected = rms < (baseline - threshold)

    # Only add to baseline if this sample is consistent with idle (noise)
    # Don't add transmission samples to the baseline
    if not voice_detected:
        baseline_tracker.add_sample(rms, current_time)

    return voice_detected


def is_hallucination(text):
    """Check for common Whisper hallucinations on silence."""
    hallucinations = [
        "thank you for watching",
        "thanks for watching",
        "subs by",
        "subscribe",
        "amara.org",
        "copyright",
    ]
    text_lower = text.lower().strip()

    # Empty or very short
    if len(text_lower) < 2:
        return True

    return any(h in text_lower for h in hallucinations)


async def _start_pipeline(config, frequency_hz, sample_rate, capture_rate):
    """Start the rtl_fm -> sox audio capture pipeline using asyncio subprocess.

    Returns:
        Tuple of (rtl_proc, sox_proc) asyncio subprocess objects, or None on failure.
    """
    squelch_val = str(config.get("squelch", 50))
    ppm_val = config.get("ppm", 0)

    # Build rtl_fm command for narrowband FM (public safety radio)
    # -f: frequency in Hz
    # -M fm: narrowband FM demodulation
    # -s: sample rate (12000 for 12.5 kHz channels)
    # -l: squelch threshold (0-200)
    # -E dc: remove DC offset
    # -E pad: output silence when squelched (prevents pipe starvation)
    # -p: PPM correction for dongle clock drift
    rtl_cmd = [
        "rtl_fm",
        "-f",
        str(frequency_hz),
        "-M",
        "fm",
        "-s",
        str(capture_rate),
        "-l",
        squelch_val,
        "-E",
        "dc",  # Remove DC offset
        "-E",
        "pad",  # Output silence when squelched (prevents pipe starvation)
        "-p",
        str(ppm_val),  # PPM correction
    ]

    if config.get("gain") and config["gain"] != "auto":
        rtl_cmd.extend(["-g", str(config["gain"])])

    rtl_cmd.append("-")

    # Build sox command to resample and optionally apply bandpass filter
    # Input: capture_rate (12000), signed 16-bit, mono, raw
    # Output: sample_rate (16000), signed 16-bit, mono, raw
    sox_cmd = [
        "sox",
        "-t",
        "raw",
        "-r",
        str(capture_rate),
        "-e",
        "signed",
        "-b",
        "16",
        "-c",
        "1",
        "-",
        "-r",
        str(sample_rate),
        "-e",
        "signed",
        "-b",
        "16",
        "-t",
        "raw",
        "-",
    ]

    # Add bandpass filter for voice clarity (300-3000 Hz default)
    # This removes low-frequency hum and high-frequency static
    if config.get("bandpass_filter", True):
        bandpass_low = config.get("bandpass_low", 300)
        bandpass_high = config.get("bandpass_high", 3000)
        sox_cmd.extend(
            [
                "highpass",
                str(bandpass_low),  # Remove low-frequency noise
                "lowpass",
                str(bandpass_high),  # Remove high-frequency static
                "gain",
                "-3",  # Compensate for volume loss from filtering
            ]
        )

    try:
        # Start rtl_fm first
        rtl_proc = await asyncio.create_subprocess_exec(
            *rtl_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        # Start sox with asyncio.PIPE stdin (not rtl_proc.stdout directly,
        # because StreamReader has no fileno() for subprocess.Popen)
        sox_proc = await asyncio.create_subprocess_exec(
            *sox_cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        # Background task: pipe rtl_fm stdout -> sox stdin asynchronously
        async def _pipe_streams(reader, writer):
            """Copy all data from reader to writer until EOF."""
            try:
                while True:
                    chunk = await reader.read(65536)  # 64 KiB chunks
                    if not chunk:
                        break
                    writer.write(chunk)
                writer.close()
                await writer.wait_closed()
            except (asyncio.CancelledError, Exception):
                writer.close()
                with contextlib.suppress(asyncio.InvalidStateError, RuntimeError):
                    await writer.wait_closed()
                raise

        _pipe_task = asyncio.create_task(
            _pipe_streams(rtl_proc.stdout, sox_proc.stdin)
        )
        # Store the task on the pipeline so it can be awaited during cleanup
        sox_proc._pipe_task = _pipe_task  # noqa: SLF001

        return rtl_proc, sox_proc
    except (OSError, FileNotFoundError) as e:
        logger.error(f"Failed to start audio pipeline: {e}")
        return None


async def _read_stderr_pipeline(rtl_proc, sox_proc):
    """Read stderr from both processes in the pipeline concurrently."""
    tasks = [
        asyncio.create_task(_read_stderr_lines(rtl_proc, "RTL_FM")),
        asyncio.create_task(_read_stderr_lines(sox_proc, "SOX")),
    ]
    return asyncio.gather(*tasks, return_exceptions=True)


async def _read_stderr_lines(proc, label):
    """Continuously read stderr lines from a subprocess and log them."""
    try:
        while True:
            line = await proc.stderr.readline()
            if not line:
                break
            decoded = line.decode("utf-8", errors="replace").strip()
            if decoded:
                logger.info(f"{label} Log: {decoded}")
    except (asyncio.CancelledError, Exception):
        pass  # Cleaned up by caller


async def _cleanup_pipeline(rtl_proc, sox_proc):
    """Safely terminate pipeline processes with specific exception handling.

    Waits for the async pipe task (rtl_fm -> sox) to finish before
    terminating processes, ensuring sox receives EOF on stdin.
    """
    # First, wait for the async pipe task to finish so sox gets EOF on stdin.
    pipe_task = getattr(sox_proc, "_pipe_task", None) if sox_proc else None
    if pipe_task is not None and not pipe_task.done():
        try:
            await asyncio.wait_for(pipe_task, timeout=10.0)
        except asyncio.TimeoutError:
            logger.warning("Pipe task did not finish within timeout")
        except Exception:
            logger.debug("Pipe task finished with error (expected on stop)")

    # Stop sox first (it depends on rtl_fm)
    for name, proc in [("SOX", sox_proc), ("RTL_FM", rtl_proc)]:
        if proc is not None:
            try:
                if proc.returncode is None:  # Still running
                    proc.terminate()
                    try:
                        await asyncio.wait_for(proc.wait(), timeout=5.0)
                    except asyncio.TimeoutError:
                        logger.warning(
                            f"{name} did not terminate gracefully, forcing kill"
                        )
                        proc.kill()
                        await proc.wait()
            except ProcessLookupError:
                logger.debug(f"{name} process already exited")
            except (OSError, Exception) as e:
                logger.debug(f"Error cleaning up {name}: {e}")


async def capture_loop(config, mqtt_client):
    """Async capture loop with persistent pipeline and automatic restart.

    Implements exponential backoff restart on pipeline failure.
    """
    frequency_hz = int(config["frequency"] * 1_000_000)
    sample_rate = 16000
    max_duration = config["chunk_duration"]
    silence_timeout = 2.0
    # 12000 Hz sample rate for narrowband FM (12.5 kHz public safety channels)
    capture_rate = 12000
    bytes_per_sec = sample_rate * 2  # 16-bit mono

    logger.info(f"Starting persistent capture on {config['frequency']} MHz")

    retry_count = 0
    stderr_reader_task = None
    
    # Create shared VAD baseline tracker
    vad_baseline_window = config.get("vad_baseline_window", 30)
    baseline_tracker = _RmsBaselineTracker(vad_baseline_window)
    logger.info(f"VAD baseline tracking enabled (window: {vad_baseline_window}s)")

    while True:
        # Build pipeline
        processes = await _start_pipeline(
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

        buffer = bytearray()
        last_data_time = time.time()
        pipeline_running = True
        stderr_reader_task = None

        try:
            # Start concurrent stderr reader
            stderr_reader_task = asyncio.create_task(
                _read_stderr_pipeline(rtl_proc, sox_proc)
            )

            # Main data reading loop
            while pipeline_running:
                # Use asyncio.wait_for with a timeout to periodically check pipeline health
                try:
                    chunk = await asyncio.wait_for(
                        sox_proc.stdout.read(4096), timeout=0.1
                    )
                except asyncio.TimeoutError:
                    # No data available - check for silence timeout
                    if len(buffer) > 0:
                        time_since_last = time.time() - last_data_time
                        if time_since_last > silence_timeout:
                            logger.info(
                                f"Silence detected ({silence_timeout}s), processing transmission"
                            )
                            await process_buffer(buffer, config, mqtt_client,
                                                 baseline_tracker=baseline_tracker)
                            buffer = bytearray()
                    await asyncio.sleep(0.01)
                    continue
                except asyncio.CancelledError:
                    break

                if not chunk:
                    # EOF from sox - pipeline broke
                    logger.error("Audio pipeline died (EOF from sox)")
                    pipeline_running = False
                    break

                buffer.extend(chunk)
                last_data_time = time.time()

                # If buffer gets too big (max duration), force process it
                if len(buffer) > max_duration * bytes_per_sec:
                    logger.info("Max duration reached, forcing transcription")
                    await process_buffer(buffer, config, mqtt_client,
                                         baseline_tracker=baseline_tracker)
                    buffer = bytearray()

                # Check if processes still alive (asyncio.Process uses .returncode, not .poll())
                if sox_proc.returncode is not None:
                    logger.error(
                        f"Sox process exited unexpectedly (return code: {sox_proc.returncode})"
                    )
                    pipeline_running = False
                    break

                if rtl_proc.returncode is not None:
                    logger.error(
                        f"RTL_FM process exited unexpectedly (return code: {rtl_proc.returncode})"
                    )
                    pipeline_running = False
                    break

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

            # Cleanup processes
            await _cleanup_pipeline(rtl_proc, sox_proc)

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


async def process_buffer(audio_data, config, mqtt_client, baseline_tracker=None):
    """Save buffer to WAV and transcribe.
    
    Args:
        audio_data: Raw audio bytes to process
        config: Configuration dictionary
        mqtt_client: MQTT client instance
        baseline_tracker: Optional _RmsBaselineTracker for VAD baseline tracking
    """
    if len(audio_data) < 16000:  # Ignore tiny blips (<0.5s)
        return

    wav_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as wav_file:
            wav_path = wav_file.name

        # Write raw buf to WAV using sox (simplest way to add header)
        # Or proper wavfile write. Let's use sox again to wrap it.
        # Actually, python wave lib is easier/faster.
        import wave

        with wave.open(wav_path, "wb") as wf:
            wf.setnchannels(1)
            wf.setsampwidth(2)
            wf.setframerate(16000)
            wf.writeframes(audio_data)

        # Debug Audio: Save raw capture BEFORE checks
        if config.get("debug_audio"):
            try:
                debug_dir = "/config/www"
                if os.path.exists(debug_dir) and os.path.isdir(debug_dir):
                    import shutil

                    # Save raw capture (what the loop heard)
                    dest_path = f"{debug_dir}/rtl_last_capture.wav"
                    shutil.copy2(wav_path, dest_path)
                    os.chmod(dest_path, 0o600)  # Restrict permissions
                    logger.info(f"Debug: Saved raw capture to {dest_path}")
            except PermissionError as e:
                logger.error(
                    f"Failed to save debug audio (permission denied): {debug_dir}/rtl_last_capture.wav - {e}"
                )
            except OSError as e:
                logger.error(f"Failed to save debug audio (OS error): {e}")
            except Exception as e:
                logger.debug(f"Failed to save debug audio (unexpected): {e}")

        # VAD Check
        vad_threshold = config.get("vad_threshold", 0.05)
        vad_baseline_window = config.get("vad_baseline_window", 30)
        vad_passed = check_audio_has_voice(wav_path, vad_threshold,
                                      baseline_tracker, vad_baseline_window)
        if not vad_passed:
            logger.warning(
                f"[VAD] Audio rejected — buffer of {len(audio_data)/2/16000:.1f}s discarded "
                f"(RMS below threshold, no voice detected)"
            )
            os.unlink(wav_path)
            return
        logger.info(
            f"[VAD] Audio passed voice activity check — "
            f"buffer size: {len(audio_data)/2/16000:.1f}s, proceeding to transcription"
        )

        # Debug Audio: Save passed audio (what is sending to Whisper)
        if config.get("debug_audio"):
            try:
                dest_path = "/config/www/rtl_last_transcription.wav"
                shutil.copy2(wav_path, dest_path)
                os.chmod(dest_path, 0o600)  # Restrict permissions
                logger.info(f"Debug: Saved transcription audio to {dest_path}")
            except PermissionError as e:
                logger.error(
                    f"Failed to save debug transcription audio (permission denied): {e}"
                )
            except OSError as e:
                logger.error(
                    f"Failed to save debug transcription audio (OS error): {e}"
                )
            except Exception as e:
                logger.debug(
                    f"Failed to save debug transcription audio (unexpected): {e}"
                )

        # Transcribe
        text = await transcribe_wyoming(wav_path, config["whisper_url"])

        if text:
            clean_text = text.strip()
            if is_hallucination(clean_text):
                logger.info(f"Filtered hallucination: '{clean_text}'")
            else:
                logger.info(f"Transcription: {clean_text}")
                message = {
                    "text": clean_text,
                    "frequency": str(config["frequency"]),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
                mqtt_topic = config["mqtt_topic"]
                mqtt_payload = json.dumps(message)
                logger.info(
                    f"[MQTT] Publishing transcription to topic='{mqtt_topic}', "
                    f"payload='{mqtt_payload}', qos=1"
                )
                
                # Check MQTT connection before publishing
                if not is_mqtt_connected(mqtt_client):
                    logger.error(
                        "[MQTT] Failed to publish — MQTT client is disconnected "
                        "and reconnection attempts failed"
                    )
                else:
                    try:
                        pub_result = mqtt_client.publish(
                            mqtt_topic, mqtt_payload, qos=1
                        )
                        logger.info(
                            f"[MQTT] Publish successful — topic='{mqtt_topic}', "
                            f"pubmid={pub_result.mid}, result_code={pub_result.rc}"
                        )
                    except Exception as publish_err:
                        logger.error(
                            f"[MQTT] Publish failed with exception: type={type(publish_err).__name__}, "
                            f"error={publish_err}"
                        )

        os.unlink(wav_path)

    except Exception as e:
        logger.error(f"Processing error: {e}")
        if wav_path and os.path.exists(wav_path):
            with contextlib.suppress(ProcessLookupError):
                os.unlink(wav_path)


def publish_discovery(config, mqtt_client):
    """Publish Home Assistant MQTT Auto Discovery payload."""
    # Unique ID based on frequency to allow multiple instances
    unique_id = f"rtl_fm_{str(config['frequency']).replace('.', '_')}"
    device_name = f"RTL-FM Scanner {config['frequency']}MHz"

    discovery_topic = f"homeassistant/sensor/{unique_id}/transcription/config"
    discovery_payload = json.dumps({
        "name": "Radio Transcription",
        "unique_id": f"{unique_id}_transcription",
        "state_topic": config["mqtt_topic"],
        "value_template": "{{ value_json.text[:255] }}",
        "json_attributes_topic": config["mqtt_topic"],
        "icon": "mdi:radio-handheld",
        "device": {
            "identifiers": [unique_id],
            "name": device_name,
            "model": "RTL-SDR",
            "manufacturer": "RTL-FM Transcriber",
        },
    })

    logger.info(
        f"[MQTT] Publishing Home Assistant Discovery to topic='{discovery_topic}', "
        f"retain=True, payload='{discovery_payload[:100]}...'"
    )

    try:
        if not is_mqtt_connected(mqtt_client):
            logger.error(
                "[MQTT] Failed to publish Discovery — MQTT client is disconnected"
            )
            return
        
        pub_result = mqtt_client.publish(discovery_topic, discovery_payload, retain=True)
        logger.info(
            f"[MQTT] Discovery published successfully — topic='{discovery_topic}', "
            f"pubmid={pub_result.mid}, result_code={pub_result.rc}"
        )
    except Exception as e:
        logger.error(
            f"[MQTT] Discovery publish failed with exception: type={type(e).__name__}, "
            f"error={e}"
        )


def main():
    logger.info("RTL-FM Transcriber starting (Wyoming Protocol)...")
    config = load_config()

    # Check RTL-SDR
    try:
        subprocess.run(["rtl_test", "-t"], capture_output=True, timeout=10)
        logger.info("RTL-SDR device check passed")
    except Exception as e:
        logger.error(f"RTL-SDR check failed: {e}")
        # Continue anyway, let rtl_fm fail if must

    mqtt_client = create_mqtt_client(config)

    # Publish HA Discovery
    publish_discovery(config, mqtt_client)

    try:
        asyncio.run(capture_loop(config, mqtt_client))
    except KeyboardInterrupt:
        pass
    finally:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()


if __name__ == "__main__":
    main()
