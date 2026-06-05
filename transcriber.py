#!/usr/bin/env python3
"""
RTL-FM Transcriber for Home Assistant
Captures FM audio via RTL-SDR, transcribes with Wyoming (faster-whisper), publishes to MQTT.
"""

import asyncio
import contextlib
import json
import logging
import math
import os
import re
import struct
import subprocess
import time
from datetime import datetime, timezone, timedelta
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
        "vad_threshold": 0.03,
        "vad_baseline_window": 30,
        "gain": "auto",
        "debug_audio": False,
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
        "vad_recovery_seconds": 1.0,
        # Audio recording options
        "audio_recording": False,
        "audio_retention_days": 7,
        "audio_max_files": 0,
    }


def get_timezone_offset(tz_name: str) -> timedelta:
    """Get the UTC offset for a given timezone name.
    
    This is a simplified lookup for common timezone names.
    For full IANA timezone support, install the `tzdata` package.
    
    Args:
        tz_name: IANA timezone name (e.g., 'America/New_York') or 'UTC'.
        
    Returns:
        timedelta offset from UTC.
    """
    if tz_name == "UTC":
        return timedelta(0)
    
    try:
        from zoneinfo import ZoneInfo
        from datetime import datetime
        
        # Use a representative date (current date may have different DST)
        # We'll use a dynamic approach below for accuracy
        tz = ZoneInfo(tz_name)
        # Get offset for current moment by creating a naive UTC now and converting
        now_utc = datetime.now(timezone.utc)
        now_local = now_utc.astimezone(tz)
        return now_local.utcoffset() or timedelta(0)
    except ImportError:
        logger.warning("zoneinfo not available, falling back to fixed offsets")
    except Exception as e:
        logger.warning(f"Could not load timezone '{tz_name}': {e}, using UTC")
    
    return timedelta(0)


def format_timestamp(tz_name: str = "UTC") -> str:
    """Generate an ISO-format timestamp in the specified timezone.
    
    Args:
        tz_name: IANA timezone name (e.g., 'America/New_York') or 'UTC'.
        
    Returns:
        ISO-format timestamp string with timezone offset.
    """
    now_utc = datetime.now(timezone.utc)
    offset = get_timezone_offset(tz_name)
    tz_info = timezone(offset, name=tz_name)
    local_time = now_utc.astimezone(tz_info)
    return local_time.isoformat()


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
                "[MQTT] loop_start() returned but client.is_connected() is False — "
                "connection may not be established yet"
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


def compute_rms(raw_bytes: bytes) -> float:
    """Calculate RMS amplitude from raw PCM16 bytes.
    
    Returns normalized RMS in range [0, 1].
    
    Args:
        raw_bytes: Raw PCM16 audio samples (little-endian signed 16-bit).
        
    Returns:
        Normalized RMS amplitude value.
    """
    if len(raw_bytes) == 0:
        return 0.0
    
    num_samples = len(raw_bytes) // 2
    if num_samples == 0:
        return 0.0
    
    # Unpack as little-endian signed 16-bit integers
    samples = struct.unpack(f'<{num_samples}h', raw_bytes[:num_samples * 2])
    
    # Calculate RMS
    sum_squares = sum(s * s for s in samples)
    return math.sqrt(sum_squares / num_samples) / 32768.0


def compute_rms_batch(raw_bytes: bytes, window_size: int = 1600) -> list[tuple[float, float]]:
    """Calculate RMS amplitude for overlapping windows of audio data.
    
    Args:
        raw_bytes: Raw PCM16 audio data.
        window_size: Number of samples per window (default 1600 = 100ms at 16kHz).
        
    Returns:
        List of (start_offset, rms) tuples for each window.
    """
    if len(raw_bytes) == 0:
        return []
    
    samples_per_byte = 2  # 16-bit = 2 bytes per sample
    windows = []
    
    for i in range(0, len(raw_bytes) - window_size * samples_per_byte + 1, window_size * samples_per_byte):
        chunk = raw_bytes[i:i + window_size * samples_per_byte]
        rms = compute_rms(chunk)
        windows.append((i / samples_per_byte, rms))
    
    return windows


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


class _WyomingStreamingClient:
    """Manages streaming ASR connection to Wyoming server.
    
    Handles the Wyoming protocol streaming flow:
    AudioStart → streaming AudioChunk → AudioStop → transcript
    
    Includes connection lifecycle management with timeout, reconnection
    with exponential backoff, and error classification for graceful handling
    of server disconnects, unavailability, and network issues.
    """
    
    # Connection states
    STATE_DISCONNECTED = "DISCONNECTED"
    STATE_CONNECTING = "CONNECTING"
    STATE_CONNECTED = "CONNECTED"
    STATE_RECONNECTING = "RECONNECTING"
    
    def __init__(self) -> None:
        """Initialize the streaming client."""
        self.client: Optional[AsyncTcpClient] = None
        self.transcript_parts: list[str] = []
        self.final_transcript: Optional[str] = None
        self._session_active: bool = False
        self._connection_state: str = self.STATE_DISCONNECTED
        self._last_error: Optional[str] = None
        self._last_error_time: float = 0.0
    
    def _classify_error(self, e: Exception) -> str:
        """Classify a Wyoming connection error for logging.
        
        Returns a string describing the error type:
        - 'connection_refused': Server is down or port not listening
        - 'connection_timeout': Connection attempt timed out
        - 'connection_reset': Connection was reset by peer (server crashed)
        - 'connection_lost': Connection dropped unexpectedly
        - 'timeout_waiting_for_response': Server didn't respond in time
        - 'unknown': Unclassified error
        """
        if isinstance(e, ConnectionRefusedError):
            return "connection_refused"
        elif isinstance(e, asyncio.TimeoutError):
            return "timeout"
        elif isinstance(e, ConnectionResetError):
            return "connection_reset"
        elif isinstance(e, BrokenPipeError):
            return "connection_lost"
        elif isinstance(e, OSError):
            if e.errno == 110:  # ETIMEDOUT
                return "connection_timeout"
            elif e.errno == 104:  # ECONNRESET
                return "connection_reset"
            return f"os_error_{e.errno or 'unknown'}"
        elif "timeout" in str(e).lower():
            return "timeout"
        elif "connection" in str(e).lower():
            return "connection_error"
        else:
            return f"unknown_{type(e).__name__}"
    
    async def connect(self, host: str, port: int, timeout: float = 10.0) -> bool:
        """Connect to Wyoming server with timeout and logging.
        
        Args:
            host: Wyoming server hostname.
            port: Wyoming server port.
            timeout: Seconds to wait for connection.
            
        Returns:
            True if connected, False on failure.
        """
        self._connection_state = self.STATE_CONNECTING
        self._last_error = None
        self._last_error_time = time.time()
        
        try:
            self.client = AsyncTcpClient(host, port)
            # Use context manager for proper cleanup
            await self.client.__aenter__()
            
            self._connection_state = self.STATE_CONNECTED
            self._last_error = None
            logger.info(f"[Wyoming] Connected to {host}:{port}")
            return True
            
        except asyncio.TimeoutError:
            self._connection_state = self.STATE_DISCONNECTED
            self._last_error = "connection_timeout"
            logger.error(f"[Wyoming] Connection to {host}:{port} timed out after {timeout}s")
            return False
        except ConnectionRefusedError:
            self._connection_state = self.STATE_DISCONNECTED
            self._last_error = "connection_refused"
            logger.error(f"[Wyoming] Connection to {host}:{port} refused (server down?)")
            return False
        except OSError as e:
            self._connection_state = self.STATE_DISCONNECTED
            self._last_error = f"os_error_{e.errno or 'unknown'}"
            logger.error(f"[Wyoming] OS error connecting to {host}:{port}: {e}")
            return False
        except Exception as e:
            self._connection_state = self.STATE_DISCONNECTED
            self._last_error = f"unknown_{type(e).__name__}"
            logger.error(f"[Wyoming] Unexpected error connecting to {host}:{port}: {e}")
            return False
    
    async def disconnect(self) -> None:
        """Gracefully close the Wyoming connection."""
        if self.client is not None:
            try:
                await self.client.__aexit__(None, None, None)
                logger.debug("[Wyoming] Connection closed gracefully")
            except Exception as e:
                logger.warning(f"[Wyoming] Error closing connection: {e}")
            self.client = None
        self._connection_state = self.STATE_DISCONNECTED
        self._session_active = False
    
    def is_connected(self) -> bool:
        """Check if Wyoming connection is healthy.
        
        Returns:
            True if connection state is CONNECTED and client exists.
        """
        return (
            self._connection_state == self.STATE_CONNECTED
            and self.client is not None
        )
    
    async def _reconnect_with_backoff(
        self, config: dict, host: str, port: int
    ) -> bool:
        """Reconnect to Wyoming with exponential backoff.
        
        Args:
            config: Configuration dictionary.
            host: Wyoming server hostname.
            port: Wyoming server port.
            
        Returns:
            True if reconnected, False after max retries.
        """
        max_attempts = config.get("wyoming_reconnect_max_attempts", 3)
        base_delay = config.get("wyoming_reconnect_delay", 1.0)
        
        for attempt in range(1, max_attempts + 1):
            delay = base_delay * (2 ** (attempt - 1))  # 1s, 2s, 4s...
            self._connection_state = self.STATE_RECONNECTING
            logger.info(
                f"[Wyoming] Reconnection attempt {attempt}/{max_attempts} "
                f"in {delay:.1f}s to {host}:{port}"
            )
            await asyncio.sleep(delay)
            
            if await self.connect(host, port):
                self._connection_state = self.STATE_CONNECTED
                logger.info(
                    f"[Wyoming] Reconnected successfully on attempt {attempt}"
                )
                return True
            else:
                logger.warning(
                    f"[Wyoming] Reconnection attempt {attempt}/{max_attempts} failed"
                )
        
        self._connection_state = self.STATE_DISCONNECTED
        logger.error(
            f"[Wyoming] All {max_attempts} reconnection attempts failed. "
            f"Will retry on next transmission."
        )
        return False
    
    async def start_session(
        self, client: AsyncTcpClient, language: Optional[str] = None
    ) -> None:
        """Send transcribe and AudioStart events to begin a transcription session.
        
        Args:
            client: AsyncTcpClient connected to the Wyoming server.
            language: Optional language code (e.g., 'en').
        """
        self.client = client
        self.transcript_parts = []
        self.final_transcript = None
        self._session_active = True
        
        # Send transcribe event (optional, can specify language)
        if language:
            await client.write_event(Transcribe(language=language).event())
        else:
            await client.write_event(Transcribe().event())
        
        # Send AudioStart
        await client.write_event(
            AudioStart(rate=16000, width=2, channels=1).event()
        )
        logger.debug("[Wyoming] AudioStart sent")
    
    async def send_chunk(self, data: bytes) -> None:
        """Stream audio chunks to the server.
        
        Args:
            data: Raw PCM16 audio bytes (16kHz, mono, 16-bit).
            
        Raises:
            Exception: Propagates connection errors for caller to handle.
        """
        if not self._session_active or self.client is None:
            logger.error("[Wyoming] Cannot send chunk: session not active")
            raise RuntimeError("Session not active")
        
        # Send in 1024-byte chunks
        chunk_size = 1024
        for i in range(0, len(data), chunk_size):
            chunk = data[i:i + chunk_size]
            await self.client.write_event(
                AudioChunk(rate=16000, width=2, channels=1, audio=chunk).event()
            )
    
    async def stop_session(self, timeout: float = 30.0) -> Optional[str]:
        """Send AudioStop and wait for the final transcript.
        
        Args:
            timeout: Seconds to wait for the transcript.
            
        Returns:
            The final transcript text, or None on failure.
        """
        if not self._session_active or self.client is None:
            logger.error("[Wyoming] Cannot stop session: session not active")
            return None
        
        # Send AudioStop
        await self.client.write_event(AudioStop().event())
        logger.debug("[Wyoming] AudioStop sent, waiting for transcript...")
        
        # Wait for transcript
        try:
            while True:
                try:
                    event = await asyncio.wait_for(
                        self.client.read_event(), timeout=timeout
                    )
                except asyncio.TimeoutError:
                    logger.warning(
                        f"[Wyoming] No transcript received within {timeout}s"
                    )
                    break
                
                if event is None:
                    logger.warning("[Wyoming] Connection closed by server")
                    break
                
                if Transcript.is_type(event.type):
                    transcript = Transcript.from_event(event)
                    self.final_transcript = transcript.text
                    logger.debug(
                        f"[Wyoming] Transcript received: '{self.final_transcript}'"
                    )
                    break
                
                # Ignore transcript-chunk events (we only need the final result)
        
        except Exception as e:
            error_type = self._classify_error(e)
            logger.error(
                f"[Wyoming] Error waiting for transcript ({error_type}): {e}"
            )
        
        self._session_active = False
        return self.final_transcript
    
    @property
    def is_active(self) -> bool:
        """Check if a session is currently active."""
        return self._session_active
    
    @property
    def connection_state(self) -> str:
        """Current Wyoming connection state."""
        return self._connection_state


class _TransmissionState:
    """Tracks state of a streaming radio transmission.
    
    States:
        IDLE: No active transmission
        WARMUP: Voice detected, buffering audio (AGC stabilization)
        STREAMING: Actively streaming audio to Wyoming
        SILENCE_DETECTED: Voice stopped, waiting for silence timeout
        WAITING_FOR_END: Silence timeout elapsed, about to end transmission
        TRANSCRIBING: Sent AudioStop, waiting for transcript
    """
    
    def __init__(self, config: dict, baseline_tracker: _RmsBaselineTracker) -> None:
        """Initialize the transmission state.
        
        Args:
            config: Configuration dictionary.
            baseline_tracker: VAD baseline tracker instance.
        """
        self.config = config
        self.baseline_tracker = baseline_tracker
        
        # Timing parameters
        self.silence_timeout = config.get("vad_recovery_seconds", 1.0)
        self.silence_recovery = config.get("silence_timeout", 2.0)
        self.min_duration = config.get("min_transmission_duration", 0.3)
        self.max_duration = config.get("max_transmission_duration", 120.0)
        self.warmup_ms = config.get("vad_warmup_ms", 150)
        self.vad_threshold = config.get("vad_threshold", 0.03)
        
        # State tracking
        self.state: str = "IDLE"
        self.start_time: float = 0.0
        self.silence_start: float = 0.0
        self.end_time: float = 0.0
        
        # Audio buffer (for warmup period)
        self.warmup_buffer: bytearray = bytearray()
        
        # Audio recording buffer (full transmission for WAV storage)
        self.recording_buffer: bytearray = bytearray()
        
        # Wyoming streaming
        self.wyoming: _WyomingStreamingClient = _WyomingStreamingClient()
        
        # Transcript result
        self.transcript: Optional[str] = None
    
    @property
    def duration(self) -> float:
        """Current transmission duration in seconds."""
        if self.start_time == 0.0:
            return 0.0
        end = self.end_time if self.end_time > 0 else time.time()
        return end - self.start_time
    
    def reset(self) -> None:
        """Reset state to IDLE."""
        self.state = "IDLE"
        self.start_time = 0.0
        self.silence_start = 0.0
        self.end_time = 0.0
        self.warmup_buffer = bytearray()
        self.recording_buffer = bytearray()
        self.transcript = None
        self.wyoming = _WyomingStreamingClient()


def create_wav_header(data_size: int) -> bytes:
    """Create a minimal WAV header for 16kHz/16-bit/mono PCM data.
    
    Args:
        data_size: Size of the audio data in bytes.
        
    Returns:
        Bytes containing the WAV header.
    """
    sample_rate: int = 16000
    num_channels: int = 1
    bits_per_sample: int = 16
    byte_rate: int = sample_rate * num_channels * bits_per_sample // 8
    block_align: int = num_channels * bits_per_sample // 8
    
    header = struct.pack(
        '<4sI4s4sIHHIIHH4sI',
        b'RIFF',
        36 + data_size,
        b'WAVE',
        b'fmt ',
        16,
        1,  # PCM format
        num_channels,
        sample_rate,
        byte_rate,
        block_align,
        bits_per_sample,
        b'data',
        data_size,
    )
    return header


AUDIO_SAVE_DIR: str = "/config/www/radio-audio"


def save_audio_recording(
    audio_data: bytes,
    frequency: str,
    timestamp_str: str,
) -> Optional[str]:
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


def _save_mqtt_discovery_audio(config, mqtt_client):
    """Publish Home Assistant MQTT Auto Discovery payload for audio sensor."""
    unique_id = f"rtl_fm_{str(config['frequency']).replace('.', '_')}_audio"
    device_name = f"RTL-FM Scanner {config['frequency']}MHz"
    
    discovery_topic = f"homeassistant/sensor/{unique_id}/config"
    discovery_payload = json.dumps({
        "name": "Radio Audio Recording",
        "unique_id": f"{unique_id}_audio",
        "state_topic": config["mqtt_topic"],
        "value_template": "{{ value_json.timestamp if value_json.audio_file else '' }}",
        "json_attributes_topic": config["mqtt_topic"],
        "icon": "mdi:record-rec",
        "device": {
            "identifiers": [f"rtl_fm_{str(config['frequency']).replace('.', '_')}"],
            "name": device_name,
            "model": "RTL-SDR",
            "manufacturer": "RTL-FM Transcriber",
        },
    })
    
    logger.info(
        f"[MQTT] Publishing audio sensor discovery to topic='{discovery_topic}'"
    )
    
    try:
        if not is_mqtt_connected(mqtt_client):
            logger.error("[MQTT] Failed to publish audio discovery — MQTT client is disconnected")
            return
        mqtt_client.publish(discovery_topic, discovery_payload, retain=True)
    except Exception as e:
        logger.error(f"[MQTT] Audio discovery publish failed: {e}")


def is_hallucination(text):
    """Check for common Whisper hallucinations on silence."""
    # Phrase-based hallucinations
    phrase_hallucinations = [
        "thank you for watching",
        "thanks for watching",
        "subs by",
        "subscribe",
        "amara.org",
        "copyright",
        "please subscribe",
        "like and subscribe",
        "transcribed by",
        "subtitles by",
    ]
    # Single-word hallucinations (only if they are the entire text)
    single_word_hallucinations = [
        "you",
    ]
    
    text_lower = text.lower().strip()

    # Empty or very short
    if len(text_lower) < 2:
        return True

    # Check for phrase-based hallucinations
    if any(h in text_lower for h in phrase_hallucinations):
        return True
        
    # Check for single-word hallucinations (exact match)
    if text_lower in single_word_hallucinations:
        return True

    return False


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
    """Async capture loop with persistent pipeline and streaming transcription.

    Implements a state machine for radio transmission segmentation:
    IDLE → WARMUP → STREAMING → SILENCE_DETECTED → WAITING_FOR_END → TRANSCRIBING → IDLE
    
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

        # Transmission state machine
        transmission = _TransmissionState(config, baseline_tracker)
        pipeline_running = True
        stderr_reader_task = None
        # Track bytes per second for warmup buffer sizing
        bytes_per_sec = sample_rate * 2  # 16-bit mono
        warmup_bytes = int(config.get("vad_warmup_ms", 150) / 1000.0 * bytes_per_sec)

        # Wyoming connection pool - one connection per pipeline lifetime
        wyoming_client: Optional[AsyncTcpClient] = None

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
                        sox_proc.stdout.read(4096), timeout=0.05
                    )
                except asyncio.TimeoutError:
                    # No data available - check pipeline health
                    if transmission.state == "STREAMING" or transmission.state == "WAITING_FOR_END":
                        # Check if silence timeout elapsed while streaming
                        if transmission.silence_start > 0:
                            elapsed = time.time() - transmission.silence_start
                            if elapsed >= transmission.silence_recovery:
                                logger.info(
                                    f"[VAD] Silence timeout ({transmission.silence_recovery}s) reached, "
                                    f"ending transmission (duration: {transmission.duration:.1f}s)"
                                )
                                await _end_transmission(transmission, config, mqtt_client,
                                                       wyoming_client)
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
                            wyoming_host = urlparse(config["whisper_url"]).hostname or "localhost"
                            wyoming_port = urlparse(config["whisper_url"]).port or 10300
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
                            await _end_transmission(transmission, config, mqtt_client,
                                                   wyoming_client)
                            wyoming_client = None
                            transmission.reset()

                # In WAITING_FOR_END state, check if silence timeout elapsed
                if transmission.state == "WAITING_FOR_END":
                    if current_time - transmission.silence_start >= transmission.silence_recovery:
                        logger.info(
                            f"[VAD] Silence timeout ({transmission.silence_recovery}s) reached, "
                            f"ending transmission (duration: {transmission.duration:.1f}s)"
                        )
                        await _end_transmission(transmission, config, mqtt_client,
                                               wyoming_client)
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
                            wyoming_host = urlparse(config["whisper_url"]).hostname or "localhost"
                            wyoming_port = urlparse(config["whisper_url"]).port or 10300
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


async def _end_transmission(transmission: _TransmissionState, config: dict,
                           mqtt_client, wyoming_client) -> None:
    """End a transmission: send AudioStop, get transcript, publish to MQTT.
    
    Args:
        transmission: The _TransmissionState to finalize.
        config: Configuration dictionary.
        mqtt_client: MQTT client instance.
        wyoming_client: The Wyoming streaming client (may be None on error).
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
    audio_file_path: Optional[str] = None
    audio_file_name: Optional[str] = None
    
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
        
        mqtt_topic = config["mqtt_topic"]
        mqtt_payload = json.dumps(message)
        logger.info(
            f"[MQTT] Publishing transcription to topic='{mqtt_topic}', "
            f"payload='{mqtt_payload[:100]}...', qos=1"
        )
        
        if not is_mqtt_connected(mqtt_client):
            logger.error(
                "[MQTT] Failed to publish — MQTT client is disconnected "
                "and reconnection attempts failed"
            )
        else:
            try:
                pub_result = mqtt_client.publish(mqtt_topic, mqtt_payload, qos=1)
                logger.info(
                    f"[MQTT] Publish successful — topic='{mqtt_topic}', "
                    f"mid={pub_result.mid}, result_code={pub_result.rc}"
                )
            except Exception as publish_err:
                logger.error(
                    f"[MQTT] Publish failed with exception: type={type(publish_err).__name__}, "
                    f"error={publish_err}"
                )
    
    transmission.state = "IDLE"


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


async def check_wyoming_connection(config: dict) -> bool:
    """Check Wyoming server connectivity on startup with reconnection retry.
    
    Args:
        config: Configuration dictionary.
        
    Returns:
        True if connected, False after max retries.
    """
    wyoming_host = urlparse(config["whisper_url"]).hostname or "localhost"
    wyoming_port = urlparse(config["whisper_url"]).port or 10300
    conn_timeout = config.get("wyoming_connection_timeout", 10.0)
    max_attempts = config.get("wyoming_reconnect_max_attempts", 3)
    base_delay = config.get("wyoming_reconnect_delay", 1.0)
    
    client = _WyomingStreamingClient()
    
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
    
    # Publish audio sensor discovery if recording is enabled
    if config.get("audio_recording", False):
        _save_mqtt_discovery_audio(config, mqtt_client)

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
