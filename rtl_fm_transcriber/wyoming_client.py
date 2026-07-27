"""Streaming ASR client for a Wyoming (faster-whisper) server."""

import asyncio
import logging

from wyoming.asr import Transcribe, Transcript
from wyoming.audio import AudioChunk, AudioStart, AudioStop
from wyoming.client import AsyncTcpClient

logger = logging.getLogger(__name__)


class WyomingStreamingClient:
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
        self.client: AsyncTcpClient | None = None
        self.transcript_parts: list[str] = []
        self.final_transcript: str | None = None
        self._session_active: bool = False
        self._connection_state: str = self.STATE_DISCONNECTED

    def classify_error(self, e: Exception) -> str:
        """Classify a Wyoming error for logging and reconnect decisions.

        Python maps ETIMEDOUT and ECONNRESET onto TimeoutError and
        ConnectionResetError, so checking errno for those is unreachable.
        """
        if isinstance(e, ConnectionRefusedError):
            return "connection_refused"
        elif isinstance(e, ConnectionResetError):
            return "connection_reset"
        elif isinstance(e, BrokenPipeError):
            return "connection_lost"
        elif isinstance(e, TimeoutError):
            return "timeout"
        elif isinstance(e, OSError):
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
        # Close any previous socket first; otherwise each attempt orphans one.
        await self.disconnect()
        self._connection_state = self.STATE_CONNECTING

        try:
            self.client = AsyncTcpClient(host, port)
            await asyncio.wait_for(self.client.__aenter__(), timeout)

            self._connection_state = self.STATE_CONNECTED
            logger.info(f"[Wyoming] Connected to {host}:{port}")
            return True

        except TimeoutError:
            self._connection_state = self.STATE_DISCONNECTED
            logger.error(f"[Wyoming] Connection to {host}:{port} timed out after {timeout}s")
            return False
        except ConnectionRefusedError:
            self._connection_state = self.STATE_DISCONNECTED
            logger.error(f"[Wyoming] Connection to {host}:{port} refused (server down?)")
            return False
        except OSError as e:
            self._connection_state = self.STATE_DISCONNECTED
            logger.error(f"[Wyoming] OS error connecting to {host}:{port}: {e}")
            return False
        except Exception as e:
            self._connection_state = self.STATE_DISCONNECTED
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
    
    async def reconnect_with_backoff(
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
        conn_timeout = config.get("wyoming_connection_timeout", 10.0)

        for attempt in range(1, max_attempts + 1):
            if attempt > 1:
                # Back off between attempts, but try immediately first.
                delay = base_delay * (2 ** (attempt - 2))  # 1s, 2s, 4s...
                logger.info(f"[Wyoming] Waiting {delay:.1f}s before retry")
                await asyncio.sleep(delay)

            self._connection_state = self.STATE_RECONNECTING
            logger.info(
                f"[Wyoming] Connection attempt {attempt}/{max_attempts} "
                f"to {host}:{port}"
            )
            if await self.connect(host, port, timeout=conn_timeout):
                logger.info(f"[Wyoming] Connected on attempt {attempt}")
                return True
            logger.warning(
                f"[Wyoming] Connection attempt {attempt}/{max_attempts} failed"
            )

        self._connection_state = self.STATE_DISCONNECTED
        logger.error(
            f"[Wyoming] All {max_attempts} connection attempts failed to "
            f"{host}:{port}"
        )
        return False
    
    async def start_session(
        self, client: AsyncTcpClient, language: str | None = None
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
    
    async def stop_session(self, timeout: float = 30.0) -> str | None:
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
                except TimeoutError:
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
            error_type = self.classify_error(e)
            logger.error(
                f"[Wyoming] Error waiting for transcript ({error_type}): {e}"
            )
        
        self._session_active = False
        return self.final_transcript
    
    @property
    def is_active(self) -> bool:
        """Check if a session is currently active."""
        return self._session_active
