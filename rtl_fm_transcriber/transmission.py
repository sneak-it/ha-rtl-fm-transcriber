"""Per-transmission state for the VAD segmentation state machine."""

import time

from .vad import RmsBaselineTracker
from .wyoming_client import WyomingStreamingClient


class TransmissionState:
    """Tracks state of a streaming radio transmission.

    States:
        IDLE: No active transmission
        WARMUP: Voice detected, buffering audio (AGC stabilization)
        STREAMING: Actively streaming audio to Wyoming
        WAITING_FOR_END: Voice stopped, waiting for the silence timeout
    """
    
    def __init__(self, config: dict, baseline_tracker: RmsBaselineTracker) -> None:
        """Initialize the transmission state.
        
        Args:
            config: Configuration dictionary.
            baseline_tracker: VAD baseline tracker instance.
        """
        self.config = config
        self.baseline_tracker = baseline_tracker
        
        # Timing parameters
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
        self.wyoming: WyomingStreamingClient = WyomingStreamingClient()
        
        # Transcript result
        self.transcript: str | None = None
    
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
        self.wyoming = WyomingStreamingClient()
