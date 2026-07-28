"""Transmission segmentation: the state set, the transition table, and per-transmission state.

`decide()` is pure so the segmentation rules can be tested against synthetic
chunk sequences without an RTL-SDR dongle or a Wyoming server.
"""

import time

# States
IDLE = "IDLE"
WARMUP = "WARMUP"
STREAMING = "STREAMING"
WAITING_FOR_END = "WAITING_FOR_END"

ACTIVE_STATES = (WARMUP, STREAMING, WAITING_FOR_END)

# Actions decide() returns for one chunk
IGNORE = "IGNORE"    # idle air, nothing to do
START = "START"      # open a transmission, buffer this chunk
BUFFER = "BUFFER"    # keep buffering warmup audio
PROMOTE = "PROMOTE"  # leave WARMUP: connect, open the session, flush the buffer
STREAM = "STREAM"    # send this chunk
HOLD = "HOLD"        # inside the silence window, hold this chunk back
END = "END"          # finalize the transmission

# Cap on how long WARMUP may buffer before streaming starts regardless.
WARMUP_TIMEOUT = 2.0


def decide(
    state,
    voice,
    *,
    duration=0.0,
    warmup_elapsed=0.0,
    warmup_buffered=0,
    warmup_needed=0,
    silence_elapsed=None,
    max_duration=120.0,
    silence_timeout=2.0,
    warmup_timeout=WARMUP_TIMEOUT,
):
    """Return the action for one chunk.

    `warmup_buffered` counts the incoming chunk. `silence_elapsed` is None when
    no silence run is open.
    """
    # Checked on every chunk, so a stuck-open carrier cannot stream forever.
    if state in ACTIVE_STATES and duration >= max_duration:
        return END

    if state == IDLE:
        return START if voice else IGNORE

    if state == WARMUP:
        # Buffer regardless of this chunk's VAD result, and leave warmup on
        # either a full buffer or the timeout. Both exits go through PROMOTE so
        # the Wyoming session is always opened before streaming starts.
        if warmup_buffered >= warmup_needed or warmup_elapsed >= warmup_timeout:
            return PROMOTE
        return BUFFER

    # STREAMING or WAITING_FOR_END
    if voice:
        return STREAM
    if silence_elapsed is not None and silence_elapsed >= silence_timeout:
        return END
    return HOLD


class TransmissionState:
    """Tracks one radio transmission through the segmentation state machine.

    States: IDLE -> WARMUP -> STREAMING -> WAITING_FOR_END -> IDLE, with
    STREAMING and WAITING_FOR_END alternating while voice comes and goes inside
    the silence window.
    """

    def __init__(self, config: dict, warmup_bytes: int) -> None:
        self.config = config
        self.warmup_bytes = warmup_bytes

        # Timing parameters
        self.silence_timeout = config.get("silence_timeout", 2.0)
        self.min_duration = config.get("min_transmission_duration", 0.3)
        self.max_duration = config.get("max_transmission_duration", 120.0)

        # None means "not set"; 0.0 is a legitimate timestamp.
        self.state: str = IDLE
        self.start_time: float | None = None
        self.silence_start: float | None = None
        self.end_time: float | None = None

        # Audio buffered during warmup, then the rest of the transmission
        self.warmup_buffer: bytearray = bytearray()
        self.recording_buffer: bytearray = bytearray()


    @property
    def duration(self) -> float:
        """Transmission duration in seconds, frozen once end_time is set."""
        if self.start_time is None:
            return 0.0
        end = self.end_time if self.end_time is not None else time.time()
        return end - self.start_time

    def next_action(self, voice: bool, now: float, incoming: int = 0) -> str:
        """Apply the transition table to the current state."""
        elapsed = now - self.start_time if self.start_time is not None else 0.0
        return decide(
            self.state,
            voice,
            duration=elapsed,
            warmup_elapsed=elapsed,
            warmup_buffered=len(self.warmup_buffer) + incoming,
            warmup_needed=self.warmup_bytes,
            silence_elapsed=(
                (now - self.silence_start) if self.silence_start is not None else None
            ),
            max_duration=self.max_duration,
            silence_timeout=self.silence_timeout,
        )

    def begin(self, chunk: bytes, now: float) -> None:
        """Open a transmission in WARMUP with its first chunk."""
        self.state = WARMUP
        self.start_time = now
        self.silence_start = None
        self.end_time = None
        self.warmup_buffer = bytearray(chunk)

    def reset(self) -> None:
        """Reset to IDLE, ready for the next transmission."""
        self.state = IDLE
        self.start_time = None
        self.silence_start = None
        self.end_time = None
        self.warmup_buffer = bytearray()
        self.recording_buffer = bytearray()
