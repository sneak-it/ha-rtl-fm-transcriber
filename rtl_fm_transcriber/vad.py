"""RMS baseline tracking for the inverted-RMS RTL-SDR VAD."""

import logging
import time

logger = logging.getLogger(__name__)


class RmsBaselineTracker:
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
        self._initial_baseline: float | None = None
    
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
