"""PCM16 amplitude math and WAV container framing."""

import math
import struct


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
