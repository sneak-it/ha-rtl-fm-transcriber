"""Voice detection for a squelch-gated rtl_fm stream.

rtl_fm runs with `-l <squelch> -E pad`: while the squelch is closed it emits
zero-padded samples, and when a carrier opens it emits real audio. Detection is
therefore "is rtl_fm passing real samples", not an amplitude judgement about
speech, and the squelch option is the sensitivity knob.

This replaces an inverted-RMS baseline detector that was incompatible with
`-E pad`: padded zeros are the most voice-like value under a "quieter than
baseline" rule, so idle air read as an endless transmission.
"""

# Padding is exact zeros, and real audio is orders of magnitude above this.
SILENCE_FLOOR = 0.001


def is_voice(rms: float) -> bool:
    """True when a chunk carries real audio rather than squelch padding."""
    return rms > SILENCE_FLOOR
