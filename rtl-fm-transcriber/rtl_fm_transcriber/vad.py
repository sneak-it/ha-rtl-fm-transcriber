"""Voice detection for a squelch-gated rtl_fm stream.

rtl_fm runs with `-l <squelch> -t <blocks>`: while the squelch is closed it
writes zeros, and when a carrier opens it writes real audio. Detection is
therefore "is rtl_fm passing real samples", not an amplitude judgement about
speech, and the squelch option is the sensitivity knob.
"""

# sox dither leaves closed-squelch output near 1 LSB (2e-5); real audio is far above this.
SILENCE_FLOOR = 0.001


def is_voice(rms: float) -> bool:
    """True when a chunk carries real audio rather than squelch padding."""
    return rms > SILENCE_FLOOR
