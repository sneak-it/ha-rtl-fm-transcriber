"""Segmentation state machine tests, driven by synthetic chunk sequences."""

import pytest

from rtl_fm_transcriber.transmission import (
    ACTIVE_STATES,
    BUFFER,
    END,
    HOLD,
    IDLE,
    IGNORE,
    PROMOTE,
    START,
    STREAM,
    STREAMING,
    WAITING_FOR_END,
    WARMUP,
    TransmissionState,
    decide,
)

VOICE = True
SILENT = False

CONFIG = {
    "silence_timeout": 2.0,
    "min_transmission_duration": 0.3,
    "max_transmission_duration": 120.0,
}
# 150ms of 16kHz 16-bit mono
WARMUP_BYTES = 4800
CHUNK = b"\x11\x11" * 2048  # 4096 bytes, the real read size


def test_idle_ignores_silence():
    assert decide(IDLE, SILENT) == IGNORE


def test_idle_starts_on_voice():
    assert decide(IDLE, VOICE) == START


def test_warmup_buffers_until_full():
    assert decide(WARMUP, VOICE, warmup_buffered=2000, warmup_needed=4800) == BUFFER
    assert decide(WARMUP, VOICE, warmup_buffered=4800, warmup_needed=4800) == PROMOTE


@pytest.mark.parametrize("voice", [VOICE, SILENT])
def test_warmup_progresses_on_either_vad_result(voice):
    """A continuous transmission must not stall in WARMUP.

    Regression: warmup only advanced on non-voice chunks, so voice chunks
    arriving in WARMUP matched no branch and were silently discarded.
    """
    assert decide(WARMUP, voice, warmup_buffered=2000, warmup_needed=4800) == BUFFER
    assert decide(WARMUP, voice, warmup_buffered=5000, warmup_needed=4800) == PROMOTE


def test_warmup_timeout_promotes_rather_than_stalling():
    """The 2s safety timeout leaves WARMUP through PROMOTE, not straight to STREAMING.

    Regression: the timeout set STREAMING without connecting to Wyoming, so
    every later chunk was dropped and the transmission vanished.
    """
    action = decide(
        WARMUP, SILENT, warmup_elapsed=2.5, warmup_buffered=100, warmup_needed=4800
    )
    assert action == PROMOTE


def test_streaming_streams_voice():
    assert decide(STREAMING, VOICE) == STREAM


def test_silence_opens_the_window_then_ends():
    """Silence must end a transmission in the normal data path.

    Regression: WAITING_FOR_END was checked but never assigned, so the only
    working end path was a read timeout that -E pad makes rare, and every
    transmission ran to the 120s cap.
    """
    assert decide(STREAMING, SILENT, silence_elapsed=None) == HOLD
    assert decide(WAITING_FOR_END, SILENT, silence_elapsed=0.5, silence_timeout=2.0) == HOLD
    assert decide(WAITING_FOR_END, SILENT, silence_elapsed=2.0, silence_timeout=2.0) == END


def test_voice_resumes_inside_the_silence_window():
    assert decide(WAITING_FOR_END, VOICE, silence_elapsed=1.0) == STREAM


@pytest.mark.parametrize("state", ACTIVE_STATES)
@pytest.mark.parametrize("voice", [VOICE, SILENT])
def test_max_duration_ends_regardless_of_vad(state, voice):
    """A stuck-open carrier must not stream forever.

    Regression: the cap was only checked in the no-voice branch, so continuous
    voice bypassed it entirely.
    """
    assert decide(state, voice, duration=120.0, max_duration=120.0) == END
    assert decide(state, voice, duration=119.9, max_duration=120.0) != END


def test_idle_is_not_capped():
    assert decide(IDLE, SILENT, duration=999.0, max_duration=120.0) == IGNORE


class TestChunkSequences:
    """Drive TransmissionState the way capture_loop does."""

    def _run(self, sequence, warmup_bytes=WARMUP_BYTES):
        """Feed (voice, now) pairs through the machine, collecting actions.

        Mirrors capture_loop's dispatch so buffer accounting stays honest.
        """
        t = TransmissionState(CONFIG, warmup_bytes)
        actions = []
        for voice, now in sequence:
            action = t.next_action(voice, now, incoming=len(CHUNK))
            actions.append(action)
            if action == START:
                t.begin(CHUNK, now)
            elif action == BUFFER:
                t.warmup_buffer.extend(CHUNK)
            elif action == PROMOTE:
                t.warmup_buffer.extend(CHUNK)
                t.state = STREAMING
            elif action == STREAM:
                t.state = STREAMING
                t.silence_start = None
                t.recording_buffer.extend(CHUNK)
            elif action == HOLD:
                if t.silence_start is None:
                    t.silence_start = now
                    t.state = WAITING_FOR_END
            elif action == END:
                t.end_time = now
        return t, actions

    def test_full_transmission_lifecycle(self):
        seq = [(VOICE, 0.0), (VOICE, 0.13), (VOICE, 0.26)]      # warmup fills
        seq += [(VOICE, 0.39), (VOICE, 0.52)]                    # streaming
        seq += [(SILENT, 0.65), (SILENT, 1.0), (SILENT, 2.70)]   # silence -> end
        _, actions = self._run(seq)
        assert actions[0] == START
        assert PROMOTE in actions
        assert actions[-1] == END
        assert actions.index(PROMOTE) < actions.index(END)

    def test_each_chunk_dispatched_once(self):
        """The chunk completing warmup must not be sent and recorded twice.

        Regression: it was appended to the warmup buffer, flushed with it, then
        fell through to the streaming branch and was sent again, so saved WAVs
        stuttered at every transmission start.
        """
        seq = [(VOICE, i * 0.13) for i in range(4)]
        t, actions = self._run(seq)
        assert actions.count(PROMOTE) == 1
        promote_at = actions.index(PROMOTE)
        # Warmup holds exactly the chunks up to and including the promoting one
        assert len(t.warmup_buffer) == (promote_at + 1) * len(CHUNK)
        # and the streamed chunks are only the ones after it
        streamed = sum(1 for a in actions[promote_at + 1:] if a == STREAM)
        assert len(t.recording_buffer) == streamed * len(CHUNK)

    def test_silence_then_voice_does_not_end(self):
        seq = [(VOICE, 0.0), (VOICE, 0.13), (VOICE, 0.26), (VOICE, 0.39)]
        seq += [(SILENT, 0.5), (SILENT, 1.0)]     # inside the 2s window
        seq += [(VOICE, 1.5), (VOICE, 1.6)]       # resumes
        t, actions = self._run(seq)
        assert END not in actions
        assert t.state == STREAMING

    def test_two_back_to_back_transmissions(self):
        first = [(VOICE, i * 0.13) for i in range(4)] + [(SILENT, 0.6), (SILENT, 2.7)]
        t, actions = self._run(first)
        assert actions[-1] == END
        t.reset()
        assert t.state == IDLE
        assert t.warmup_buffer == bytearray()
        assert t.next_action(VOICE, 3.0, incoming=len(CHUNK)) == START

    def test_squelch_padding_never_starts_a_transmission(self):
        """Digital silence is not a transmission.

        Regression: under the old inverted-RMS rule, zero-padded output from a
        closed squelch was the most voice-like value possible and streamed an
        endless transmission of silence to Whisper.
        """
        t, actions = self._run([(SILENT, i * 0.13) for i in range(50)])
        assert set(actions) == {IGNORE}
        assert t.state == IDLE

    def test_duration_freezes_at_end_time(self):
        t, _ = self._run([(VOICE, 0.0), (VOICE, 0.13)])
        t.end_time = 5.0
        assert t.duration == pytest.approx(5.0)
        assert t.duration == pytest.approx(5.0)  # stable, not wall-clock


def test_state_constants_are_the_whole_set():
    """Nothing should reference a state the machine never assigns."""
    assert set(ACTIVE_STATES) == {WARMUP, STREAMING, WAITING_FOR_END}
    assert IDLE not in ACTIVE_STATES
