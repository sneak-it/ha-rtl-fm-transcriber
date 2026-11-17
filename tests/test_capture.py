"""Capture loop tests: a scripted sox stream through the real segmentation path."""

import asyncio
import json
import time
from datetime import datetime

import pytest

from rtl_fm_transcriber import capture

VOICE = b"\x00\x10" * 2048  # 4096 bytes of loud audio, 0.128s
ZEROS = bytes(4096)
CONFIG = {
    "frequency": 155.1075,
    "whisper_url": "tcp://stt:10300",
    "mqtt_topic": "radio/transcription",
    "silence_timeout": 0.2,
    "min_transmission_duration": 0.3,
    "max_transmission_duration": 120.0,
}


class ScriptedStream:
    """sox stdout stand-in. A float in the script is a gap with no data; then EOF."""

    def __init__(self, script):
        self.script = list(script)
        self.resume_at = 0.0

    async def read(self, _n):
        if self.script and isinstance(self.script[0], float):
            self.resume_at = time.monotonic() + self.script.pop(0)
        # Polled, so the gap survives the capture loop cancelling this read.
        while time.monotonic() < self.resume_at:  # noqa: ASYNC110
            await asyncio.sleep(0.01)
        return self.script.pop(0) if self.script else b""


class FakeStderr:
    async def readline(self):
        return b""


class FakeProc:
    def __init__(self, script):
        self.stdout = ScriptedStream(script)
        self.stderr = FakeStderr()


class FakeWyoming:
    """Records what one transmission streamed and whether it was transcribed."""

    sessions: list = []  # noqa: RUF012 - reset per test by the fixture

    def __init__(self):
        self.client = object()
        self.sent = bytearray()
        self.transcribed = False
        self.is_active = False
        FakeWyoming.sessions.append(self)

    def is_connected(self):
        return False

    async def reconnect_with_backoff(self, _config, _host, _port):
        return True

    async def start_session(self, _client):
        self.is_active = True

    async def send_chunk(self, data):
        self.sent.extend(data)

    async def stop_session(self, timeout=30.0):
        self.transcribed = True
        return "Engine 4 responding"

    async def disconnect(self):
        self.is_active = False


class FakeMqtt:
    def __init__(self):
        self.published = []

    def is_connected(self):
        return True

    def publish(self, _topic, payload, qos=0, retain=False):
        self.published.append(json.loads(payload))
        return type("Result", (), {"mid": 1, "rc": 0})


@pytest.fixture(autouse=True)
def fakes(monkeypatch):
    FakeWyoming.sessions = []
    monkeypatch.setattr(capture, "WyomingStreamingClient", FakeWyoming)
    monkeypatch.setattr(capture, "STALL_SECONDS", 0.1)


async def run(script, **overrides):
    mqtt = FakeMqtt()
    proc = FakeProc(script)
    await capture._run_pipeline(proc, proc, CONFIG | overrides, mqtt, 4800)
    return mqtt.published


async def test_silence_chunks_end_a_transmission():
    began = time.time()
    published = await run([VOICE] * 5 + [ZEROS] * 2 + [0.5])
    assert [m["text"] for m in published] == ["Engine 4 responding"]
    streamed = [s for s in FakeWyoming.sessions if s.sent]
    assert len(streamed) == 1 and bytes(streamed[0].sent) == VOICE * 5

    # Stamped when the transmission began, not when its transcript came back.
    stamp = datetime.fromisoformat(published[0]["timestamp"]).timestamp()
    assert began <= stamp < began + 0.15


async def test_a_stalled_stream_ends_a_transmission():
    """A stream that stops right after voice must not leave the transmission open.

    Regression: only a silence chunk opened the silence window, so when rtl_fm
    stopped writing before one arrived, the transmission stayed open until the
    next carrier (and was merged into it) or the max-duration cap.
    """
    published = await run([VOICE] * 5 + [0.6])
    assert len(published) == 1


async def test_separate_transmissions_get_separate_transcripts():
    published = await run(([VOICE] * 4 + [ZEROS, 0.4]) * 2)
    assert len(published) == 2


async def test_a_blip_is_discarded_untranscribed():
    """One chunk of noise from a flickering squelch never reaches Whisper.

    Regression: the minimum was compared against wall-clock duration, which
    includes the silence timeout, so nothing was ever short enough to discard.
    """
    published = await run(
        [VOICE, ZEROS, ZEROS, 0.9],
        silence_timeout=0.4, min_transmission_duration=0.3,
    )
    assert published == []
    assert not any(s.transcribed for s in FakeWyoming.sessions)
