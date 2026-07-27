"""Wyoming connection lifecycle tests: timeouts, socket closure, retry timing."""

import asyncio
import typing

import pytest

from rtl_fm_transcriber import wyoming_client as wc
from rtl_fm_transcriber.wyoming_client import WyomingStreamingClient


class FakeTcpClient:
    """Stands in for AsyncTcpClient, recording enter/exit calls."""

    instances: typing.ClassVar[list["FakeTcpClient"]] = []

    def __init__(self, host, port, enter_delay=0.0, fail=None):
        self.host = host
        self.port = port
        self.enter_delay = enter_delay
        self.fail = fail
        self.entered = False
        self.exited = False
        FakeTcpClient.instances.append(self)

    async def __aenter__(self):
        if self.enter_delay:
            await asyncio.sleep(self.enter_delay)
        if self.fail:
            raise self.fail
        self.entered = True
        return self

    async def __aexit__(self, *exc):
        self.exited = True


@pytest.fixture(autouse=True)
def _reset_instances():
    FakeTcpClient.instances = []
    yield
    FakeTcpClient.instances = []


def _patch(monkeypatch, **kwargs):
    monkeypatch.setattr(
        wc, "AsyncTcpClient", lambda host, port: FakeTcpClient(host, port, **kwargs)
    )


async def test_connect_succeeds(monkeypatch):
    _patch(monkeypatch)
    client = WyomingStreamingClient()
    assert await client.connect("host", 10300) is True
    assert client.is_connected()


async def test_connect_applies_its_timeout(monkeypatch):
    """A black-holed host must not block for the OS TCP timeout.

    Regression: connect() accepted a timeout, logged it, and never wrapped the
    connection attempt in it, so audio piled up for minutes.
    """
    _patch(monkeypatch, enter_delay=10.0)
    client = WyomingStreamingClient()

    loop = asyncio.get_running_loop()
    started = loop.time()
    assert await client.connect("blackhole", 10300, timeout=0.05) is False
    assert loop.time() - started < 1.0
    assert not client.is_connected()


async def test_connect_closes_the_previous_socket(monkeypatch):
    """Each attempt must not orphan a half-open socket.

    Regression: connect() assigned a fresh client over the previous one, so
    every reconnect leaked a connection.
    """
    _patch(monkeypatch)
    client = WyomingStreamingClient()
    await client.connect("host", 10300)
    first = FakeTcpClient.instances[0]

    await client.connect("host", 10300)
    assert first.exited, "previous socket was left open"
    assert len(FakeTcpClient.instances) == 2


async def test_connect_refused_reports_failure(monkeypatch):
    _patch(monkeypatch, fail=ConnectionRefusedError())
    client = WyomingStreamingClient()
    assert await client.connect("host", 10300) is False
    assert not client.is_connected()


async def test_disconnect_is_idempotent(monkeypatch):
    _patch(monkeypatch)
    client = WyomingStreamingClient()
    await client.connect("host", 10300)
    await client.disconnect()
    await client.disconnect()
    assert client.client is None
    assert not client.is_connected()


async def test_retry_tries_immediately_before_backing_off(monkeypatch):
    """The first attempt must not be preceded by a sleep.

    Regression: both retry paths slept before attempt 1, adding a pointless
    delay to every startup and reconnect.
    """
    _patch(monkeypatch)
    sleeps = []

    async def fake_sleep(d):
        sleeps.append(d)

    monkeypatch.setattr(wc.asyncio, "sleep", fake_sleep)
    client = WyomingStreamingClient()
    assert await client.reconnect_with_backoff({}, "host", 10300) is True
    assert sleeps == [], "slept before the first attempt"


async def test_retry_backs_off_between_attempts(monkeypatch):
    _patch(monkeypatch, fail=ConnectionRefusedError())
    sleeps = []

    async def fake_sleep(d):
        sleeps.append(d)

    monkeypatch.setattr(wc.asyncio, "sleep", fake_sleep)
    client = WyomingStreamingClient()
    config = {"wyoming_reconnect_max_attempts": 4, "wyoming_reconnect_delay": 1.0}
    assert await client.reconnect_with_backoff(config, "host", 10300) is False
    # Attempt 1 immediate, then exponential between the rest
    assert sleeps == [1.0, 2.0, 4.0]
    assert len(FakeTcpClient.instances) == 4


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (ConnectionRefusedError(), "connection_refused"),
        (ConnectionResetError(), "connection_reset"),
        (BrokenPipeError(), "connection_lost"),
        (TimeoutError(), "timeout"),
        # Python maps ETIMEDOUT onto TimeoutError, so this is not an os_error
        (OSError(110, "timed out"), "timeout"),
        (OSError(999, "weird"), "os_error_999"),
        (RuntimeError("Session not active"), "unknown_RuntimeError"),
    ],
)
def test_classify_error(exc, expected):
    assert WyomingStreamingClient().classify_error(exc) == expected
