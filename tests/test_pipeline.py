"""Pipeline teardown tests: no hangs, no leaked reader tasks."""

import asyncio

import pytest

from rtl_fm_transcriber.pipeline import cleanup_pipeline, read_stderr_pipeline


class FakeStream:
    """A stderr/stdout stream that yields queued lines then blocks forever."""

    def __init__(self, lines=()):
        self.lines = list(lines)
        self.blocked = False

    async def readline(self):
        if self.lines:
            return self.lines.pop(0)
        # A live process holds stderr open without sending anything.
        self.blocked = True
        await asyncio.sleep(3600)


class FakeProc:
    """Minimal asyncio subprocess stand-in."""

    def __init__(self, stderr_lines=(), exits=True):
        self.returncode = None
        self.stderr = FakeStream(stderr_lines)
        self.terminated = False
        self.killed = False
        self._exits = exits
        self._exited = asyncio.Event()

    def _exit(self, code):
        self.returncode = code
        self._exited.set()

    def terminate(self):
        self.terminated = True
        if self._exits:
            self._exit(-15)

    def kill(self):
        self.killed = True
        self._exit(-9)

    async def wait(self):
        await self._exited.wait()
        return self.returncode


async def test_cleanup_terminates_both_processes():
    rtl, sox = FakeProc(), FakeProc()
    await cleanup_pipeline(rtl, sox)
    assert rtl.terminated and sox.terminated


async def test_cleanup_does_not_wait_on_a_pipe_task_that_never_ends():
    """Teardown must not wait for an EOF that -E pad never produces.

    Regression: cleanup waited up to 10s for the pipe task before terminating
    rtl_fm, but the pipe task only exits on rtl_fm EOF, so every restart and
    every add-on stop burned the full timeout.
    """
    rtl, sox = FakeProc(), FakeProc()
    never_ends = asyncio.create_task(asyncio.sleep(3600))
    sox._pipe_task = never_ends

    loop = asyncio.get_running_loop()
    started = loop.time()
    await cleanup_pipeline(rtl, sox)
    assert loop.time() - started < 1.0, "teardown stalled"
    assert never_ends.cancelled() or never_ends.done()


async def test_cleanup_kills_a_process_that_ignores_terminate():
    rtl = FakeProc(exits=False)
    sox = FakeProc()
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("rtl_fm_transcriber.pipeline.PROC_TERM_TIMEOUT", 0.05)
        await cleanup_pipeline(rtl, sox)
    assert rtl.terminated and rtl.killed


async def test_cleanup_tolerates_an_already_exited_process():
    rtl, sox = FakeProc(), FakeProc()
    rtl._exit(0)
    await cleanup_pipeline(rtl, sox)
    assert not rtl.terminated


async def test_stderr_readers_are_cancellable():
    """Cancelling the stderr task must actually stop the readers.

    Regression: read_stderr_pipeline returned an un-awaited gather, so the
    wrapping task completed immediately, cancel() cancelled nothing, and the
    inner readers leaked across every pipeline restart.
    """
    rtl, sox = FakeProc(), FakeProc()
    task = asyncio.create_task(read_stderr_pipeline(rtl, sox))
    await asyncio.sleep(0.05)

    assert not task.done(), "returned before the readers finished"
    assert rtl.stderr.blocked and sox.stderr.blocked

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_stderr_readers_log_until_eof(caplog):
    rtl = FakeProc(stderr_lines=[b"Tuned to 155.107 MHz\n", b""])
    sox = FakeProc(stderr_lines=[b""])
    with caplog.at_level("INFO"):
        await read_stderr_pipeline(rtl, sox)
    assert "Tuned to 155.107 MHz" in caplog.text
