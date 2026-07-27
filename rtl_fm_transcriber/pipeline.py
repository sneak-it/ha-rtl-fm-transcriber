"""The rtl_fm -> sox capture pipeline: spawn, stderr drain, teardown."""

import asyncio
import contextlib
import logging

logger = logging.getLogger(__name__)

PROC_TERM_TIMEOUT = 5.0
PIPE_TASK_TIMEOUT = 2.0


async def start_pipeline(config, frequency_hz, sample_rate, capture_rate):
    """Start the rtl_fm -> sox audio capture pipeline using asyncio subprocess.

    Returns:
        Tuple of (rtl_proc, sox_proc) asyncio subprocess objects, or None on failure.
    """
    squelch_val = str(config.get("squelch", 50))
    ppm_val = config.get("ppm", 0)

    # Build rtl_fm command for narrowband FM (public safety radio)
    # -f: frequency in Hz
    # -M fm: narrowband FM demodulation
    # -s: sample rate (12000 for 12.5 kHz channels)
    # -l: squelch threshold (0-200)
    # -E dc: remove DC offset
    # -E pad: output silence when squelched (prevents pipe starvation)
    # -p: PPM correction for dongle clock drift
    rtl_cmd = [
        "rtl_fm",
        "-f",
        str(frequency_hz),
        "-M",
        "fm",
        "-s",
        str(capture_rate),
        "-l",
        squelch_val,
        "-E",
        "dc",  # Remove DC offset
        "-E",
        "pad",  # Output silence when squelched (prevents pipe starvation)
        "-p",
        str(ppm_val),  # PPM correction
    ]

    if config.get("gain") and config["gain"] != "auto":
        rtl_cmd.extend(["-g", str(config["gain"])])

    rtl_cmd.append("-")

    # Build sox command to resample and optionally apply bandpass filter
    # Input: capture_rate (12000), signed 16-bit, mono, raw
    # Output: sample_rate (16000), signed 16-bit, mono, raw
    sox_cmd = [
        "sox",
        "-t",
        "raw",
        "-r",
        str(capture_rate),
        "-e",
        "signed",
        "-b",
        "16",
        "-c",
        "1",
        "-",
        "-r",
        str(sample_rate),
        "-e",
        "signed",
        "-b",
        "16",
        "-t",
        "raw",
        "-",
    ]

    # Add bandpass filter for voice clarity (300-3000 Hz default)
    # This removes low-frequency hum and high-frequency static
    if config.get("bandpass_filter", True):
        bandpass_low = config.get("bandpass_low", 300)
        bandpass_high = config.get("bandpass_high", 3000)
        sox_cmd.extend(
            [
                "highpass",
                str(bandpass_low),  # Remove low-frequency noise
                "lowpass",
                str(bandpass_high),  # Remove high-frequency static
                "gain",
                "-3",  # Compensate for volume loss from filtering
            ]
        )

    try:
        # Start rtl_fm first
        rtl_proc = await asyncio.create_subprocess_exec(
            *rtl_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        # Start sox with asyncio.PIPE stdin (not rtl_proc.stdout directly,
        # because StreamReader has no fileno() for subprocess.Popen)
        sox_proc = await asyncio.create_subprocess_exec(
            *sox_cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        # Background task: pipe rtl_fm stdout -> sox stdin asynchronously
        async def _pipe_streams(reader, writer):
            """Copy all data from reader to writer until EOF, with backpressure."""
            try:
                while True:
                    chunk = await reader.read(65536)  # 64 KiB chunks
                    if not chunk:
                        break
                    writer.write(chunk)
                    # Without this, a stalled sox lets rtl_fm output pile up in
                    # memory unbounded.
                    await writer.drain()
                writer.close()
                await writer.wait_closed()
            except (asyncio.CancelledError, Exception):
                writer.close()
                with contextlib.suppress(asyncio.InvalidStateError, RuntimeError):
                    await writer.wait_closed()
                raise

        _pipe_task = asyncio.create_task(
            _pipe_streams(rtl_proc.stdout, sox_proc.stdin)
        )
        # Store the task on the pipeline so it can be awaited during cleanup
        sox_proc._pipe_task = _pipe_task

        return rtl_proc, sox_proc
    except (OSError, FileNotFoundError) as e:
        logger.error(f"Failed to start audio pipeline: {e}")
        return None


async def read_stderr_pipeline(rtl_proc, sox_proc):
    """Log stderr from both pipeline processes until they close it."""
    return await asyncio.gather(
        _read_stderr_lines(rtl_proc, "RTL_FM"),
        _read_stderr_lines(sox_proc, "SOX"),
        return_exceptions=True,
    )


async def _read_stderr_lines(proc, label):
    """Continuously read stderr lines from a subprocess and log them."""
    try:
        while True:
            line = await proc.stderr.readline()
            if not line:
                break
            decoded = line.decode("utf-8", errors="replace").strip()
            if decoded:
                logger.info(f"{label} Log: {decoded}")
    except asyncio.CancelledError:
        raise  # Let cancellation propagate so the caller's cancel() works
    except Exception as e:
        logger.debug(f"{label} stderr reader stopped: {e}")


async def cleanup_pipeline(rtl_proc, sox_proc):
    """Terminate the pipeline processes and stop the pipe task.

    Processes are terminated first. Waiting for the pipe task before that would
    mean waiting for an EOF from rtl_fm that `-E pad` never produces, which cost
    the full timeout on every restart and every add-on stop.
    """
    # Terminate rtl_fm first so the pipe task sees EOF, then sox.
    for name, proc in (("RTL_FM", rtl_proc), ("SOX", sox_proc)):
        if proc is None:
            continue
        try:
            if proc.returncode is None:  # Still running
                proc.terminate()
                try:
                    await asyncio.wait_for(proc.wait(), timeout=PROC_TERM_TIMEOUT)
                except TimeoutError:
                    logger.warning(f"{name} did not terminate gracefully, killing")
                    proc.kill()
                    await proc.wait()
        except ProcessLookupError:
            logger.debug(f"{name} process already exited")
        except OSError as e:
            logger.debug(f"Error cleaning up {name}: {e}")

    pipe_task = getattr(sox_proc, "_pipe_task", None) if sox_proc else None
    if pipe_task is not None and not pipe_task.done():
        pipe_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await asyncio.wait_for(pipe_task, timeout=PIPE_TASK_TIMEOUT)
