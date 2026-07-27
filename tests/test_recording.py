"""Recording filenames, WAV framing, retention cleanup, and hallucination filtering."""

import os
import struct
import time
import wave
from datetime import UTC, datetime, timedelta, timezone

import pytest

from rtl_fm_transcriber.audio import compute_rms, create_wav_header
from rtl_fm_transcriber.filters import is_hallucination
from rtl_fm_transcriber.recording import (
    cleanup_old_recordings,
    recording_filename,
    save_audio_recording,
)

NY = timezone(timedelta(hours=-4))  # a negative UTC offset
BERLIN = timezone(timedelta(hours=2))
STAMP = datetime(2026, 7, 27, 12, 15, 0, tzinfo=NY)


class TestFilenames:
    def test_matches_the_documented_format(self):
        assert recording_filename(STAMP, "155.1075") == "20260727-121500-155_1075.wav"

    @pytest.mark.parametrize("tz", [NY, BERLIN, UTC])
    def test_offset_never_leaks_into_the_name(self, tz):
        """Regression: the name was scrubbed out of an ISO timestamp.

        .replace("-", "") stripped the minus sign from a negative offset and
        .split("+")[0] only handled positive ones, so US-timezone users got
        names like 20260727T1215000400-155_1075.wav, with the offset digits
        fused into the time and the T left in.
        """
        name = recording_filename(STAMP.astimezone(tz), "155.1075")
        stem = name.removesuffix(".wav")
        date_part, time_part, freq_part = stem.split("-")
        assert "T" not in stem
        assert len(date_part) == 8 and date_part.isdigit()
        assert len(time_part) == 6 and time_part.isdigit()
        assert freq_part == "155_1075"

    def test_collisions_get_a_suffix(self):
        assert recording_filename(STAMP, "155.1075", 2).endswith("-155_1075_2.wav")


class TestSaving:
    def test_writes_a_playable_wav(self, tmp_path):
        pcm = struct.pack("<8h", *([4000, -4000] * 4))
        path = save_audio_recording(pcm, "155.1075", STAMP, str(tmp_path))
        assert path is not None

        with wave.open(path) as w:
            assert w.getnchannels() == 1
            assert w.getsampwidth() == 2
            assert w.getframerate() == 16000
            assert w.readframes(w.getnframes()) == pcm

    def test_does_not_overwrite_an_existing_recording(self, tmp_path):
        first = save_audio_recording(b"\x00\x00", "155.1075", STAMP, str(tmp_path))
        second = save_audio_recording(b"\x01\x01", "155.1075", STAMP, str(tmp_path))
        assert first != second
        assert len(list(tmp_path.iterdir())) == 2

    def test_reports_failure_on_an_unwritable_directory(self, tmp_path):
        blocker = tmp_path / "blocked"
        blocker.write_text("not a directory")
        assert save_audio_recording(b"\x00\x00", "155.1", STAMP, str(blocker)) is None


class TestCleanup:
    def _wav(self, tmp_path, name, age_days):
        f = tmp_path / name
        f.write_bytes(create_wav_header(0))
        old = time.time() - age_days * 86400
        os.utime(f, (old, old))
        return f

    def test_removes_files_past_retention(self, tmp_path):
        old = self._wav(tmp_path, "old.wav", 10)
        fresh = self._wav(tmp_path, "fresh.wav", 1)
        cleanup_old_recordings(7, 0, str(tmp_path))
        assert not old.exists()
        assert fresh.exists()

    def test_enforces_max_files(self, tmp_path):
        for i in range(5):
            self._wav(tmp_path, f"r{i}.wav", i)
        cleanup_old_recordings(365, 2, str(tmp_path))
        assert len(list(tmp_path.glob("*.wav"))) == 2

    def test_does_not_retry_files_retention_already_deleted(self, tmp_path, caplog):
        """Regression: the max_files pass reprocessed entries the retention pass
        had deleted, logging spurious "Failed to remove" warnings."""
        for i in range(4):
            self._wav(tmp_path, f"old{i}.wav", 30)
        self._wav(tmp_path, "fresh.wav", 0)

        with caplog.at_level("WARNING"):
            cleanup_old_recordings(7, 1, str(tmp_path))

        assert "Failed to remove" not in caplog.text
        assert [f.name for f in tmp_path.glob("*.wav")] == ["fresh.wav"]

    def test_ignores_non_wav_files(self, tmp_path):
        keep = tmp_path / "notes.txt"
        keep.write_text("hi")
        os.utime(keep, (0, 0))
        cleanup_old_recordings(1, 0, str(tmp_path))
        assert keep.exists()

    def test_missing_directory_is_not_an_error(self, tmp_path):
        cleanup_old_recordings(7, 0, str(tmp_path / "nope"))


class TestHallucinationFilter:
    @pytest.mark.parametrize(
        "text",
        [
            "Thank you for watching",
            "thanks for watching",
            "Please subscribe",
            "Subscribe to my channel",
            "copyright 2026",
            "transcribed by someone",
            "you",
            "You",
            "a",
            "",
        ],
    )
    def test_filters_hallucinations(self, text):
        assert is_hallucination(text) is True

    @pytest.mark.parametrize(
        "text",
        [
            # "subscriber" is routine trunked-radio jargon and contains
            # "subscribe"; substring matching discarded these transmissions.
            "Engine 12 to subscriber unit on channel 3",
            "The subscriber ID is 4471",
            "Unit 4 responding",
            "Copy that, en route",
            "you copy?",
        ],
    )
    def test_keeps_legitimate_traffic(self, text):
        assert is_hallucination(text) is False


class TestRms:
    def test_silence_is_zero(self):
        assert compute_rms(b"\x00\x00" * 2048) == 0.0

    def test_empty_input(self):
        assert compute_rms(b"") == 0.0

    def test_odd_trailing_byte_is_ignored(self):
        assert compute_rms(b"\x00") == 0.0

    def test_full_scale_square_wave(self):
        pcm = struct.pack("<4h", 16384, -16384, 16384, -16384)
        assert compute_rms(pcm) == pytest.approx(0.5, abs=1e-4)

    def test_wav_header_is_44_bytes(self):
        assert len(create_wav_header(1000)) == 44
