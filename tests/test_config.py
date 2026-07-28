"""Option loading, validation, and the config.yaml/code contract."""

import json
import pathlib

import pytest
import yaml

from rtl_fm_transcriber.config import (
    DEFAULT_WYOMING_PORT,
    ConfigError,
    load_config,
    parse_wyoming_url,
    validate_config,
)
from rtl_fm_transcriber.recording import MEDIA_DIR, WWW_DIR, audio_dir, audio_url

ADDON = pathlib.Path(__file__).resolve().parent.parent / "rtl-fm-transcriber"
MANIFEST = yaml.safe_load((ADDON / "config.yaml").read_text())


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("tcp://host:10300", ("host", 10300)),
        ("http://host:10300", ("host", 10300)),
        ("tcp://host", ("host", DEFAULT_WYOMING_PORT)),
        # A scheme-less value used to parse with hostname None and silently fall
        # back to localhost.
        ("10.0.0.1:10300", ("10.0.0.1", 10300)),
        ("myhost", ("myhost", DEFAULT_WYOMING_PORT)),
        ("  10.0.0.1:9000  ", ("10.0.0.1", 9000)),
    ],
)
def test_parse_wyoming_url(url, expected):
    assert parse_wyoming_url(url) == expected


@pytest.mark.parametrize("url", ["", "   ", None])
def test_parse_wyoming_url_rejects_empty(url):
    with pytest.raises(ConfigError):
        parse_wyoming_url(url)


def test_defaults_validate():
    validate_config(load_config())


def test_manifest_options_validate():
    validate_config(MANIFEST["options"])


def test_manifest_and_code_defaults_agree():
    """config.yaml and load_config must not drift apart."""
    assert set(load_config()) == set(MANIFEST["options"])


def test_every_option_has_a_schema_entry():
    assert set(MANIFEST["options"]) == set(MANIFEST["schema"])


def test_declared_arches_have_a_base_image():
    """Every arch in config.yaml needs a build.json entry, and vice versa.

    An arch declared without a base image fails at build time on the user's
    machine, not here.
    """
    build = json.loads((ADDON / "build.json").read_text())
    assert set(MANIFEST["arch"]) == set(build["build_from"])


def test_password_uses_the_password_schema_type():
    """Otherwise the broker password renders in clear text in the UI."""
    assert MANIFEST["schema"]["mqtt_password"] == "password?"


def test_ppm_allows_negative_correction():
    """Cheap dongles commonly need a negative PPM value, e.g. -12."""
    assert MANIFEST["schema"]["ppm"] == "int(-500,500)"


def test_host_network_is_not_requested():
    """The add-on only makes outbound connections."""
    assert "host_network" not in MANIFEST


def test_media_is_mapped_for_recordings():
    assert "media:rw" in MANIFEST["map"]


@pytest.mark.parametrize(
    ("bad", "field"),
    [
        ({"frequency": 5000.0}, "frequency"),
        ({"frequency": 1.0}, "frequency"),
        ({"frequency": "155.1075"}, "frequency"),
        ({"gain": "medium"}, "gain"),
        ({"bandpass_low": 4000, "bandpass_high": 3000}, "bandpass_low"),
        ({"min_transmission_duration": 200.0}, "min_transmission_duration"),
    ],
)
def test_validate_rejects(bad, field):
    config = load_config() | bad
    with pytest.raises(ConfigError, match=field):
        validate_config(config)


@pytest.mark.parametrize("gain", ["auto", "AUTO", "28.0", "28", 28, 28.0])
def test_validate_accepts_gain_forms(gain):
    validate_config(load_config() | {"gain": gain})


def test_recordings_default_to_the_authenticated_media_dir():
    """Home Assistant's www folder is served at /local/ with no authentication."""
    config = load_config()
    assert config["audio_public_www"] is False
    assert audio_dir(config) == MEDIA_DIR
    assert audio_dir(config).startswith("/media/")


def test_default_audio_url_matches_the_media_dir():
    """The advertised URI must resolve to where the file actually is.

    Regression: files were written to /config/www while audio_url advertised
    media-source://media_source/local/..., which resolves to /media, so any
    automation using audio_url got "media not found".
    """
    url = audio_url(load_config(), "x.wav")
    assert url == "media-source://media_source/local/radio-audio/x.wav"


def test_public_www_opt_in_changes_both_dir_and_url():
    config = load_config() | {"audio_public_www": True}
    assert audio_dir(config) == WWW_DIR
    assert audio_url(config, "x.wav") == "/local/radio-audio/x.wav"


def test_public_www_dir_matches_the_declared_config_mapping():
    """WWW_DIR must sit under the mount the manifest actually asks for.

    The deprecated "config" mapping mounted Home Assistant's config at /config;
    homeassistant_config mounts it at /homeassistant. Writing under the wrong
    root would silently produce files that /local/ does not serve.
    """
    assert "homeassistant_config:rw" in MANIFEST["map"]
    assert WWW_DIR.startswith("/homeassistant/www/")


def test_public_www_logs_a_warning(caplog):
    with caplog.at_level("WARNING"):
        validate_config(load_config() | {"audio_public_www": True})
    assert "no authentication" in caplog.text
