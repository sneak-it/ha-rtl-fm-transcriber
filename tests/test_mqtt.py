"""MQTT publishing, discovery payloads, and availability signalling."""

import json

import pytest

from rtl_fm_transcriber.mqtt import (
    AVAILABILITY_OFFLINE,
    AVAILABILITY_ONLINE,
    availability_topic,
    client_id,
    device_id,
    publish_audio_discovery,
    publish_availability,
    publish_discovery,
    safe_publish,
)

CONFIG = {
    "frequency": 155.1075,
    "mqtt_topic": "radio/transcription",
    "mqtt_host": "core-mosquitto",
    "mqtt_port": 1883,
}


class FakeResult:
    mid = 1
    rc = 0


class FakeClient:
    """Records publishes; connected state is controllable."""

    def __init__(self, connected=True, raises=None):
        self._connected = connected
        self._raises = raises
        self.published = []

    def is_connected(self):
        return self._connected

    def publish(self, topic, payload, qos=0, retain=False):
        if self._raises:
            raise self._raises
        self.published.append((topic, payload, qos, retain))
        return FakeResult()


def test_client_id_is_per_frequency():
    """Two instances must not evict each other in a reconnect storm.

    Regression: a fixed client_id meant a second instance on another frequency
    kicked the first off the broker, repeatedly.
    """
    a = client_id({"frequency": 155.1075})
    b = client_id({"frequency": 154.265})
    assert a != b
    assert a == "rtl-fm-transcriber-155_1075"


def test_safe_publish_skips_when_disconnected():
    client = FakeClient(connected=False)
    assert safe_publish(client, "t", "p") is False
    assert client.published == []


def test_safe_publish_publishes_when_connected():
    client = FakeClient()
    assert safe_publish(client, "t", "p", qos=1, retain=True) is True
    assert client.published == [("t", "p", 1, True)]


def test_safe_publish_swallows_broker_errors():
    client = FakeClient(raises=RuntimeError("broker gone"))
    assert safe_publish(client, "t", "p") is False


def test_discovery_declares_availability():
    """HA must be able to mark entities unavailable when the add-on dies."""
    client = FakeClient()
    publish_discovery(CONFIG, client)
    _, payload, _, retain = client.published[0]
    body = json.loads(payload)

    assert retain is True
    assert body["availability_topic"] == availability_topic(CONFIG)
    assert body["payload_available"] == AVAILABILITY_ONLINE
    assert body["payload_not_available"] == AVAILABILITY_OFFLINE
    assert body["unique_id"] == f"{device_id(CONFIG)}_transcription"


def test_audio_discovery_unique_id_has_no_doubled_suffix():
    """Regression: unique_id came out as rtl_fm_<freq>_audio_audio."""
    client = FakeClient()
    publish_audio_discovery(CONFIG, client)
    payloads = [p for _, p, _, _ in client.published if p]
    body = json.loads(payloads[-1])
    assert body["unique_id"] == f"{device_id(CONFIG)}_audio"
    assert "_audio_audio" not in body["unique_id"]


def test_audio_discovery_clears_the_stale_entity_first():
    client = FakeClient()
    publish_audio_discovery(CONFIG, client)
    first_payload = client.published[0][1]
    assert first_payload == "", "stale retained discovery was not cleared"


def test_both_sensors_share_one_device():
    client = FakeClient()
    publish_discovery(CONFIG, client)
    publish_audio_discovery(CONFIG, client)
    bodies = [json.loads(p) for _, p, _, _ in client.published if p]
    identifiers = {tuple(b["device"]["identifiers"]) for b in bodies}
    assert len(identifiers) == 1


@pytest.mark.parametrize("state", [AVAILABILITY_ONLINE, AVAILABILITY_OFFLINE])
def test_publish_availability_is_retained(state):
    client = FakeClient()
    publish_availability(client, CONFIG, state)
    topic, payload, qos, retain = client.published[0]
    assert topic == availability_topic(CONFIG)
    assert payload == state
    assert retain is True
    assert qos == 1
