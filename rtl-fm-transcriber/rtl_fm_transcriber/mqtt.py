"""MQTT client lifecycle, publishing, and HA discovery payloads."""

import json
import logging
import threading
import time

import paho.mqtt.client as mqtt

logger = logging.getLogger(__name__)

# Startup connect retry
CONNECT_ATTEMPTS = 10
CONNECT_BACKOFF_MAX = 30.0
CONNECT_WAIT = 5.0

AVAILABILITY_ONLINE = "online"
AVAILABILITY_OFFLINE = "offline"


def availability_topic(config) -> str:
    """Topic carrying online/offline, referenced by the discovery payloads."""
    return f"{config['mqtt_topic']}/availability"


def client_id(config) -> str:
    """Per-frequency client id, so two instances do not evict each other."""
    return f"rtl-fm-transcriber-{str(config['frequency']).replace('.', '_')}"


def create_mqtt_client(config):
    """Connect to the broker, retrying with backoff, and return the client.

    Connectivity is tracked through the on_connect/on_disconnect callbacks
    rather than polled, and paho's own reconnect thread does the reconnecting;
    calling reconnect() by hand raced it and tore down connections that had just
    succeeded.
    """
    logger.info("[MQTT] Initializing MQTT client...")
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=client_id(config),
    )
    connected = threading.Event()
    client.rtl_connected = connected

    def on_connect(_client, _userdata, _flags, reason_code, _properties=None):
        if reason_code == 0:
            logger.info(
                f"[MQTT] Connected to {config['mqtt_host']}:{config['mqtt_port']}"
            )
            connected.set()
            _client.publish(
                availability_topic(config), AVAILABILITY_ONLINE, qos=1, retain=True
            )
        else:
            connected.clear()
            logger.error(f"[MQTT] Connection refused by broker: {reason_code}")

    def on_disconnect(_client, _userdata, _flags, reason_code, _properties=None):
        connected.clear()
        if reason_code:
            logger.warning(
                f"[MQTT] Disconnected ({reason_code}); paho will reconnect"
            )

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect

    if config.get("mqtt_tls", False):
        ca = config.get("mqtt_tls_ca") or None
        client.tls_set(ca_certs=ca)
        logger.info(
            f"[MQTT] TLS enabled (ca_certs={ca or 'system trust store'})"
        )

    if config.get("mqtt_username"):
        client.username_pw_set(config["mqtt_username"], config.get("mqtt_password", ""))
        logger.info(f"[MQTT] Username configured for broker at {config['mqtt_host']}")
    else:
        logger.info("[MQTT] No username configured (anonymous connection)")

    # Last will, so Home Assistant sees the add-on go away instead of showing
    # stale transcripts forever.
    client.will_set(
        availability_topic(config), AVAILABILITY_OFFLINE, qos=1, retain=True
    )
    client.reconnect_delay_set(min_delay=1, max_delay=int(CONNECT_BACKOFF_MAX))

    host, port = config["mqtt_host"], config["mqtt_port"]
    client.loop_start()

    for attempt in range(1, CONNECT_ATTEMPTS + 1):
        logger.info(
            f"[MQTT] Connecting to {host}:{port} (attempt {attempt}/{CONNECT_ATTEMPTS})"
        )
        try:
            client.connect(host, port, keepalive=60)
        except OSError as e:
            logger.error(f"[MQTT] Connect to {host}:{port} failed: {e}")
        else:
            if connected.wait(timeout=CONNECT_WAIT):
                return client
            logger.warning(f"[MQTT] No CONNACK from {host}:{port} within {CONNECT_WAIT}s")

        if attempt < CONNECT_ATTEMPTS:
            delay = min(2.0 ** (attempt - 1), CONNECT_BACKOFF_MAX)
            logger.info(f"[MQTT] Retrying in {delay:.0f}s")
            time.sleep(delay)

    # Returning the client rather than raising: a broker that is briefly
    # unavailable at boot should not kill the add-on, and paho keeps retrying.
    logger.error(
        f"[MQTT] Could not reach {host}:{port} after {CONNECT_ATTEMPTS} attempts. "
        f"Continuing; publishes will be skipped until the broker returns."
    )
    return client


def is_mqtt_connected(client) -> bool:
    """Whether the broker connection is currently up.

    No manual reconnect here: paho's network thread owns that, and polling
    is_connected() straight after a reconnect() call raced it.
    """
    return bool(client.is_connected())


def safe_publish(client, topic, payload, qos=0, retain=False, label="message") -> bool:
    """Publish after verifying connectivity. Logs and swallows failures."""
    if not is_mqtt_connected(client):
        logger.error(f"[MQTT] Cannot publish {label}: client is disconnected")
        return False
    try:
        result = client.publish(topic, payload, qos=qos, retain=retain)
    except Exception as e:  # noqa: BLE001 - publishing must never raise into callers
        logger.error(
            f"[MQTT] {label} publish failed: type={type(e).__name__}, error={e}"
        )
        return False
    logger.info(
        f"[MQTT] Published {label} to topic='{topic}' "
        f"(mid={result.mid}, rc={result.rc})"
    )
    return True


def publish_availability(client, config, state: str) -> None:
    """Publish the availability state, used on clean shutdown."""
    safe_publish(
        client, availability_topic(config), state,
        qos=1, retain=True, label=f"availability={state}",
    )


def device_id(config) -> str:
    """Stable per-frequency device identifier."""
    return f"rtl_fm_{str(config['frequency']).replace('.', '_')}"


def _publish_sensor_discovery(
    config, mqtt_client, *, topic, key, name, value_template, icon, label
):
    """Publish one HA discovery payload for this device."""
    dev_id = device_id(config)
    payload = json.dumps({
        "name": name,
        "unique_id": f"{dev_id}_{key}",
        "state_topic": config["mqtt_topic"],
        "value_template": value_template,
        "json_attributes_topic": config["mqtt_topic"],
        "availability_topic": availability_topic(config),
        "payload_available": AVAILABILITY_ONLINE,
        "payload_not_available": AVAILABILITY_OFFLINE,
        "icon": icon,
        "device": {
            "identifiers": [dev_id],
            "name": f"RTL-FM Scanner {config['frequency']}MHz",
            "model": "RTL-SDR",
            "manufacturer": "RTL-FM Transcriber",
        },
    })
    safe_publish(mqtt_client, topic, payload, retain=True, label=label)


def publish_discovery(config, mqtt_client):
    """Publish HA discovery for the transcription sensor."""
    _publish_sensor_discovery(
        config, mqtt_client,
        topic=f"homeassistant/sensor/{device_id(config)}/transcription/config",
        key="transcription",
        name="Radio Transcription",
        value_template="{{ value_json.text[:255] }}",
        icon="mdi:radio-handheld",
        label="transcription discovery",
    )


def publish_audio_discovery(config, mqtt_client):
    """Publish HA discovery for the audio recording sensor."""
    topic = f"homeassistant/sensor/{device_id(config)}_audio/config"

    # Earlier versions set unique_id to "<device>_audio_audio". Clearing the
    # retained payload first makes HA drop that entity instead of leaving an
    # orphan alongside the corrected one.
    safe_publish(mqtt_client, topic, "", retain=True, label="stale audio discovery")

    _publish_sensor_discovery(
        config, mqtt_client,
        topic=topic,
        key="audio",
        name="Radio Audio Recording",
        value_template="{{ value_json.timestamp if value_json.audio_file else '' }}",
        icon="mdi:record-rec",
        label="audio discovery",
    )
