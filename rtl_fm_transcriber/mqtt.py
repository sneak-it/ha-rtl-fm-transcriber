"""MQTT client lifecycle, publishing, and HA discovery payloads."""

import json
import logging

import paho.mqtt.client as mqtt

logger = logging.getLogger(__name__)


def create_mqtt_client(config):
    """Create and connect MQTT client."""
    logger.info("[MQTT] Initializing MQTT client...")
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id="rtl-fm-transcriber",
    )

    if config.get("mqtt_username"):
        client.username_pw_set(config["mqtt_username"], config.get("mqtt_password", ""))
        logger.info(f"[MQTT] Username configured for broker at {config['mqtt_host']}")
    else:
        logger.info("[MQTT] No username configured (anonymous connection)")

    logger.info(
        f"[MQTT] Connecting to broker at {config['mqtt_host']}:{config['mqtt_port']} (timeout: 60s)..."
    )

    try:
        client.connect(config["mqtt_host"], config["mqtt_port"], 60)
        logger.info("[MQTT] Connect packet sent, starting network loop...")
        client.loop_start()
        logger.info("[MQTT] Network loop started (background thread)")

        # Verify connection state after starting the loop
        import time as _time
        _time.sleep(0.5)  # Give network thread time to establish connection
        if client.is_connected():
            logger.info(
                f"[MQTT] Successfully connected to MQTT broker at {config['mqtt_host']}:{config['mqtt_port']}"
            )
        else:
            logger.warning(
                "[MQTT] loop_start() returned but client.is_connected() is False — "
                "connection may not be established yet"
            )
        return client
    except Exception as e:
        logger.error(f"[MQTT] Failed to connect to MQTT broker at {config['mqtt_host']}:{config['mqtt_port']}: {e}")
        raise


def is_mqtt_connected(client):
    """Check if MQTT client is still connected and reconnect if needed.
    
    Args:
        client: MQTT client instance
        
    Returns:
        True if connected (or reconnected), False if reconnection failed
    """
    try:
        if client.is_connected():
            return True
        
        logger.warning("[MQTT] Client reports disconnected — attempting reconnection...")
        reconnect_delay = 1.0
        max_retries = 3
        for attempt in range(1, max_retries + 1):
            logger.info(
                f"[MQTT] Reconnection attempt {attempt}/{max_retries}..."
            )
            try:
                client.reconnect()
                if client.is_connected():
                    logger.info(f"[MQTT] Reconnected successfully on attempt {attempt}")
                    return True
                else:
                    logger.warning(f"[MQTT] Reconnect returned but not connected (attempt {attempt})")
            except Exception as reconnect_err:
                logger.error(
                    f"[MQTT] Reconnection attempt {attempt} failed: {reconnect_err}"
                )
            
            if attempt < max_retries:
                delay = reconnect_delay * attempt
                logger.info(f"[MQTT] Waiting {delay:.1f}s before next reconnection attempt...")
                import time as _time
                _time.sleep(delay)
        
        logger.error("[MQTT] All reconnection attempts failed — MQTT publishing will be skipped")
        return False
    except Exception as e:
        logger.error(f"[MQTT] Error checking connection state: {e}")
        return False


def safe_publish(client, topic, payload, qos=0, retain=False, label="message") -> bool:
    """Publish after verifying connectivity. Logs and swallows failures."""
    if not is_mqtt_connected(client):
        logger.error(f"[MQTT] Cannot publish {label}: client is disconnected")
        return False
    try:
        result = client.publish(topic, payload, qos=qos, retain=retain)
    except Exception as e:
        logger.error(
            f"[MQTT] {label} publish failed: type={type(e).__name__}, error={e}"
        )
        return False
    logger.info(
        f"[MQTT] Published {label} to topic='{topic}' "
        f"(mid={result.mid}, rc={result.rc})"
    )
    return True


def publish_discovery(config, mqtt_client):
    """Publish Home Assistant MQTT Auto Discovery payload."""
    # Unique ID based on frequency to allow multiple instances
    unique_id = f"rtl_fm_{str(config['frequency']).replace('.', '_')}"
    device_name = f"RTL-FM Scanner {config['frequency']}MHz"

    discovery_topic = f"homeassistant/sensor/{unique_id}/transcription/config"
    discovery_payload = json.dumps({
        "name": "Radio Transcription",
        "unique_id": f"{unique_id}_transcription",
        "state_topic": config["mqtt_topic"],
        "value_template": "{{ value_json.text[:255] }}",
        "json_attributes_topic": config["mqtt_topic"],
        "icon": "mdi:radio-handheld",
        "device": {
            "identifiers": [unique_id],
            "name": device_name,
            "model": "RTL-SDR",
            "manufacturer": "RTL-FM Transcriber",
        },
    })

    safe_publish(
        mqtt_client, discovery_topic, discovery_payload,
        retain=True, label="transcription discovery",
    )


def publish_audio_discovery(config, mqtt_client):
    """Publish Home Assistant MQTT Auto Discovery payload for audio sensor."""
    unique_id = f"rtl_fm_{str(config['frequency']).replace('.', '_')}_audio"
    device_name = f"RTL-FM Scanner {config['frequency']}MHz"
    
    discovery_topic = f"homeassistant/sensor/{unique_id}/config"
    discovery_payload = json.dumps({
        "name": "Radio Audio Recording",
        "unique_id": f"{unique_id}_audio",
        "state_topic": config["mqtt_topic"],
        "value_template": "{{ value_json.timestamp if value_json.audio_file else '' }}",
        "json_attributes_topic": config["mqtt_topic"],
        "icon": "mdi:record-rec",
        "device": {
            "identifiers": [f"rtl_fm_{str(config['frequency']).replace('.', '_')}"],
            "name": device_name,
            "model": "RTL-SDR",
            "manufacturer": "RTL-FM Transcriber",
        },
    })
    
    safe_publish(
        mqtt_client, discovery_topic, discovery_payload,
        retain=True, label="audio discovery",
    )
