# RTL-FM Transcriber

Home Assistant add-on that captures FM radio via RTL-SDR, transcribes with Whisper, and publishes to MQTT.

## Installation

1. Copy this folder to your Home Assistant's `/addons/` directory
2. In Home Assistant, go to **Settings → Add-ons → Add-on Store**
3. Click the **⋮** menu (top right) → **Check for updates**
4. Find "RTL-FM Transcriber" in **Local add-ons** section
5. Click **Install**

## Configuration

| Option | Description | Default |
|--------|-------------|---------|
| `frequency` | FM frequency in MHz | 155.1075 |
| `squelch` | Noise threshold (0-100) | 50 |
| `chunk_duration` | Audio chunk length in seconds | 15 |
| `whisper_url` | Wyoming server URL (e.g. `tcp://host:10300`) | http://10.0.10.21:10300 |
| `mqtt_host` | MQTT broker hostname | core-mosquitto |
| `mqtt_port` | MQTT broker port | 1883 |
| `mqtt_topic` | Topic for transcriptions | radio/transcription |
| `mqtt_username` | MQTT username (optional) | |
| `mqtt_password` | MQTT password (optional) | |

## MQTT Output

Published to `radio/transcription`:
```json
{
  "text": "Unit 42 responding to Main Street",
  "frequency": "155.1075",
  "timestamp": "2026-01-25T18:15:00Z"
}
```

## Home Assistant Sensor

Add to `configuration.yaml`:
```yaml
mqtt:
  sensor:
    - name: "Radio Transcription"
      state_topic: "radio/transcription"
      value_template: "{{ value_json.text[:255] }}"
      json_attributes_topic: "radio/transcription"
```

## Example Automation

```yaml
automation:
  - alias: "Radio Keyword Alert"
    trigger:
      - platform: mqtt
        topic: radio/transcription
    condition:
      - condition: template
        value_template: "{{ 'fire' in trigger.payload_json.text|lower }}"
    action:
      - service: notify.mobile_app
        data:
          title: "🚒 Radio Alert"
          message: "{{ trigger.payload_json.text }}"
```
