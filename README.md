# RTL-FM Transcriber

Home Assistant add-on that captures FM radio via RTL-SDR, transcribes with Whisper, and publishes to MQTT.

Optimized for emergency services (public safety) radio monitoring on VHF (150-174 MHz) and UHF (421-512 MHz) bands using narrowband FM (12.5 kHz channels).

## Installation

1. Copy this folder to your Home Assistant's `/addons/` directory
2. In Home Assistant, go to **Settings → Add-ons → Add-on Store**
3. Click the **⋮** menu (top right) → **Check for updates**
4. Find "RTL-FM Transcriber" in **Local add-ons** section
5. Click **Install**

## Configuration

| Option | Description | Default |
|--------|-------------|---------|
| `frequency` | FM frequency in MHz (e.g. 155.1075) | 155.1075 |
| `squelch` | Noise threshold (0-200, higher = more selective) | 50 |
| `chunk_duration` | Maximum transmission duration safety cap (seconds) | 15 |
| `whisper_url` | Wyoming server URL (e.g. `tcp://host:10300`) | http://10.0.10.21:10300 |
| `mqtt_host` | MQTT broker hostname | core-mosquitto |
| `mqtt_port` | MQTT broker port | 1883 |
| `mqtt_topic` | Topic for transcriptions | radio/transcription |
| `mqtt_username` | MQTT username (optional) | |
| `mqtt_password` | MQTT password (optional) | |
| `vad_threshold` | RMS amplitude threshold for voice detection | 0.03 |
| `vad_baseline_window` | Seconds of idle audio to track for baseline | 30 |
| `gain` | RTL-SDR gain (number or "auto") | auto |
| `ppm` | PPM correction for RTL-SDR clock drift (0-500) | 0 |
| `bandpass_filter` | Enable voice bandpass filter (300-3000 Hz) | true |
| `bandpass_low` | Bandpass filter low cutoff in Hz | 300 |
| `bandpass_high` | Bandpass filter high cutoff in Hz | 3000 |
| `debug_audio` | Save debug audio to /config/www/ | false |
| `timezone` | Timezone for timestamps (IANA name) | America/New_York |
| `silence_timeout` | Seconds of silence before transcription is sent | 2.0 |
| `vad_warmup_ms` | Milliseconds to buffer before streaming (AGC stabilization) | 150 |
| `min_transmission_duration` | Minimum transmission length to transcribe (seconds) | 0.3 |
| `max_transmission_duration` | Maximum transmission length safety cap (seconds) | 120.0 |
| `vad_recovery_seconds` | Silence duration before considering transmission ended (seconds) | 1.0 |

### rtl_fm Parameters Explained

- **squelch**: Controls the noise threshold. Higher values require stronger signals to open the squelch. For public safety monitoring, start at 50 and adjust based on your environment (range 0-200).
- **gain**: Use manual gain values for best results. Run `rtl_test` to find optimal gain for your dongle. Set to "auto" for automatic gain control.
- **ppm**: RTL-SDR dongles have slight clock drift. Run `rtl_test -p` to measure your dongle's PPM offset and enter it here for accurate frequency tuning.
- **bandpass_filter**: Removes low-frequency hum (below 300 Hz) and high-frequency static (above 3000 Hz) to improve transcription quality. This is recommended for emergency services monitoring.

### Voice Activity Detection (VAD) and Streaming Transcription

The add-on uses a baseline tracking algorithm with **streaming transcription** for real-time radio message segmentation. In RTL-SDR setups with automatic gain control (AGC), idle noise often has a **higher** RMS amplitude than active transmissions. When a strong signal arrives, AGC reduces gain, causing the RMS to drop.

The system automatically tracks a baseline of idle noise RMS values and detects transmissions when the signal drops significantly below that baseline. Audio is streamed directly to the Wyoming server as it arrives - no more 15-second hard cuts mid-message.

**Transmission state machine:**
1. **IDLE** - Monitoring for voice activity
2. **WARMUP** - Voice detected, buffering 150ms (AGC stabilization)
3. **STREAMING** - Actively streaming audio to Wyoming
4. **SILENCE_DETECTED** - Voice stopped, starting silence timer
5. **WAITING_FOR_END** - Waiting for silence timeout to confirm end of transmission
6. **TRANSCRIBING** - Sent AudioStop, waiting for final transcript

**Recommended settings:**
```yaml
vad_threshold: 0.03
vad_baseline_window: 30
silence_timeout: 2.0
vad_recovery_seconds: 1.0
min_transmission_duration: 0.3
max_transmission_duration: 120.0
```

With these settings:
- The system collects ~30 seconds of idle noise RMS values for baseline
- Voice is detected when RMS drops below `baseline - 0.03`
- 150ms warmup buffer allows AGC to stabilize before streaming
- 1.0s silence recovery timeout detects end of transmission
- 2.0s final silence timeout confirms transmission end
- Minimum 0.3s transmission filters out clicks/spurs
- Maximum 120s safety cap prevents runaway transmissions

### Wyoming Server Connection

The add-on connects to the Wyoming server (faster-whisper) for real-time streaming transcription. Connection management includes:

- **Connection timeout** — Connection attempts timeout after `wyoming_connection_timeout` seconds (default: 10s)
- **Reconnection with exponential backoff** — If Wyoming is unavailable, reconnection attempts use 1s, 2s, 4s delays (max 3 attempts)
- **Error classification** — Distinguishes between "connection refused" (server down), "connection timeout" (network issue), and "connection reset" (mid-stream disconnect)
- **Graceful degradation** — If Wyoming is unavailable, the RTL-SDR pipeline continues running; transmissions are skipped until the server recovers

**Recommended settings:**
```yaml
wyoming_connection_timeout: 10.0
wyoming_read_timeout: 30.0
wyoming_reconnect_max_attempts: 3
wyoming_reconnect_delay: 1.0
```

| Option | Default | Description |
|--------|---------|-------------|
| `wyoming_connection_timeout` | 10.0 | Seconds to wait for Wyoming connection |
| `wyoming_read_timeout` | 30.0 | Seconds to wait for transcript after AudioStop |
| `wyoming_reconnect_max_attempts` | 3 | Max reconnection attempts per transmission |
| `wyoming_reconnect_delay` | 1.0 | Initial delay between reconnection attempts (seconds) |

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
