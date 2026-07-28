# RTL-FM Transcriber

Configuration reference and usage for the RTL-FM Transcriber add-on.

## Requirements

Home Assistant 2023.11 or newer, an RTL-SDR dongle, an MQTT broker (the Mosquitto
add-on is detected automatically), and a Wyoming speech-to-text service such as
the Whisper add-on. The version floor comes from the `homeassistant_config`
folder mapping, which replaced the deprecated `config` mapping in Supervisor
2023.11.

## Configuration

| Option | Description | Default |
|--------|-------------|---------|
| `frequency` | FM frequency in MHz (e.g. 155.1075) | 155.1075 |
| `squelch` | Carrier detection threshold (0-200, higher = more selective). This is the sensitivity knob for transmission detection. | 50 |
| `whisper_url` | Wyoming server URL (e.g. `tcp://host:10300`) | http://10.0.10.21:10300 |
| `mqtt_host` | MQTT broker hostname | core-mosquitto |
| `mqtt_port` | MQTT broker port | 1883 |
| `mqtt_topic` | Topic for transcriptions | radio/transcription |
| `mqtt_username` | MQTT username (optional) | |
| `mqtt_password` | MQTT password (optional) | |
| `gain` | RTL-SDR gain (number or "auto") | auto |
| `ppm` | PPM correction for RTL-SDR clock drift (0-500) | 0 |
| `bandpass_filter` | Enable voice bandpass filter (300-3000 Hz) | true |
| `bandpass_low` | Bandpass filter low cutoff in Hz | 300 |
| `bandpass_high` | Bandpass filter high cutoff in Hz | 3000 |
| `timezone` | Timezone for timestamps (IANA name) | UTC |
| `silence_timeout` | Seconds of silence that ends a transmission | 2.0 |
| `vad_warmup_ms` | Milliseconds to buffer before streaming (AGC stabilization) | 150 |
| `min_transmission_duration` | Minimum transmission length to transcribe (seconds) | 0.3 |
| `max_transmission_duration` | Maximum transmission length safety cap (seconds) | 120.0 |
| `audio_recording` | Enable audio recording for successful transcriptions | false |
| `audio_public_www` | Write recordings to the unauthenticated `/local/` path instead of `/media` (see warning below) | false |
| `mqtt_tls` | Connect to the broker over TLS | false |
| `mqtt_tls_ca` | Path to a CA certificate for `mqtt_tls` (blank = system trust store) | |
| `audio_retention_days` | Days to keep audio recordings | 7 |
| `audio_max_files` | Maximum number of audio files (0 = unlimited) | 0 |

### Audio Recording

When `audio_recording` is enabled, WAV audio files are saved for every successful transcription. Files go to `/media/radio-audio/`, which Home Assistant serves only through its authenticated endpoints, and which is what the `media-source://` URI in the MQTT payload resolves to.

**`audio_public_www` (default `false`):** setting this to `true` writes recordings to the `www/radio-audio/` folder inside your Home Assistant configuration directory instead (the same folder you would reach as `/config/www/radio-audio/` from the Home Assistant side; the add-on sees it at `/homeassistant/www/radio-audio/`). Home Assistant serves it at `/local/` with **no authentication**. Anyone who can reach your Home Assistant instance can then list and download recorded traffic. The only reason to enable it is that `/local/` URLs work in a plain HTML5 `<audio>` tag inside a markdown card; `/media` playback needs the media browser or a media player card.

**Audio files are named using the format:** `YYYYMMDD-HHMMSS-XXXX.XX.wav` (16kHz, 16-bit, mono PCM).

**Playback in Lovelace:** with the default (`/media`), use the `media_player.play_media` action or the Media browser with the `audio_url` value from the payload. With `audio_public_www: true`, `audio_url` is a `/local/...` path that an HTML5 `<audio>` element can play inline; see `home-assistant/lovelace-radio-transcriptions.yaml`.

**Retention policy:** Old recordings are automatically cleaned up based on `audio_retention_days` and `audio_max_files` settings.

### MQTT Output with Audio

When audio recording is enabled, the MQTT payload includes additional fields:
```json
{
  "text": "Unit 42 responding to Main Street",
  "frequency": "155.1075",
  "timestamp": "2026-01-25T18:15:00Z",
  "audio_file": "radio-audio/20260125-181500-155_1075.wav",
  "audio_url": "media-source://media_source/local/radio-audio/20260125-181500-155_1075.wav"
}
```

### rtl_fm Parameters Explained

- **squelch**: Controls the noise threshold. Higher values require stronger signals to open the squelch. For public safety monitoring, start at 50 and adjust based on your environment (range 0-200).
- **gain**: Use manual gain values for best results. Run `rtl_test` to find optimal gain for your dongle. Set to "auto" for automatic gain control.
- **ppm**: RTL-SDR dongles have slight clock drift. Run `rtl_test -p` to measure your dongle's PPM offset and enter it here for accurate frequency tuning.
- **bandpass_filter**: Removes low-frequency hum (below 300 Hz) and high-frequency static (above 3000 Hz) to improve transcription quality. This is recommended for emergency services monitoring.

### Voice Activity Detection (VAD) and Streaming Transcription

Detection is squelch-gated. `rtl_fm` runs with `-l <squelch> -E pad`, so while
the squelch is closed it emits zero-padded samples, and when a carrier opens it
emits real audio. A chunk counts as voice when it carries real audio rather than
padding, which means `squelch` is the sensitivity knob: raise it to ignore weak
signals and noise, lower it to catch weaker transmissions.

Audio is streamed to the Wyoming server as it arrives, so there are no hard cuts
mid-message.

**Transmission state machine:**
1. **IDLE** - squelch closed, monitoring
2. **WARMUP** - carrier opened, buffering `vad_warmup_ms` before the session opens
3. **STREAMING** - actively streaming audio to Wyoming
4. **WAITING_FOR_END** - audio stopped, silence timer running; returns to
   STREAMING if audio resumes before `silence_timeout`

A transmission ends when `silence_timeout` elapses with no audio, or when it hits
the `max_transmission_duration` cap. A carrier that stays open past the cap is
split into consecutive transmissions rather than being truncated.

**Recommended settings:**
```yaml
squelch: 50
silence_timeout: 2.0
vad_warmup_ms: 150
min_transmission_duration: 0.3
max_transmission_duration: 120.0
```

With these settings:
- Squelch 50 gates out background noise; tune with `rtl_fm -l` if transmissions
  are missed (too high) or noise is transcribed (too low)
- 150ms warmup buffer is captured before the Wyoming session opens, so the start
  of the transmission is not lost
- 2.0s of silence ends a transmission
- Minimum 0.3s transmission filters out clicks and spurs
- Maximum 120s safety cap prevents a stuck carrier from streaming forever

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
