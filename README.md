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
| `chunk_duration` | Audio chunk length in seconds | 15 |
| `whisper_url` | Wyoming server URL (e.g. `tcp://host:10300`) | http://10.0.10.21:10300 |
| `mqtt_host` | MQTT broker hostname | core-mosquitto |
| `mqtt_port` | MQTT broker port | 1883 |
| `mqtt_topic` | Topic for transcriptions | radio/transcription |
| `mqtt_username` | MQTT username (optional) | |
| `mqtt_password` | MQTT password (optional) | |
| `vad_mode` | Voice detection mode: `standard` (RMS > threshold) or `rtl_sdr` (RMS drops below baseline for RTL-SDR with AGC) | standard |
| `vad_threshold` | RMS amplitude threshold for voice detection | 0.01 |
| `vad_baseline_window` | Seconds of idle audio to track for baseline (rtl_sdr mode only) | 30 |
| `gain` | RTL-SDR gain (number or "auto") | auto |
| `ppm` | PPM correction for RTL-SDR clock drift (0-500) | 0 |
| `bandpass_filter` | Enable voice bandpass filter (300-3000 Hz) | true |
| `bandpass_low` | Bandpass filter low cutoff in Hz | 300 |
| `bandpass_high` | Bandpass filter high cutoff in Hz | 3000 |
| `debug_audio` | Save debug audio to /config/www/ | false |

### rtl_fm Parameters Explained

- **squelch**: Controls the noise threshold. Higher values require stronger signals to open the squelch. For public safety monitoring, start at 50 and adjust based on your environment (range 0-200).
- **gain**: Use manual gain values for best results. Run `rtl_test` to find optimal gain for your dongle. Set to "auto" for automatic gain control.
- **ppm**: RTL-SDR dongles have slight clock drift. Run `rtl_test -p` to measure your dongle's PPM offset and enter it here for accurate frequency tuning.
- **bandpass_filter**: Removes low-frequency hum (below 300 Hz) and high-frequency static (above 3000 Hz) to improve transcription quality. This is recommended for emergency services monitoring.

### Voice Activity Detection (VAD) Modes

The add-on supports two VAD modes to handle different audio characteristics:

- **standard**: Detects voice when RMS amplitude exceeds the threshold. This is the traditional approach for normal audio recording (microphone input).
- **rtl_sdr**: Detects voice when RMS amplitude **drops below** the baseline minus threshold. This handles the inverted RMS behavior common in RTL-SDR setups with automatic gain control (AGC), where idle noise has higher RMS than active transmissions.

If you notice that your idle noise has a higher RMS value than actual transmissions (voice causes RMS to drop), use `rtl_sdr` mode. The mode automatically tracks a baseline of idle noise and detects transmissions when the signal drops significantly below that baseline.

**Recommended settings for RTL-SDR with AGC:**
```yaml
vad_mode: "rtl_sdr"
vad_threshold: 0.03
vad_baseline_window: 30
```

With these settings, the system collects ~30 seconds of idle noise RMS values, calculates the median baseline, and detects voice when RMS drops below `baseline - 0.03`.

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
