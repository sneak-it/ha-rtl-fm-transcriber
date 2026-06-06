# Changelog

## 1.4.0

- Added IANA timezone support (`timezone` env var) for localized timestamps in transcriptions and recordings
- Added streaming segmentation options for finer-grained transmission control:
  - `silence_timeout`: Time to wait for silence before ending transmission (default: 2.0s)
  - `vad_warmup_ms`: VAD warmup period in milliseconds (default: 150ms)
  - `min_transmission_duration`: Minimum duration to consider a transmission valid (default: 0.3s)
  - `max_transmission_duration`: Maximum transmission duration limit (default: 120.0s)
  - `vad_recovery_seconds`: Recovery time after voice activity ends (default: 1.0s)
- Added Wyoming protocol connection settings for tuning stability:
  - `wyoming_connection_timeout`: Connection timeout in seconds (default: 10.0s)
  - `wyoming_read_timeout`: Read timeout in seconds (default: 30.0s)
  - `wyoming_reconnect_max_attempts`: Maximum reconnection attempts (default: 3)
  - `wyoming_reconnect_delay`: Base reconnection delay in seconds (default: 1.0s)
- Added optional local audio recording feature:
  - `audio_recording`: Enable/disable audio recording (default: false)
  - `audio_retention_days`: Number of days to retain recordings (default: 7)
  - `audio_max_files`: Maximum number of recordings to keep, 0 for unlimited (default: 0)

## 1.3.0

- **Critical fix**: Changed rtl_fm sample rate from 48000 Hz to 12000 Hz for narrowband FM (public safety radio)
- Added `-E dc` to rtl_fm command for DC offset removal (reduces low-frequency hum)
- Added PPM correction support (`ppm` option) for RTL-SDR dongle clock drift calibration
- Added optional voice bandpass filter (300-3000 Hz default) to improve transcription quality
  - `bandpass_filter`: Enable/disable the filter (default: true)
  - `bandpass_low`: Low cutoff frequency in Hz (default: 300)
  - `bandpass_high`: High cutoff frequency in Hz (default: 3000)
- Fixed squelch schema range from int(0,1000) to int(0,200) (correct rtl_fm range)
- Improved VAD RMS parsing reliability using regex for cross-version sox compatibility
- Added `ppm`, `bandpass_filter`, `bandpass_low`, `bandpass_high` to default configuration

## 1.2.2

- Added `debug_audio` option. If enabled, saves `rtl_last_capture.wav` to `/config/www/` for debugging signal quality.

## 1.2.1

- Added Home Assistant **Auto-Discovery**: Sensor will appear automatically in HA (no YAML needed!)
- Inspired by `rtl_433` project best practices

## 1.2.0

- Added `-E pad` to `rtl_fm`: forces output of silence (zeros) when squelched
- This fixes pipeline crashes/timeouts by ensuring a constant data stream
- Combined with persistent pipeline, this provides stable, low-CPU operation

## 1.1.9

- Added detailed debug logging for `rtl_fm` crashes (captures stderr output)
- Fixed potential EOF handling in pipeline

## 1.1.8

- Major rewrite of capture logic to use Persistent Subprocess Pipeline
- **Fixed High CPU**: processes now cycle only once, not every 15s
- **Fixed Squelch handling**: Uses proper buffering to detect end-of-transmission rather than crude timeouts
- Improved `squelch` support (try ~100-200)

## 1.1.7

- Fixed critical bug: `NameError: capture_rate` (regression in 1.1.6)

## 1.1.6

- Architect change: Reverted to `rtl_fm` hardware squelch for low CPU usage
- Implemented "Partial Chunk" handling: if squelch is active and chunk times out, we accept whatever partial audio was captured (silence/blips) rather than crashing
- `squelch` option is now the standard rtl_fm squelch level again (try ~50-150)
- `vad_threshold` still applies to verify the captured partial audio contains voice

## 1.1.5

- Major Squelch Change: Switched from `rtl_fm` squelch (which pauses stream/causes timeouts) to `sox` noise gate
- The `squelch` option now sets the dB threshold for the software noise gate (0-1000 scale maps to -50dB to 0dB)
- This should fix timeouts and high RMS noise floor issues
- Try `squelch: 200` to start (approx -40dB)

## 1.1.4

- Increased audio capture timeout buffer (from 5s to 10s) to prevent errors with longer chunk durations

## 1.1.3

- Added `gain` configuration option (default: `auto`)
- Increased max squelch limit in config schema to 1000
- Lowered default `vad_threshold` to 0.01 (IMPORTANT: You must tune `squelch` until noise RMS is near 0)

## 1.1.2

- Added `vad_threshold` configuration option (default: 0.2)
- Expose VAD tuning to handle varying noise floors

## 1.1.1

- Enabled INFO logging for Audio RMS levels to help tune VAD threshold

## 1.1.0

- Cleaned up default configuration (removed unused HTTP path from `whisper_url`)
- Documentation updates to reflect Wyoming protocol usage

## 1.0.9

- Improved Voice Activity Detection (VAD) sensitivity (RMS threshold 0.01 -> 0.02)
- Added filter for common Whisper hallucinations ("Thank you for watching", etc.)
- Added debug logging for audio levels

## 1.0.8

- Fixed `AudioChunk` argument error (wyoming protocol)

## 1.0.7

- User requested change: Switched from HTTP API to Wyoming protocol (TCP)
- Updated `transcriber.py` to use `wyoming` python library
- Changed audio sampling to 16kHz (standard for Whisper/Wyoming)
- `whisper_url` setting is now parsed for host/port (e.g. `http://10.0.0.1:10300` -> `10.0.0.1:10300`)

## 1.0.6

- Added detailed Whisper API logging for debugging connection issues

## 1.0.5

- Fixed MQTT deprecation warning by updating to callback API version 2

## 1.0.4

- Fixed config.yaml - replaced invalid `privileged`/`full_access` options with `usb: true` and `udev: true`
- This should fix the add-on not appearing in the store

## 1.0.3

- Added `privileged: true` and `full_access: true` for RTL-SDR USB access (invalid options)
- Updated base images to Alpine 3.21

## 1.0.2

- Disabled s6-overlay init system (`init: false`) to fix startup error
- Simplified Dockerfile and run script

## 1.0.1

- Fixed s6-overlay compatibility for Home Assistant base images
- Run script now placed in `/etc/services.d/` for proper process supervision
- Uses `with-contenv bashio` for HA environment integration

## 1.0.0

- Initial release
- FM radio capture via RTL-SDR
- Whisper API transcription
- MQTT publishing
- Voice activity detection
