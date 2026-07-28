# Changelog

## 1.5.0

Production-readiness release. Behaviour changes, so read the breaking notes
before updating.

### Breaking

- **Voice detection is now squelch-gated.** `rtl_fm` already ran with
  `-l <squelch> -E pad`, which emits zero-padded samples while the squelch is
  closed, but detection treated "quieter than the baseline" as voice, making
  digital silence the most voice-like input possible. Depending on what the air
  sounded like at startup, that either streamed endless silence to Whisper or
  never detected anything at all. A chunk now counts as voice when rtl_fm is
  passing real audio, and **`squelch` is the sensitivity knob**: raise it to
  ignore noise, lower it to catch weaker transmissions.
- **Removed options** `vad_threshold`, `vad_baseline_window` and
  `vad_recovery_seconds`. The first two belonged to the old baseline detector.
  `vad_recovery_seconds` and `silence_timeout` were assigned to each other's
  attributes and only one was ever read; `silence_timeout` is now the single
  silence setting.
- **Removed options** `chunk_duration` and `debug_audio`, which nothing read.
- **Recordings moved to `/media/radio-audio`.** `/config/www` is served at
  `/local/` with no authentication, so recorded public-safety traffic was
  downloadable by anyone who could reach Home Assistant. `/media` is served only
  through authenticated endpoints and is what the `media-source://` URI in the
  MQTT payload always claimed. Set `audio_public_www: true` to keep the old
  location, needed only for inline `<audio>` playback in a markdown card.
- **The audio sensor's entity is recreated.** Its `unique_id` was
  `rtl_fm_<freq>_audio_audio`; the doubled suffix is fixed. The old retained
  discovery payload is cleared on startup so Home Assistant drops the stale
  entity rather than leaving an orphan.
- **`host_network` removed.** The add-on only makes outbound connections.
- **Home Assistant 2023.11 or newer is now required** (declared via the
  `homeassistant` key). The deprecated `config` folder mapping is replaced with
  `homeassistant_config`, which the Supervisor mounts at `/homeassistant`. Only
  `audio_public_www` uses it; the `/local/` URLs it produces are unchanged, and
  existing recordings stay where they are, since it is the same host folder.
- The `mqtt:need` service is declared, so the Supervisor starts this add-on
  after Mosquitto and can supply broker credentials.

### Fixed, transmission segmentation

- Silence never ended a transmission on the normal data path: the
  `WAITING_FOR_END` state was checked but never assigned, so the only working
  end path was a read timeout that `-E pad` makes rare. Every transmission ran
  to the 120s cap, arriving minutes late with separate transmissions merged.
- Voice chunks arriving during warmup were discarded, and warmup only advanced
  on non-voice chunks, so a strong continuous transmission stalled and lost
  audio.
- The warmup timeout started streaming without connecting to the Wyoming server,
  after which every chunk was dropped and the whole transmission vanished with
  "No transcript".
- The chunk completing warmup was sent and recorded twice, so saved WAVs
  stuttered at every transmission start.
- `max_transmission_duration` was only checked when no voice was present, so a
  stuck-open carrier streamed forever. It is now checked on every chunk, and a
  carrier that outlives it splits into consecutive transmissions instead of
  being truncated.

### Fixed, connections and shutdown

- TCP connections to the Wyoming server leaked on every transmission and every
  retry, eventually exhausting sockets on a busy channel.
- The Wyoming connect timeout was accepted and logged but never applied, so an
  unreachable host blocked for the OS TCP timeout while audio piled up.
- Waiting for a transcript blocked audio capture for up to 30s and skewed the
  timers for the following transmission. Transcription now runs in the
  background, and the configurable `wyoming_read_timeout` is finally used.
- Discarding a too-short transmission left the server holding orphaned audio.
- Pipeline teardown waited for an EOF that `-E pad` never produces, so every
  restart and every add-on stop burned a 10s timeout; a second drain could hang
  the add-on outright instead of restarting it.
- Two stderr reader tasks leaked on every pipeline restart.
- Restart backoff was unreachable: an unplugged or claimed dongle looped at 1s
  forever. Backoff now only resets after a pipeline has actually run.
- Audio was written to `sox` without backpressure, so a stall grew memory
  without bound.
- MQTT reconnection raced paho's own network thread and tore down connections
  that had just succeeded, and it blocked the event loop for up to 3s.
  Connectivity is now tracked through callbacks and reconnection left to paho.
- A briefly-unavailable broker at startup killed the add-on; startup now retries
  and then continues.
- SIGTERM was ignored, because Python runs as PID 1. Every stop or update waited
  out Docker's kill timeout and then SIGKILLed, leaving the USB device in a bad
  state. Stops are now clean.
- A fixed MQTT `client_id` meant two instances on different frequencies
  disconnected each other repeatedly.

### Fixed, other

- Recording filenames mangled negative UTC offsets, so every US-timezone user
  got names like `20260727T1215000400-155_1075.wav`. They now match the
  documented `YYYYMMDD-HHMMSS-<freq>.wav`.
- The hallucination filter discarded legitimate traffic: "subscriber", routine
  trunked-radio jargon, matched the "subscribe" phrase. Matching is now on word
  boundaries.
- Retention cleanup logged spurious "Failed to remove" warnings for files it had
  already deleted.
- Timestamps used a fixed UTC offset, which was wrong on either side of a DST
  transition; they now use `zoneinfo`.
- Frequency tuning truncated instead of rounding, tuning some frequencies 1 Hz
  low.

### Added

- Optional `mqtt_tls` with an optional CA path.
- A retained availability topic plus a last will, so Home Assistant marks
  entities unavailable instead of showing stale transcripts forever.
- Startup validation of the Wyoming URL, frequency range, gain, bandpass
  ordering and duration bounds, failing with a message naming the option. A
  scheme-less `host:port` is now accepted; previously it silently fell back to
  localhost.
- An AppArmor profile, pinned dependencies and pinned base images.
- `DOCS.md`, `translations/en.yaml` for the options UI, and CI running lint,
  tests and an add-on build for all three architectures.

### Changed

- The single 1870-line `transcriber.py` is now the `rtl_fm_transcriber` package,
  twelve modules with one concern each. `transcriber.py` remains as an
  entry-point shim. Segmentation is a pure function covered by 119 tests.
- Roughly 300 lines of never-called code removed.
- `ppm` accepts negative values, which cheap dongles commonly need.
- The broker password no longer renders in clear text in the configuration UI.
- Base images updated to Alpine 3.24, which moves the runtime from Python 3.12
  to 3.14. CI tests on the same version.

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
