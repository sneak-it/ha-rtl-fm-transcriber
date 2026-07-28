# RTL-FM Transcriber

Home Assistant add-on that captures FM radio via RTL-SDR, transcribes with Whisper, and publishes to MQTT.

Optimized for emergency services (public safety) radio monitoring on VHF (150-174 MHz) and UHF (421-512 MHz) bands using narrowband FM (12.5 kHz channels).

Requires Home Assistant 2023.11 or newer, on amd64 or aarch64.

## Installation

1. In Home Assistant, go to **Settings → Add-ons → Add-on Store**
2. Click the **⋮** menu (top right) → **Repositories**
3. Add `https://github.com/sneak-it/ha-rtl-fm-transcriber` and close the dialog
4. Find "RTL-FM Transcriber" in the store and click **Install**

Installing downloads a prebuilt image, so it takes seconds rather than minutes.

## Configuration

Every option is documented in [DOCS.md](rtl-fm-transcriber/DOCS.md), which is
also what the add-on's Documentation tab shows in Home Assistant.

## Repository layout

`repository.yaml` at the root marks this as an add-on repository; everything the
add-on ships lives in [rtl-fm-transcriber/](rtl-fm-transcriber/), and the tests,
lint config and workflows at the root are development-only.

The operational code is `rtl-fm-transcriber/rtl_fm_transcriber/`, one concern per
module; `transcriber.py` is a thin entry-point shim. Segmentation rules are a
pure function in `transmission.py`, so they are tested against synthetic chunk
sequences without an RTL-SDR dongle or a Wyoming server.

## Development

```bash
pip install -r rtl-fm-transcriber/requirements.txt -r requirements-dev.txt
ruff check .
pytest -q
```

## Releasing

The Supervisor pulls the image tag matching `version` in
`rtl-fm-transcriber/config.yaml`, so a version bump is only usable once its
images exist:

1. Bump `version` in `rtl-fm-transcriber/config.yaml` and update the changelog
2. Merge to `main`
3. Publish a GitHub release whose tag is that same version

The release workflow refuses to run if the tag and the manifest version differ,
since publishing a mismatch would leave users unable to install or update.
