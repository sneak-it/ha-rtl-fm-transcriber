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

Every option is documented in [DOCS.md](DOCS.md), which is also what the
add-on's Documentation tab shows in Home Assistant.

## Development

```bash
pip install -r requirements.txt -r requirements-dev.txt
ruff check .
pytest -q
```

The operational code lives in `rtl_fm_transcriber/`, one concern per module;
`transcriber.py` is a thin entry-point shim. Segmentation rules are a pure
function in `rtl_fm_transcriber/transmission.py`, so they are tested against
synthetic chunk sequences without an RTL-SDR dongle or a Wyoming server.
