#!/bin/bash
set -e

echo "Starting RTL-FM Transcriber..."

# Set timezone from add-on configuration
if [ -f /data/options.json ]; then
    TZ_VALUE=$(python3 -c "import json; print(json.load(open('/data/options.json')).get('timezone', 'UTC'))" 2>/dev/null || echo "UTC")
    export TZ="$TZ_VALUE"
    echo "Timezone set to: $TZ"
else
    export TZ="${TZ:-UTC}"
    echo "Timezone set to: $TZ (from environment)"
fi

exec python3 /transcriber.py
