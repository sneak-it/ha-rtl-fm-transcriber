#!/bin/bash
set -e

echo "Starting RTL-FM Transcriber..."

# Timezone comes from the add-on options and is applied in Python.
exec python3 -m rtl_fm_transcriber
