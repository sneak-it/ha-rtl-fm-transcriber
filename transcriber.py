#!/usr/bin/env python3
"""
RTL-FM Transcriber for Home Assistant
Captures FM audio via RTL-SDR, transcribes with Wyoming (faster-whisper), publishes to MQTT.
"""

import asyncio
import json
import logging
import os
import subprocess
import tempfile
import time
from urllib.parse import urlparse
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from wyoming.audio import AudioChunk, AudioStart, AudioStop
from wyoming.client import AsyncTcpClient
from wyoming.asr import Transcribe, Transcript

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_config():
    """Load configuration from Home Assistant add-on options."""
    options_path = '/data/options.json'
    
    if os.path.exists(options_path):
        with open(options_path) as f:
            return json.load(f)
    
    # Defaults
    return {
        'frequency': 155.1075,
        'squelch': 50,
        'chunk_duration': 15,
        'whisper_url': 'http://youriphere:10300',
        'mqtt_host': 'core-mosquitto',
        'mqtt_port': 1883,
        'mqtt_topic': 'radio/transcription',
        'mqtt_username': '',
        'mqtt_password': ''
    }


def create_mqtt_client(config):
    """Create and connect MQTT client."""
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id="rtl-fm-transcriber"
    )
    
    if config.get('mqtt_username'):
        client.username_pw_set(
            config['mqtt_username'],
            config.get('mqtt_password', '')
        )
    
    try:
        client.connect(config['mqtt_host'], config['mqtt_port'], 60)
        client.loop_start()
        logger.info(f"Connected to MQTT broker at {config['mqtt_host']}:{config['mqtt_port']}")
        return client
    except Exception as e:
        logger.error(f"Failed to connect to MQTT broker: {e}")
        raise


async def transcribe_wyoming(audio_path, whisper_url):
    """Send audio file to Wyoming server for transcription."""
    # Parse host/port from URL
    try:
        parsed = urlparse(whisper_url)
        host = parsed.hostname or 'localhost'
        port = parsed.port or 10300
    except Exception as e:
        logger.error(f"Failed to parse Wyoming URL {whisper_url}: {e}")
        return None

    logger.info(f"Connecting to Wyoming server at {host}:{port}")
    
    try:
        async with AsyncTcpClient(host, port) as client:
            # Read audio file
            with open(audio_path, 'rb') as f:
                audio_data = f.read()

            # 1. Send AudioStart (for 16kHz, 16-bit mono, usually expected by Whisper)
            # RTL-SDR -> sox is configured for 24000Hz in previous code, 
            # but Whisper usually expects 16000Hz. We should update sox to 16000Hz.
            await client.write_event(AudioStart(rate=16000, width=2, channels=1).event())

            # 2. Send AudioChunk(s)
            chunk_size = 1024
            for i in range(0, len(audio_data), chunk_size):
                chunk = audio_data[i:i+chunk_size]
                await client.write_event(AudioChunk(rate=16000, width=2, channels=1, audio=chunk).event())

            # 3. Send AudioStop
            await client.write_event(AudioStop().event())
            
            # 4. Request Transcription? 
            # Actually, standard Wyoming pipeline usually implies transcription if we sent audio
            # OR we might need to send a Describe or Transcribe event first.
            # Usually purely sending audio + AudioStop triggers processing in simpler servers.
            # But let's send a Transcribe event just in case it's supported/needed.
            await client.write_event(Transcribe().event())

            # 5. Wait for Transcript
            logger.info("Waiting for transcript...")
            while True:
                event = await client.read_event()
                if event is None:
                    logger.warning("Connection closed by server")
                    break
                
                if Transcript.is_type(event.type):
                    transcript = Transcript.from_event(event)
                    return transcript.text

    except ConnectionRefusedError:
        logger.error(f"Connection refused to Wyoming server at {host}:{port}")
    except Exception as e:
        logger.error(f"Wyoming transcription error: {e}")
        
    return None


def check_audio_has_voice(wav_path, threshold=0.02):
    """
    Simple voice activity detection using audio amplitude.
    Returns True if audio likely contains voice.
    """
    try:
        # Use sox to get audio statistics
        result = subprocess.run(
            ['sox', wav_path, '-n', 'stat'],
            capture_output=True,
            text=True
        )
        
        # Parse RMS amplitude from sox stat output
        for line in result.stderr.split('\n'):
            if 'RMS' in line and 'amplitude' in line:
                parts = line.split()
                if len(parts) >= 3:
                    rms = float(parts[-1])
                    logger.info(f"Audio RMS: {rms} (Threshold: {threshold})")
                    return rms > threshold
        
        # If we can't parse, assume silence to be safe
        logger.warning("Could not parse VAD stats")
        return False
        
    except Exception as e:
        logger.warning(f"VAD check failed: {e}")
        return False


def is_hallucination(text):
    """Check for common Whisper hallucinations on silence."""
    hallucinations = [
        "thank you for watching",
        "thanks for watching",
        "subs by",
        "subscribe",
        "amara.org",
        "copyright"
    ]
    text_lower = text.lower().strip()
    
    # Empty or very short
    if len(text_lower) < 2:
        return True
        
    for h in hallucinations:
        if h in text_lower:
            return True
            
    return False


async def capture_loop(config, mqtt_client):
    """Async capture loop using persistent pipeline."""
    frequency_hz = int(config['frequency'] * 1_000_000)
    sample_rate = 16000 
    # Use max_duration as a hard limit for a single transcription
    max_duration = config['chunk_duration']
    # If silence (no data) lasts this long, consider transmission done
    silence_timeout = 2.0 
    
    logger.info(f"Starting persistent capture on {config['frequency']} MHz")
    
    # 1. Build Pipeline
    # rtl_fm (squelched) -> sox (format conv) -> stdout
    
    squelch_val = str(config.get('squelch', 50))
    capture_rate = 48000
    
    rtl_cmd = [
        'rtl_fm',
        '-f', str(frequency_hz),
        '-M', 'fm',
        '-s', str(capture_rate),
        '-l', squelch_val,
        '-E', 'pad', # Output silence when squelched (prevents pipe starvation)
    ]
    
    if config.get('gain') and config['gain'] != 'auto':
        rtl_cmd.extend(['-g', str(config['gain'])])
    
    rtl_cmd.append('-')
    
    sox_cmd = [
        'sox',
        '-t', 'raw', '-r', str(capture_rate), '-e', 'signed', '-b', '16', '-c', '1', '-',
        '-r', str(sample_rate),
        '-e', 'signed', '-b', '16',
        '-t', 'raw',
        '-' # Output raw audio to stdout
    ]
    
    # Start processes
    try:
        # Capture stderr for debugging
        rtl_proc = subprocess.Popen(rtl_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        sox_proc = subprocess.Popen(sox_cmd, stdin=rtl_proc.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        rtl_proc.stdout.close() # Allow rtl_proc to receive SIGPIPE
    except Exception as e:
        logger.error(f"Failed to start subprocesses: {e}")
        return

    logger.info("Pipeline started. Waiting for audio...")

    import select
    
    bytes_per_sec = sample_rate * 2 # 16-bit
    buffer = bytearray()
    last_data_time = time.time()
    
    try:
        while True:
            # Monitor both stdout (data) AND stderr (errors)
            # rtl_fm outputs logs to stderr. on crash we want to see them.
            readable, _, _ = select.select([sox_proc.stdout, rtl_proc.stderr], [], [], 0.1)
            
            # Check for RTL errors first
            if rtl_proc.stderr in readable:
                err_line = rtl_proc.stderr.readline()
                if err_line:
                    logger.info(f"RTL_FM Log: {err_line.decode().strip()}")
            
            if sox_proc.stdout in readable:
                # Data available! Read it.
                chunk = sox_proc.stdout.read(4096)
                if not chunk:
                    # EOF - unexpected!
                    logger.error("Audio pipeline died (EOF)")
                    
                    # Check return codes
                    rtl_ret = rtl_proc.poll()
                    sox_ret = sox_proc.poll()
                    logger.error(f"Process status - RTL: {rtl_ret}, SOX: {sox_ret}")
                    
                    # Dump remaining stderr
                    if rtl_ret is not None and rtl_proc.stderr:
                         logger.error(f"RTL_FM Error: {rtl_proc.stderr.read().decode()}")
                    if sox_ret is not None and sox_proc.stderr:
                         logger.error(f"SOX Error: {sox_proc.stderr.read().decode()}")
                         
                    break
                    
                buffer.extend(chunk)
                last_data_time = time.time()
                
                # If buffer gets too big (max duration), force process it
                if len(buffer) > max_duration * bytes_per_sec:
                    logger.info("Max duration reached, forcing transcription")
                    await process_buffer(buffer, config, mqtt_client)
                    buffer = bytearray()
                    
            else:
                # No data (Silence from squelch)
                # If we have data in buffer and it's been silent for a while, process it
                time_since_last = time.time() - last_data_time
                if len(buffer) > 0 and time_since_last > silence_timeout:
                    # Transmission ended
                    logger.info(f"Silence detected ({silence_timeout}s), processing transmission")
                    await process_buffer(buffer, config, mqtt_client)
                    buffer = bytearray()
                    
            # Check if processes still alive
            if sox_proc.poll() is not None:
                logger.error("Sox process exited unexpectedly")
                break

            # Sleep tiny bit to yield event loop
            await asyncio.sleep(0.01)

    except Exception as e:
        logger.error(f"Capture loop error: {e}")
    finally:
        # Cleanup
        try: rtl_proc.kill() 
        except: pass
        try: sox_proc.kill() 
        except: pass


async def process_buffer(audio_data, config, mqtt_client):
    """Save buffer to WAV and transcribe."""
    if len(audio_data) < 16000: # Ignore tiny blips (<0.5s)
        return

    wav_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix='.wav', delete=False) as wav_file:
            wav_path = wav_file.name
            
        # Write raw buf to WAV using sox (simplest way to add header)
        # Or proper wavfile write. Let's use sox again to wrap it.
        # Actually, python wave lib is easier/faster.
        import wave
        with wave.open(wav_path, 'wb') as wf:
            wf.setnchannels(1)
            wf.setsampwidth(2)
            wf.setframerate(16000)
            wf.writeframes(audio_data)
            
        # Debug Audio: Save raw capture BEFORE checks
        if config.get('debug_audio'):
            try:
                # Ensure directory exists once (e.g. in main, but here is safe too)
                debug_dir = '/config/www'
                if os.path.exists(debug_dir):
                    import shutil
                    # Save raw capture (what the loop heard)
                    shutil.copy(wav_path, f"{debug_dir}/rtl_last_capture.wav")
                    logger.info(f"Debug: Saved raw capture to {debug_dir}/rtl_last_capture.wav")
            except Exception as e:
                logger.error(f"Failed to save debug audio: {e}")

        # VAD Check
        if not check_audio_has_voice(wav_path, config.get('vad_threshold', 0.05)):
            os.unlink(wav_path)
            return

        # Debug Audio: Save passed audio (what is sending to Whisper)
        if config.get('debug_audio'):
            try:
                shutil.copy(wav_path, f"/config/www/rtl_last_transcription.wav")
            except: pass

        # Transcribe
        text = await transcribe_wyoming(wav_path, config['whisper_url'])
        
        if text:
            clean_text = text.strip()
            if is_hallucination(clean_text):
                logger.info(f"Filtered hallucination: '{clean_text}'")
            else:
                logger.info(f"Transcription: {clean_text}")
                message = {
                    'text': clean_text,
                    'frequency': str(config['frequency']),
                    'timestamp': datetime.now(timezone.utc).isoformat()
                }
                mqtt_client.publish(config['mqtt_topic'], json.dumps(message), qos=1)
        
        os.unlink(wav_path)
        
    except Exception as e:
        logger.error(f"Processing error: {e}")
        if wav_path and os.path.exists(wav_path):
            try: os.unlink(wav_path)
            except: pass


def publish_discovery(config, mqtt_client):
    """Publish Home Assistant MQTT Auto Discovery payload."""
    # Unique ID based on frequency to allow multiple instances
    unique_id = f"rtl_fm_{str(config['frequency']).replace('.', '_')}"
    device_name = f"RTL-FM Scanner {config['frequency']}MHz"
    
    discovery_topic = f"homeassistant/sensor/{unique_id}/transcription/config"
    
    payload = {
        "name": "Radio Transcription",
        "unique_id": f"{unique_id}_transcription",
        "state_topic": config['mqtt_topic'],
        "value_template": "{{ value_json.text[:255] }}",
        "json_attributes_topic": config['mqtt_topic'],
        "icon": "mdi:radio-handheld",
        "device": {
            "identifiers": [unique_id],
            "name": device_name,
            "model": "RTL-SDR",
            "manufacturer": "RTL-FM Transcriber"
        }
    }
    
    mqtt_client.publish(discovery_topic, json.dumps(payload), retain=True)
    logger.info(f"Published Discovery to {discovery_topic}")


def main():
    logger.info("RTL-FM Transcriber starting (Wyoming Protocol)...")
    config = load_config()
    
    # Check RTL-SDR
    try:
        subprocess.run(['rtl_test', '-t'], capture_output=True, timeout=10)
        logger.info("RTL-SDR device check passed")
    except Exception as e:
        logger.error(f"RTL-SDR check failed: {e}")
        # Continue anyway, let rtl_fm fail if must
        
    mqtt_client = create_mqtt_client(config)
    
    # Publish HA Discovery
    publish_discovery(config, mqtt_client)
    
    try:
        asyncio.run(capture_loop(config, mqtt_client))
    except KeyboardInterrupt:
        pass
    finally:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()


if __name__ == '__main__':
    main()
