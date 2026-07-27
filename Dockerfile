ARG BUILD_FROM
FROM $BUILD_FROM

# Install RTL-SDR, audio tools, Python, and timezone data
RUN apk add --no-cache \
    rtl-sdr \
    sox \
    python3 \
    py3-pip \
    py3-requests \
    tzdata \
    && pip3 install --no-cache-dir --break-system-packages paho-mqtt wyoming

# Copy application files
COPY transcriber.py /transcriber.py
COPY run.sh /run.sh

RUN chmod a+x /run.sh /transcriber.py

CMD [ "/run.sh" ]
