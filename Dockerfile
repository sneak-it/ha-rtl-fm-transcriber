ARG BUILD_FROM
FROM $BUILD_FROM

# Install RTL-SDR, audio tools, Python, and timezone data
RUN apk add --no-cache \
    rtl-sdr \
    sox \
    python3 \
    py3-pip \
    tzdata \
    && pip3 install --no-cache-dir --break-system-packages paho-mqtt wyoming

WORKDIR /app

# Copy application files
COPY rtl_fm_transcriber/ /app/rtl_fm_transcriber/
COPY run.sh /run.sh

RUN chmod a+x /run.sh

CMD [ "/run.sh" ]
