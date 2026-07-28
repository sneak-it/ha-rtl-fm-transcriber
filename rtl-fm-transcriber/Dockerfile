# check=skip=InvalidDefaultArgInFrom

# BUILD_FROM is required and deliberately has no default: the Supervisor passes
# the base image for the machine's architecture, from build.json. A default would
# have to name one architecture and would silently build the wrong base if the
# arg were ever missing, so failing loudly is better. The directive above must
# stay on the first line to take effect.
ARG BUILD_FROM
FROM $BUILD_FROM

# Install RTL-SDR, audio tools, Python, and timezone data
RUN apk add --no-cache \
    rtl-sdr \
    sox \
    python3 \
    py3-pip \
    tzdata

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip3 install --no-cache-dir --break-system-packages -r /app/requirements.txt

COPY rtl_fm_transcriber/ /app/rtl_fm_transcriber/
COPY run.sh /run.sh

RUN chmod a+x /run.sh

CMD [ "/run.sh" ]
