FROM alpine:3.18

COPY . /apc

WORKDIR /apc

RUN apk update && \
    apk add --no-cache python3 py3-pip net-snmp-tools tzdata && \
    pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python", "-u", "apc.py"]