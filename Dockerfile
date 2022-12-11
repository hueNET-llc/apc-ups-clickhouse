FROM python:3.11-alpine3.17

COPY . /apc

WORKDIR /apc

RUN pip install -r requirements.txt && apk update && apk add --no-cache net-snmp-tools

ENTRYPOINT ["python", "-u", "apc.py"]