FROM python:3.11-slim-buster

COPY . /apc

WORKDIR /apc

RUN pip install -r requirements.txt

ENTRYPOINT ["python", "-u", "apc.py"]