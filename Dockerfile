FROM python:3.8.10
USER root:root
WORKDIR /app
#COPY . /app
COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt