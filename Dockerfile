FROM python:3.10-slim-bullseye

ENV TZ="Asia/Jerusalem"

ENV PYTHONPATH=/app:/home/omgili/python

RUN DEBIAN_FRONTEND=noninteractive && apt-get -y install tzdata 

RUN echo "Setting time zone to '${TZ}'" \
  && echo "${TZ}" > /etc/timezone \
  && dpkg-reconfigure --frontend noninteractive tzdata

RUN apt-get update && apt-get install -y curl

WORKDIR /app

COPY . .

RUN pip install -r requirements.txt

# Create a directory for NLTK data
RUN mkdir -p /usr/local/share/nltk_data

# Set environment variable
ENV NLTK_DATA=/usr/local/share/nltk_data

# Add the following line to download 'punkt'
RUN python -m nltk.downloader punkt

# Add the following line to download 'stopwords'
RUN python -m nltk.downloader stopwords

RUN chown 1000:1000 -R /usr/local/share/nltk_data
