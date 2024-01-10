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

CMD ["python" , "server.py"]
