FROM python:3.7.9

ENV DEV_MODE 0

RUN mkdir /code
WORKDIR /code

RUN pip install pip -U

ADD requirements.txt /code/

RUN pip install -r requirements.txt

ADD . /code/
