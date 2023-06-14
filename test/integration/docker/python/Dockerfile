FROM python:3.9

WORKDIR /opt

RUN python -m pip install --upgrade pip
RUN python -m pip install pytest requests pandas atlassian-python-api bs4 toml

ADD scripts/* ./
ADD configuration/* ./
