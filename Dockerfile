# pull official base image
FROM python:3.11.1-slim

# set work directory
WORKDIR /usr/src/app

# copy project
COPY . /usr/src/app/

# install dependencies
RUN apt-get update && apt-get install -y libpq-dev\
    && pip install --upgrade pip setuptools wheel \
    && pip install -r /usr/src/app/requirements.txt \
    && python setup.py install \
    && rm -rf /root/.cache/pip

