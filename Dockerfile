FROM python:3.6.3-stretch
MAINTAINER Enrique Cornejo enrique.cornejo@vizzuality.comm

ENV NAME nexgddp-data-prep
ENV USER nexgddp
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get dist-upgrade -y && apt-get install -y \
    libgdal-dev \
    gdal-bin \
    python-gdal \
    && apt-get clean

ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

RUN pip install --upgrade pip
RUN pip install virtualenv numpy

RUN mkdir -p /opt/$NAME
RUN mkdir -p /opt/$NAME/data && chmod 755 -R /opt/$NAME/data
RUN mkdir -p /opt/$NAME/downloads && chmod 755 -R /opt/$NAME/downloads

COPY entrypoint.sh /opt/$NAME/entrypoint.sh
RUN chmod +x /opt/$NAME/entrypoint.sh

WORKDIR /opt/$NAME

RUN cd /opt/$NAME && virtualenv venv && /bin/bash -c "source venv/bin/activate"
COPY requirements.txt /opt/$NAME/requirements.txt
RUN pip install -r requirements.txt

RUN groupadd -r $USER && useradd --no-log-init -r -g $USER $USER
RUN mkdir /home/$USER && mkdir /home/$USER/.aws

COPY credentials /home/$USER/.aws/credentials

RUN chown -R $USER /home/$USER/.aws && chmod 600 /home/$USER/.aws/credentials

RUN chown -R $USER /opt/$NAME && chmod 755 -R /opt/$NAME
RUN chown -R $USER /opt/$NAME/data && chmod 755 -R /opt/$NAME/data

USER $USER

COPY src/process_step_one.py process_step_one.py
COPY src/process_step_two.py process_step_two.py