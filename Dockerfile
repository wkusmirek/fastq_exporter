FROM        ubuntu:20.04
MAINTAINER  Kuśmirek Wiktor <kusmirekwiktor@gmail.com>

ARG TARGETARCH
ARG BIN_DIR=.

RUN apt-get update
RUN apt-get install -y python3 pip
RUN pip install minknow_api

COPY ${BIN_DIR}/minknow_exporter /minknow_exporter
COPY ${BIN_DIR}/python /python

EXPOSE     9309
ENTRYPOINT [ "/minknow_exporter" ]
