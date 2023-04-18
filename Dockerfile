FROM        ubuntu:20.04
MAINTAINER  Kuśmirek Wiktor <kusmirekwiktor@gmail.com>

ARG TARGETARCH
ARG BIN_DIR=.

RUN apt-get update
RUN apt-get install -y python3 pip

COPY ${BIN_DIR}/fastq_exporter /fastq_exporter
COPY ${BIN_DIR}/python /python

EXPOSE 9308
ENTRYPOINT [ "/fastq_exporter" ]
