FROM ubuntu:18.04

LABEL maintainer="Jomit V"

COPY \opcua-MTServer /

RUN chmod +x opcua-MTServer

RUN apt-get update
RUN apt-get install sudo
RUN apt-get install libssl-dev sudo

EXPOSE 4840

CMD ["./opcua-MTServer", "https://smstestbed.nist.gov/vds/GFAgie01", "60"]