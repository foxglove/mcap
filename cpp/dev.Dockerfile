FROM ubuntu:jammy

# https://askubuntu.com/questions/909277/avoiding-user-interaction-with-tzdata-when-installing-certbot-in-a-docker-contai
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
  apt-get install -y --no-install-recommends --no-install-suggests \
  build-essential \
  ca-certificates \
  curl \
  cmake \
  gnupg \
  make \
  perl \
  python3 \
  python3-pip \
  clang \
  clang-format

ENV CC=clang
ENV CXX=clang++

WORKDIR /src

RUN pip --no-cache-dir install "conan>=2.0,<3"

CMD [ "./build.sh" ]
