FROM ubuntu:resolute

# https://askubuntu.com/questions/909277/avoiding-user-interaction-with-tzdata-when-installing-certbot-in-a-docker-contai
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
  apt-get install -y --no-install-recommends --no-install-suggests \
  build-essential \
  ca-certificates \
  clang \
  clang-format \
  curl \
  cmake \
  gnupg \
  make \
  perl \
  python3 \
  python3-pip

ENV CC=clang
ENV CXX=clang++

WORKDIR /src

RUN pip --no-cache-dir install --break-system-packages conan~=2.25.1

CMD [ "./build.sh" ]
