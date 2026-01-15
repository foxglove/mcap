FROM ubuntu:focal

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
  python3-pip


RUN echo "deb http://apt.llvm.org/focal/ llvm-toolchain-focal-13 main" >> /etc/apt/sources.list && \
  curl https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add -  &&\
  apt-get update && \
  apt-get install -y --no-install-recommends --no-install-suggests \
  clang-13 \
  clang-format-13

RUN update-alternatives --install /usr/bin/clang-format clang-format /usr/bin/clang-format-13 100
RUN update-alternatives --install /usr/bin/git-clang-format git-clang-format /usr/bin/git-clang-format-13 100

ENV CC=clang-13
ENV CXX=clang++-13

WORKDIR /src

RUN pip --no-cache-dir install conan

CMD [ "./build.sh" ]
