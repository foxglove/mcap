ARG IMAGE=ubuntu:jammy
FROM $IMAGE

ARG IMAGE=ubuntu:jammy

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
  clang-format \
  && rm -rf /var/lib/apt/lists/*

RUN pip --no-cache-dir install "conan>=2.0,<3"

WORKDIR /mcap/cpp

COPY --from=hdoc /hdoc/build/hdoc /usr/local/bin/hdoc

CMD [ "./build.sh" ]
