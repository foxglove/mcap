ARG IMAGE=ubuntu:jammy
FROM $IMAGE

ARG IMAGE=ubuntu:jammy

# https://askubuntu.com/questions/909277/avoiding-user-interaction-with-tzdata-when-installing-certbot-in-a-docker-contai
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
  apt-get install -y --no-install-recommends --no-install-suggests \
  ca-certificates \
  curl \
  gpg \
  wget \
  gnupg \
  make \
  perl \
  python3 \
  python3-pip \
  clang \
  clang-format \
  && rm -rf /var/lib/apt/lists/*

RUN if [ "$IMAGE" = "ubuntu:focal" ]; then \
    test -f /usr/share/doc/kitware-archive-keyring/copyright || \
    wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc 2>/dev/null | gpg --dearmor - | tee /usr/share/keyrings/kitware-archive-keyring.gpg >/dev/null && \
    echo "RELEASE: ${RELEASE}" && \
    echo 'deb [signed-by=/usr/share/keyrings/kitware-archive-keyring.gpg] https://apt.kitware.com/ubuntu/ focal main' | tee /etc/apt/sources.list.d/kitware.list >/dev/null && \
    apt update && \
    test -f /usr/share/doc/kitware-archive-keyring/copyright || rm /usr/share/keyrings/kitware-archive-keyring.gpg && \
    apt-get -y install kitware-archive-keyring cmake \
    ; fi
RUN if [ "$IMAGE" = "ubuntu:jammy" ]; then \
    test -f /usr/share/doc/kitware-archive-keyring/copyright || \
    wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc 2>/dev/null | gpg --dearmor - | tee /usr/share/keyrings/kitware-archive-keyring.gpg >/dev/null && \
    echo "RELEASE: ${RELEASE}" && \
    echo 'deb [signed-by=/usr/share/keyrings/kitware-archive-keyring.gpg] https://apt.kitware.com/ubuntu/ jammy main' | tee /etc/apt/sources.list.d/kitware.list >/dev/null && \
    apt update && \
    test -f /usr/share/doc/kitware-archive-keyring/copyright || rm /usr/share/keyrings/kitware-archive-keyring.gpg && \
    apt-get -y install kitware-archive-keyring cmake \
    ; fi

RUN if [ "$IMAGE" = "ubuntu:focal" ]; then \
  echo "deb http://apt.llvm.org/focal/ llvm-toolchain-focal-13 main" >> /etc/apt/sources.list && \
  curl https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add -  && \
  apt-get update && \
  apt-get install -y --no-install-recommends --no-install-suggests \
  clang-13 \
  clang-format-13 \
  && rm -rf /var/lib/apt/lists/* \
  ; fi

RUN if [ "$IMAGE" = "ubuntu:focal" ]; then \
  update-alternatives --install /usr/bin/clang clang /usr/bin/clang-13 100; \
  update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-13 100; \
  update-alternatives --install /usr/bin/clang-format clang-format /usr/bin/clang-format-13 100; \
  update-alternatives --install /usr/bin/git-clang-format git-clang-format /usr/bin/git-clang-format-13 100 \
  ; fi

RUN pip --no-cache-dir install conan~=1.0

WORKDIR /mcap/cpp

COPY --from=hdoc /hdoc/build/hdoc /usr/local/bin/hdoc

CMD [ "./build.sh" ]
