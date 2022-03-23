FROM ubuntu:jammy

RUN apt-get update && apt-get install -y --no-install-recommends --no-install-suggests \
    git \
    python3 \
    python3-pip \
    cmake \
    clang \
    llvm-dev \
    libclang-dev \
    pkg-config \
    libssl-dev \
    ninja-build \
    xxd \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --user meson
RUN git clone https://github.com/hdoc/hdoc.git /hdoc --depth 1 --branch 1.2.2 --single-branch

WORKDIR /hdoc

COPY hdoc.patch /hdoc.patch
RUN git apply /hdoc.patch

RUN ~/.local/bin/meson build
RUN ninja -C build hdoc
