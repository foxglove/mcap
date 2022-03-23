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

# Enable C language support to fix CMake build
RUN sed -i -E "s/^project\('hdoc', 'cpp'/project('hdoc', 'cpp', 'c'/" meson.build

# Remove timestamp from build output
RUN sed -i -E 's/\+ " on " \+ cfg\.timestamp//' src/serde/HTMLWriter.cpp

# Remove enormous hdoc link from sidebar
RUN sed -i -E 's/aside\.AddChild\(.+a\.is-button is-size-1.+"https:\/\/hdoc\.io".+\);//' src/serde/HTMLWriter.cpp

RUN ~/.local/bin/meson build
RUN ninja -C build hdoc
