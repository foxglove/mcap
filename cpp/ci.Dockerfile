FROM ubuntu:jammy

RUN apt-get update && \
  apt-get install -y --no-install-recommends --no-install-suggests \
  cmake \
  make \
  python3 \
  python3-pip \
  clang \
  clang-format \
  && rm -rf /var/lib/apt/lists/*

RUN pip --no-cache-dir install conan

WORKDIR /mcap/cpp

ENV CONAN_V2_MODE=1

COPY --from=hdoc /hdoc/build/hdoc /usr/local/bin/hdoc

CMD [ "./build.sh" ]
