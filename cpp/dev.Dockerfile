FROM ubuntu:focal AS base

# https://askubuntu.com/questions/909277/avoiding-user-interaction-with-tzdata-when-installing-certbot-in-a-docker-contai
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
  apt-get install -y --no-install-recommends --no-install-suggests \
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

FROM base as build
RUN pip --no-cache-dir install conan

ENV CONAN_V2_MODE=1
RUN conan config init

FROM build as build_executables
COPY ./bench /src/bench/
COPY ./examples /src/examples/
COPY ./mcap /src/mcap/
COPY ./test /src/test/
COPY ./.clang-format /src/
RUN conan editable add ./mcap mcap/0.0.1
RUN conan install bench --install-folder bench/build/Release \
  -s compiler.cppstd=17 -s build_type=Release --build missing
RUN conan install examples --install-folder examples/build/Release \
  -s compiler.cppstd=17 -s build_type=Release --build missing
RUN conan install test --install-folder test/build/Debug \
  -s compiler.cppstd=17 -s build_type=Debug --build missing

FROM build_executables AS bag2mcap
COPY --from=build_executables /src /src
COPY --from=build_executables /src/examples/build/ /src/examples/build/
RUN conan build examples --build-folder examples/build/Release
ENTRYPOINT ["examples/build/Release/bin/bag2mcap"]

FROM build_executables AS bench
COPY --from=build_executables /src /src
COPY --from=build_executables /src/bench/build/ /src/bench/build/
RUN conan build bench --build-folder bench/build/Release
ENTRYPOINT ["bench/build/Release/bin/bench-tests"]

FROM build_executables AS test
COPY --from=build_executables /src /src
COPY --from=build_executables /src/test/build/ /src/test/build/
RUN conan build test --build-folder test/build/Debug
ENTRYPOINT ["test/build/Debug/bin/unit-tests"]
