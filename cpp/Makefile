.PHONY: default
default: build

.PHONY: dev-image
dev-image:
	docker build -t mcap_cpp_dev -f dev.Dockerfile .

.PHONY: dev-shell
dev-shell: dev-image
	docker run --rm -v $(CURDIR):/src -it mcap_cpp_dev /bin/bash

.PHONY: build
build: dev-image
	docker run -t --rm -v $(CURDIR):/src mcap_cpp_dev

.PHONY: build-host
build-host:
	./build.sh

.PHONY: clean
clean:
	docker rm -f hdoc
	docker rmi -f mcap_cpp_dev
	docker rmi -f mcap_cpp_ci
	docker rmi -f mcap_cpp_ci_jammy
	rm -rf bench/build
	rm -rf examples/build
	rm -rf test/build

.PHONY: format-check
format-check: dev-image
	docker run -t --rm -v $(CURDIR):/src mcap_cpp_dev python3 scripts/format.py /src

.PHONY: format-fix
format-fix: dev-image
	docker run -t --rm -v $(CURDIR):/src mcap_cpp_dev python3 scripts/format.py --fix /src

.PHONY: bench
bench: dev-image
	docker run -t --rm -v $(CURDIR):/src mcap_cpp_dev ./bench/build/Release/bench-tests

.PHONY: bench-host
bench-host:
	./bench/build/Release/bench-tests

.PHONY: test
test: dev-image
	docker run -t --rm -v $(CURDIR):/src mcap_cpp_dev make test-host

.PHONY: test-host
test-host:
	./test/build/Debug/unit-tests

.PHONY: hdoc-build
hdoc-build:
	docker build -t hdoc -f hdoc.Dockerfile .

.PHONY: run-examples
run-examples: dev-image
	docker run -t --rm -v $(CURDIR):/src mcap_cpp_ci ./run_examples.sh

.PHONY: run-examples-host
run-examples-host:
	./run_examples.sh

.PHONY: test-examples
test-examples: dev-image
	docker run -t --rm -v $(CURDIR):/src mcap_cpp_ci examples/build/protobuf/Release/example_protobuf_unit_tests

.PHONY: test-examples-host
test-examples-host:
	./examples/build/protobuf/Release/example_protobuf_unit_tests

.PHONY: ci-image
ci-image: hdoc-build
	docker build -t mcap_cpp_ci -f ci.Dockerfile --build-arg IMAGE=ubuntu:focal .

.PHONY: ci-clang
ci-clang: ci-image
	docker run -t --rm -v $(CURDIR):/mcap/cpp -v $(HOME)/.conan/data:/root/.conan/data -e CC=clang-13 -e CXX=clang++-13 mcap_cpp_ci ./build.sh

.PHONY: ci-gcc
ci-gcc: ci-image
	docker run -t --rm -v $(CURDIR):/mcap/cpp -v $(HOME)/.conan/data:/root/.conan/data -e CC=gcc -e CXX=g++ mcap_cpp_ci ./build.sh

.PHONY: ci-docs
ci-docs: ci-image
	docker build -t mcap_cpp_ci_jammy -f ci.Dockerfile .
	docker run -t --rm -v $(CURDIR):/mcap/cpp -v $(CURDIR)/../__docs__/cpp:/hdoc-output -v $(HOME)/.conan/data:/root/.conan/data mcap_cpp_ci_jammy ./build-docs.sh

.PHONY: ci-format-check
ci-format-check: ci-image
	docker run -t --rm -v $(CURDIR):/mcap/cpp -v $(HOME)/.conan/data:/root/.conan/data mcap_cpp_ci python3 scripts/format.py .
