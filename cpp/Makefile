default: build

.PHONY: dev-image
dev-image:
	docker build -t mcap_cpp_dev -f dev.Dockerfile .

.PHONY: build
build: dev-image
	docker run -t --rm --name mcap_cpp_dev -v $(CURDIR):/src mcap_cpp

.PHONY: build-host
build-host:
	./build.sh

clean:
	docker rm -f mcap_cpp
	docker rmi -f mcap_cpp_dev
	docker rmi -f mcap_cpp_ci
	rm -rf bench/build
	rm -rf examples/build
	rm -rf test/build

.PHONY: format-check
format-check: dev-image
	docker run -t --rm --name mcap_cpp_dev -v $(CURDIR):/src mcap_cpp python3 scripts/format.py /src

.PHONY: format-fix
format-fix: dev-image
	docker run -t --rm --name mcap_cpp_dev -v $(CURDIR):/src mcap_cpp python3 scripts/format.py --fix /src

.PHONY: bench
bench: dev-image
	docker run -t --rm --name mcap_cpp_dev -v $(CURDIR):/src mcap_cpp ./bench/build/Release/bin/bench-tests

.PHONY: bench-host
bench-host:
	./bench/build/Release/bin/bench-tests

.PHONY: test
test: dev-image
	docker run -t --rm --name mcap_cpp_dev -v $(CURDIR):/src mcap_cpp ./test/build/Debug/bin/unit-tests

.PHONY: test-host
test-host:
	./test/build/Debug/bin/unit-tests

.PHONY: hdoc-build
hdoc-build:
	docker build -t hdoc -f hdoc.Dockerfile .

.PHONY: run-examples
run-examples: dev-image
	docker run -t --rm --name mcap_cpp_dev -v $(CURDIR):/src mcap_cpp ./run_examples.sh

.PHONY: ci-image
ci-image:
	docker build -t mcap_cpp_ci -f ci.Dockerfile --build-arg IMAGE=ubuntu:focal --build-arg CC=clang-13 --build-arg CXX=clang++-13 .

.PHONY: ci
ci: ci-image
	docker run -t --rm --name mcap_cpp_ci -v $(CURDIR):/mcap/cpp mcap_cpp ./build.sh

.PHONY: ci-docs
ci-docs: hdoc-build ci-image
	docker build -t mcap_cpp -f ci.Dockerfile .
	docker run -t --rm --name mcap_cpp_ci -v $(CURDIR):/mcap/cpp -v $(CURDIR)/../__docs__/cpp:/hdoc-output mcap_cpp ./build-docs.sh

.PHONY: ci-format-check
ci-format-check: hdoc-build ci-image
	docker run -t --rm --name mcap_cpp_ci -v $(CURDIR):/mcap/cpp mcap_cpp python3 scripts/format.py .
