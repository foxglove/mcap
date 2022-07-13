default: build

.PHONY: build
build:
	docker build -t mcap_cpp -f dev.Dockerfile .
	docker run -t --rm --name mcap_cpp -v $(CURDIR):/src mcap_cpp

.PHONE: build-host
build-host:
	./build.sh

clean:
	docker rm -f mcap_cpp
	docker rmi -f mcap_cpp
	rm -rf bench/build
	rm -rf examples/build
	rm -rf test/build

.PHONY: format-check
format-check:
	docker build -t mcap_cpp -f dev.Dockerfile .
	docker run -t --rm --name mcap_cpp -v $(CURDIR):/src mcap_cpp python3 scripts/format.py /src

.PHONY: format-fix
format-fix:
	docker build -t mcap_cpp -f dev.Dockerfile .
	docker run -t --rm --name mcap_cpp -v $(CURDIR):/src mcap_cpp python3 scripts/format.py --fix /src

.PHONY: bench
bench:
	docker run -t --rm --name mcap_cpp -v $(CURDIR):/src mcap_cpp ./bench/build/Release/bin/bench-tests

.PHONY: bench-host
bench-host:
	./bench/build/Release/bin/bench-tests

.PHONY: test
test:
	docker run -t --rm --name mcap_cpp -v $(CURDIR):/src mcap_cpp ./test/build/Debug/bin/unit-tests

.PHONY: test-host
test-host:
	./test/build/Debug/bin/unit-tests

.PHONY: hdoc-build
hdoc-build:
	docker build -t hdoc -f hdoc.Dockerfile .

.PHONY: ci
ci: hdoc-build
	docker build -t mcap_cpp -f ci.Dockerfile --build-arg IMAGE=ubuntu:focal --build-arg CC=clang-13 --build-arg CXX=clang++-13 .
	docker run -t --rm --name mcap_cpp -v $(CURDIR):/mcap/cpp mcap_cpp ./build.sh

.PHONY: ci-docs
ci-docs: hdoc-build
	docker build -t mcap_cpp -f ci.Dockerfile .
	docker run -t --rm --name mcap_cpp -v $(CURDIR):/mcap/cpp -v $(CURDIR)/../__docs__/cpp:/hdoc-output mcap_cpp ./build-docs.sh

.PHONY: ci-format-check
ci-format-check: hdoc-build
	docker build -t mcap_cpp -f ci.Dockerfile --build-arg IMAGE=ubuntu:focal --build-arg CC=clang-13 --build-arg CXX=clang++-13 .
	docker run -t --rm --name mcap_cpp -v $(CURDIR):/mcap/cpp mcap_cpp python3 scripts/format.py .

.PHONY: dev-shell
dev-shell:
	docker build -t mcap_cpp_dev -f dev.Dockerfile .
	docker run -it --rm --name mcap_cpp_dev -v $(CURDIR):/mcap/cpp mcap_cpp_dev /bin/bash