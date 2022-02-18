default: build

.PHONY: build
build:
	docker build -t mcap_cpp -f dev.Dockerfile .
	docker run --rm --name mcap_cpp -v $(CURDIR):/src mcap_cpp

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
	docker run --rm --name mcap_cpp -v $(CURDIR):/src mcap_cpp python3 scripts/format.py /src

.PHONY: format-fix
format-fix:
	docker build -t mcap_cpp -f dev.Dockerfile .
	docker run --rm --name mcap_cpp -v $(CURDIR):/src mcap_cpp python3 scripts/format.py --fix /src

.PHONY: bench
bench:
	docker run --rm --name mcap_cpp -v $(CURDIR):/src mcap_cpp ./bench/build/Release/bin/bench-tests

.PHONY: bench-host
bench-host:
	./bench/build/Release/bin/bench-tests

.PHONY: test
test:
	docker run --rm --name mcap_cpp -v $(CURDIR):/src mcap_cpp ./test/build/Debug/bin/unit-tests

.PHONY: test-host
test-host:
	./test/build/Debug/bin/unit-tests

.PHONY: ci
ci:
	docker build -t mcap_cpp -f ci.Dockerfile .
	docker run --rm --name mcap_cpp -v $(HOME):$(HOME) mcap_cpp

.PHONY: ci-format-check
ci-format-check:
	docker build -t mcap_cpp -f ci.Dockerfile .
	docker run --rm --name mcap_cpp -v $(HOME):$(HOME) mcap_cpp python3 scripts/format.py .
