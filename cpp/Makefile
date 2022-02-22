default: build

.PHONE: build
build:
	./build.sh

clean:
	rm -rf build

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
	./build/bench/bench-tests

.PHONY: test
test:
	./build/test/unit-tests

.PHONY: ci
ci:
	docker build -t mcap_cpp -f ci.Dockerfile .
	docker run --rm --name mcap_cpp -v $(HOME):$(HOME) mcap_cpp

.PHONY: ci-format-check
ci-format-check:
	docker build -t mcap_cpp -f ci.Dockerfile .
	docker run --rm --name mcap_cpp -v $(HOME):$(HOME) mcap_cpp python3 scripts/format.py .
