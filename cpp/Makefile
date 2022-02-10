default: build

.PHONY: build
build:
	docker build -t mcap_cpp -f dev.Dockerfile .
	docker run --rm --name mcap_cpp -v $(CURDIR):/src mcap_cpp

.PHONE: build-host
build-host:
	conan install bench --install-folder bench/build/Release \
    -s compiler.cppstd=17 -s build_type=Release --build missing
	conan install examples --install-folder examples/build/Release \
    -s compiler.cppstd=17 -s build_type=Release --build missing
	conan install test --install-folder test/build/Debug \
    -s compiler.cppstd=17 -s build_type=Debug --build missing
	conan build examples --build-folder examples/build/Release
	conan build bench --build-folder bench/build/Release
	conan build test --build-folder test/build/Debug

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

.PHONY: test
test:
	docker run --rm --name mcap_cpp -v $(CURDIR):/src mcap_cpp ./test/build/Debug/bin/unit-tests
