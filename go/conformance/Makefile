build:
	mkdir -p bin
	make -C test-write-conformance build
	make -C test-read-conformance build

test:
	make -C test-write-conformance test
	make -C test-read-conformance test

lint:
	golangci-lint run ./test-read-conformance/...
	golangci-lint run ./test-write-conformance/...

clean:
	rm -rf bin
