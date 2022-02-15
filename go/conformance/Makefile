build:
	mkdir -p bin
	make -C test-streamed-write-conformance build
	make -C test-streamed-read-conformance build

test:
	make -C test-streamed-write-conformance test
	make -C test-streamed-read-conformance test

clean:
	rm -rf bin
