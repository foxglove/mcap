build:
	mkdir -p bin
	make -C test-write-conformance build
	make -C test-read-conformance build

test:
	make -C test-write-conformance test
	make -C test-read-conformance test

clean:
	rm -rf bin
