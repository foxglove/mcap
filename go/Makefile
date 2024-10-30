test:
	make -C mcap test
	make -C ros test
	make -C conformance test
	make -C cli/mcap test

lint:
	make -C mcap lint
	make -C ros lint
	make -C cli/mcap lint
	make -C conformance lint

build-conformance-binaries:
	make -C conformance build

clean:
	make -C conformance clean

bench:
	make -C mcap bench
