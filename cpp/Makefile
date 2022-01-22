default: build

.PHONY: build
build:
	docker compose build

.PHONE: build-host
build-host:
	conan install bench --install-folder bench/build/Release \
  	-s compiler.cppstd=17 -s build_type=Release --build missing
	conan install examples --install-folder examples/build/Release \
  	-s compiler.cppstd=17 -s build_type=Release --build missing
	conan build examples --build-folder examples/build/Release
	conan build bench --build-folder bench/build/Release

.PHONY: format-check
format-check:
	docker compose run --rm -v .:/src base python3 scripts/format.py /src

.PHONY: format-fix
format-fix:
	docker compose run --rm -v .:/src base python3 scripts/format.py --fix /src

.PHONY: bench
bench:
	docker compose run bench

.PHONY: bag2mcap
bag2mcap:
	docker compose run bag2mcap
