# The C++ benches build through cpp_bench/CMakeLists.txt, which consumes the
# mcap project in ../cpp and holds the compiler flags. CMake honors CC/CXX on
# the first configure.
CPP_BUILD_DIR = cpp_bench/build
CMAKE_ARGS ?=

RUST_WRITE = rust_bench/target/release/bench_write
RUST_READ = rust_bench/target/release/bench_read

GO_WRITE = go_bench/bench_write
GO_READ = go_bench/bench_read

.PHONY: all bench cpp_bench rust_bench go_bench python_bench typescript_bench clean

all: cpp_bench rust_bench go_bench python_bench typescript_bench

# Always runs CMake; the build is incremental and tracks the mcap headers.
cpp_bench:
	cmake -S cpp_bench -B $(CPP_BUILD_DIR) $(CMAKE_ARGS)
	cmake --build $(CPP_BUILD_DIR)

python_bench:
	@test -f python_bench/bench_write.py || (echo "ERROR: python_bench/bench_write.py not found" >&2; exit 1)
	@test -f python_bench/bench_read.py || (echo "ERROR: python_bench/bench_read.py not found" >&2; exit 1)
	@test -f gen_blob.py || (echo "ERROR: gen_blob.py not found" >&2; exit 1)

rust_bench: rust_bench/src/bin/bench_write.rs rust_bench/src/bin/bench_read.rs rust_bench/Cargo.toml
	cd rust_bench && cargo build --release

go_bench: go_bench/cmd/bench_write/main.go go_bench/cmd/bench_read/main.go go_bench/go.mod
	cd go_bench && go build -o bench_write ./cmd/bench_write && go build -o bench_read ./cmd/bench_read

# Build the packages the TypeScript benches import.
TS_PACKAGES = core nodejs support

typescript_bench:
	@test -f typescript_bench/bench_write.ts || (echo "ERROR: typescript_bench/bench_write.ts not found" >&2; exit 1)
	@test -f typescript_bench/bench_read.ts || (echo "ERROR: typescript_bench/bench_read.ts not found" >&2; exit 1)
	cd .. && $(foreach p,$(TS_PACKAGES),yarn workspace @mcap/$(p) build &&) true

bench: all
	./run_bench.sh

clean:
	rm -f cpp_bench/bench_write cpp_bench/bench_read
	rm -rf $(CPP_BUILD_DIR)
	cd rust_bench && cargo clean
	rm -f go_bench/bench_write go_bench/bench_read
