CXX ?= g++
CXXFLAGS ?= -O2 -Wall -Wextra -Wpedantic -Wconversion \
  -Wundef -Wshadow -Walloca -Wcast-qual \
  -Wdisabled-optimization -Wdouble-promotion -Wfloat-equal \
  -Wformat-signedness -Winit-self -Wmissing-include-dirs -Wmultichar \
  -Wpacked -Wpointer-arith -Wredundant-decls \
  -Wswitch-default -Wwrite-strings \
  -fvisibility=hidden \
  -std=c++17 -I../cpp/mcap/include

RUST_WRITE = rust_bench/target/release/bench_write
RUST_READ = rust_bench/target/release/bench_read

GO_WRITE = go_bench/bench_write
GO_READ = go_bench/bench_read

.PHONY: all bench cpp_bench rust_bench go_bench python_bench typescript_bench clean

all: cpp_bench rust_bench go_bench python_bench typescript_bench

cpp_bench: cpp_bench/bench_write cpp_bench/bench_read

python_bench:
	@test -f python_bench/bench_write.py || (echo "ERROR: python_bench/bench_write.py not found" >&2; exit 1)
	@test -f python_bench/bench_read.py || (echo "ERROR: python_bench/bench_read.py not found" >&2; exit 1)

cpp_bench/bench_write: cpp_bench/bench_write.cpp
	$(CXX) $(CXXFLAGS) cpp_bench/bench_write.cpp -o cpp_bench/bench_write -llz4 -lzstd

cpp_bench/bench_read: cpp_bench/bench_read.cpp
	$(CXX) $(CXXFLAGS) cpp_bench/bench_read.cpp -o cpp_bench/bench_read -llz4 -lzstd

rust_bench: rust_bench/src/bin/bench_write.rs rust_bench/src/bin/bench_read.rs rust_bench/Cargo.toml
	cd rust_bench && cargo build --release

go_bench: go_bench/cmd/bench_write/main.go go_bench/cmd/bench_read/main.go go_bench/go.mod
	cd go_bench && go build -o bench_write ./cmd/bench_write && go build -o bench_read ./cmd/bench_read

typescript_bench:
	@test -f typescript_bench/bench_write.ts || (echo "ERROR: typescript_bench/bench_write.ts not found" >&2; exit 1)
	@test -f typescript_bench/bench_read.ts || (echo "ERROR: typescript_bench/bench_read.ts not found" >&2; exit 1)

bench: all
	FILL=uniform ./run_bench.sh
	FILL=varied ./run_bench.sh

clean:
	rm -f cpp_bench/bench_write cpp_bench/bench_read
	cd rust_bench && cargo clean
	rm -f go_bench/bench_write go_bench/bench_read
