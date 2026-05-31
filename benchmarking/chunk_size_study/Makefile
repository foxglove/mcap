CXX ?= g++
CXXFLAGS ?= -O2 -Wall -Wextra -std=c++17 -I../../cpp/mcap/include

.PHONY: all clean

all: bench

bench: bench.cpp
	$(CXX) $(CXXFLAGS) bench.cpp -o bench -llz4 -lzstd

clean:
	rm -f bench
