SHELL=/bin/bash
BINARY_NAME ?= mcap

GOOS=$(shell go env GOOS)
ifeq ($(GOOS),linux)
flags:: version
	# statically link binaries to support alpine linux
	$(eval FLAGS="-tags sqlite_omit_load_extension,netgo,osusergo -ldflags '-s -w -extldflags "-static" -X github.com/foxglove/mcap/go/cli/mcap/cmd.Version=$(VERSION)'")
else
flags:: version
	$(eval FLAGS="-tags sqlite_omit_load_extension -ldflags '-s -w -X github.com/foxglove/mcap/go/cli/mcap/cmd.Version=$(VERSION)'")
endif
ifeq ($(GOOS),windows)
	ifneq ($(suffix $(BINARY_NAME)),.exe)
    BINARY_NAME := $(addsuffix .exe,$(BINARY_NAME))
	endif
endif

version: $(eval VERSION=$(shell sed 's_releases/mcap-cli/__g' <<< $$(git describe --tags)))
version:
	@echo VERSION=$(VERSION)

build: flags
	mkdir -p bin
	go build -o bin/$(BINARY_NAME) $(subst $\",,$(FLAGS)) .

lint:
	golangci-lint run ./...

test:
	go test ./...

bench:
	make -C cmd bench
