# enable cgo to build sqlite
export CGO_ENABLED = 1

GOOS=$(shell go env GOOS)

OUTPUT ?= mcap

ifeq ($(GOOS),windows)
	OUTPUT := $(addsuffix .exe,$(OUTPUT))
endif

ifeq ($(GOOS),linux)
flags:: version
	# statically link binaries to support alpine linux
	$(eval FLAGS="-tags sqlite_omit_load_extension,netgo,osusergo -ldflags '-s -w -extldflags "-static" -X github.com/foxglove/mcap/go/cli/mcap/cmd.Version=$(VERSION)'")
else
flags:: version
	$(eval FLAGS="-tags sqlite_omit_load_extension -ldflags '-s -w -X github.com/foxglove/mcap/go/cli/mcap/cmd.Version=$(VERSION)'")
endif

version: $(eval VERSION=$(shell sed 's_releases/mcap-cli/__g' <<< $$(git describe --tags)))
version:
	@echo VERSION=$(VERSION)

build: flags
	mkdir -p bin
	go build -o bin/$(OUTPUT) $(subst $\",,$(FLAGS)) .

lint:
	golangci-lint run ./...

test:
	go test ./...

bench:
	make -C cmd bench
