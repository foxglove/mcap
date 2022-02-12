# stripe binaries
FLAGS := -tags sqlite_omit_load_extension -ldflags '-s -w'

# platform-specific settings
GOOS := $(shell go env GOOS)
ifeq ($(GOOS),linux)
	# statically link binaries to support alpine linux
	FLAGS := -tags sqlite_omit_load_extension,netgo,osusergo -ldflags '-s -w -extldflags "-static"'
endif

build:
	go build $(FLAGS) .

lint:
	golangci-lint run ./...
