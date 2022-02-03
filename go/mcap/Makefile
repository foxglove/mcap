build:
# cspell:disable-next-line
	go build -ldflags="-extldflags=-static" -tags sqlite_omit_load_extension,netgo

lint:
	golangci-lint run ./...
