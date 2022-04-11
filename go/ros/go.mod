module github.com/foxglove/mcap/go/ros

go 1.18

replace github.com/foxglove/mcap/go/mcap => ../../mcap

require (
	github.com/foxglove/mcap/go/mcap v0.0.0-20220316142927-cc81709134cd
	github.com/mattn/go-sqlite3 v1.14.11
	github.com/pierrec/lz4/v4 v4.1.12
	github.com/stretchr/testify v1.7.0
)

require (
	github.com/davecgh/go-spew v1.1.0 // indirect
	github.com/klauspost/compress v1.14.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.0-20200313102051-9f266ea9e77c // indirect
)
