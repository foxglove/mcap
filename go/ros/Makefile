test:
	go test ./...

lint:
	golangci-lint run ./...

bench:
	go test -benchmem -run=^$$ -count 5 -bench ^Benchmark -memprofile mem.prof -cpuprofile cpu.prof
