test:
	go test -cover ./...

bench:
	go test -benchmem -run=^$$ -count 5 -bench ^BenchmarkLexer/demo -memprofile mem.out -cpuprofile cpu.out

lint:
	golangci-lint run ./...
