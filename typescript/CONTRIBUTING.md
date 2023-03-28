## Development guide

Install dependencies:

```
corepack enable
yarn install
```

Run lint/tests:

```
yarn workspace @mcap/core lint
yarn workspace @mcap/core test
```

Read and validate an MCAP file:

```
yarn workspace @foxglove/mcap-example-validate validate file.mcap
```

Run benchmarks:

```
yarn workspace @foxglove/mcap-benchmarks bench
```

Run benchmarks with Chrome debugger attached to use profiling tools:

```
yarn workspace @foxglove/mcap-benchmarks bench:debug
```
