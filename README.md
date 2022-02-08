# MCAP file format ![](https://img.shields.io/badge/alpha-orange)

MCAP is a modular container format for recording pub/sub messages with arbitrary message serialization.

It is primarily intended for use in robotics applications, and works well under various workloads, resource constraints, and durability requirements.

## Documentation

- [File format specification](./docs/specification)

## Developer quick start

### TypeScript

Run lint/tests:

```
yarn workspace @foxglove/mcap lint
yarn workspace @foxglove/mcap test
```

Read and validate an mcap file:

```
yarn workspace @foxglove/mcap validate file.mcap
```

## License

Licensed under the [Apache License, Version 2.0](/LICENSE). Contributors are required to accept the [Contributor License Agreement](https://github.com/foxglove/cla).
