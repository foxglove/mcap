
- [File format specification](./docs/specification)
  - [Kaitai Struct definition](./docs/specification/mcap.ksy)
- [Motivation](./docs/motivation/evaluation-of-robotics-data-recording-file-formats.md)
- [Support Matrix](./docs/support-matrix.md)

## Developer quick start

MCAP libraries are provided in the following languages. For guidance on each language, see its corresponding README:

| Language              | Readme                 | API docs                                                        | Package name | Version                                                                              |
| --------------------- | ---------------------- | --------------------------------------------------------------- | ------------ | ------------------------------------------------------------------------------------ |
| C++                   | [readme](./cpp)        | [API docs](https://mcap.dev/docs/cpp)                           | `mcap`       | [![](https://shields.io/conan/v/mcap)](https://conan.io/center/mcap)                 |
| Go                    | [readme](./go/mcap)    | [API docs](https://pkg.go.dev/github.com/foxglove/mcap/go/mcap) |              | see [releases](https://github.com/foxglove/mcap/releases)                            |
| Python                | [readme](./python)     | [API docs](https://mcap.dev/docs/python)                        | `mcap`       | [![](https://shields.io/pypi/v/mcap)](https://pypi.org/project/mcap/)                |
| JavaScript/TypeScript | [readme](./typescript) | [API docs](https://mcap.dev/docs/typescript)                    | `@mcap/core` | [![](https://shields.io/npm/v/@mcap/core)](https://www.npmjs.com/package/@mcap/core) |
| Swift                 | [readme](./swift)      | [API docs](https://mcap.dev/docs/swift/documentation/mcap)      |              | see [releases](https://github.com/foxglove/mcap/releases)                            |
| Rust                  | [readme](./rust)       | [API docs](https://mcap.dev/docs/rust/mcap)                     | `mcap`       | [![](https://shields.io/crates/v/mcap)](https://crates.io/crates/mcap)               |

To run the conformance tests, you will need to use [Git LFS](https://git-lfs.github.com/),
which is used to store the test logs under `tests/conformance/data`.

## CLI tool

A CLI tool for interacting with the format is available [here](./go/cli/mcap).

## License

[MIT License](/LICENSE). Contributors are required to accept the [Contributor License Agreement](https://github.com/foxglove/cla).
