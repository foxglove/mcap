<img src="/website/static/img/mcap720.webp" alt="MCAP logo" width="240" height="180"/>

# MCAP

MCAP is a modular container format and logging library for pub/sub messages with arbitrary message serialization. It is primarily intended for use in robotics applications, and works well under various workloads, resource constraints, and durability requirements.

## Documentation

- [File format specification](https://mcap.dev/spec)
  - [Kaitai Struct definition](./website/docs/spec/mcap.ksy)
- [Motivation](https://mcap.dev/files/evaluation.pdf)
- [Support Matrix](https://mcap.dev/reference)

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

## MCAP CLI

Inspect, merge, and split MCAP files from the command line using the MCAP CLI.

Install with `brew install mcap` or download the latest version directly from the [releases page](https://github.com/foxglove/mcap/releases?q=mcap-cli).

## License

[MIT License](/LICENSE).

## Release process

Release numbering follows a major.minor.patch format, abbreviated as "X.Y.Z" below.

CI will build the appropriate packages once tags are pushed, as described below.

### Go library

1. Update the `Version` in go/mcap/version.go
2. Tag a release matching the version number `go/mcap/vX.Y.Z`.

### CLI

Tag a release matching `releases/mcap-cli/vX.Y.Z`.

The version number is set at build time based on the tag.

### C++

1. Update the version in all relevant files
   - cpp/bench/conanfile.py
   - cpp/build-docs.sh
   - cpp/build.sh
   - cpp/docs/conanfile.py
   - cpp/examples/conanfile.py
   - cpp/mcap/include/mcap/types.hpp (`MCAP_LIBRARY_VERSION`)
   - cpp/mcap/include/conanfile.py
   - cpp/test/conanfile.py
2. Tag a release matching the version number `releases/cpp/vX.Y.Z`

### Python

There are several python packages; updating any follows a similar process.

1. Update the version in the appropriate `__init__.py` file
2. Tag a release
   - For the core mcap library, match the pattern `releases/python/vX.Y.Z`
   - For other packages, use `releases/python/PACKAGE/vX.Y.Z`
     - For example, `releases/python/mcap/v1.2.3`

### TypeScript

There are several TS packages; updating any follows a similar process.

1. Update the version in the appropriate `package.json`
2. Tag a release matching `releases/typescript/PACKAGE/vX.Y.Z`
   - For example, `releases/typescript/core/v1.2.3`

### Swift

Tag a release matching the version number `releases/swift/vX.Y.Z`

### Rust

1. Update the version in rust/Cargo.toml
2. Tag a release matching the version number `releases/rust/vX.Y.Z`

## Citations

If you use MCAP in your research, please cite it in your work. Our suggested
citation format is:

```
@software{MCAP,
  title = {MCAP: serialization-agnostic log container file format},
  author = {{Foxglove Developers}},
  url = {https://mcap.dev},
  version = {your version},
  date = {your date of access},
  year = {2024},
  publisher = {{Foxglove Technologies}},
  note = {Available from https://github.com/foxglove/mcap}
}
```

Please replace the version and date fields with the version of the software you
used, and the date you obtained it. Citing MCAP will help spread awareness of
the project and strengthen the ecosystem.

## Stay in touch

Join our [Discord community](https://foxglove.dev/chat) to ask questions, share feedback, and stay up to date on what our team is working on.
