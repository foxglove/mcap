# Plan: Remove Conan from C++ CI

## Goal
Replace Conan usage in the C++ CI pipeline with vendored dependencies and
CMake-only configuration so CI is faster and simpler.

## Steps
1. Audit C++ CI and build scripts to locate Conan usage and dependency
   resolution points.
2. Inventory current Conan dependencies and map them to vendored sources
   (submodules, third-party directory, or checked-in archives).
3. Add/adjust CMake configuration to use vendored dependencies directly
   (include paths, targets, and link settings).
4. Remove Conan install/setup steps from CI workflows and local build docs.
5. Validate C++ builds/tests in CI, update caches if needed, and document
   the new dependency flow.

## Notes
- Keep changes scoped to C++ CI and build tooling.
- Ensure vendored dependencies have clear versions and licenses.

## Step 1 Findings (Conan usage)
- CI workflows cache `~/.conan/data` for C++ jobs and install Conan on Windows
  (`conformance-cpp`, `cpp`, `cpp-windows`). C++ Windows uses `pip install
  conan~=1.0` then runs `build.sh --build-tests-only`.
- `cpp/Makefile` runs Docker-based C++ CI (`ci-clang`, `ci-gcc`, `ci-docs`,
  `ci-format-check`) and mounts `~/.conan/data` into the container.
- `cpp/build.sh` and `cpp/build-docs.sh` drive Conan: `conan config init`,
  `conan editable add`, `conan install`, and `conan build` for tests, examples,
  benchmarks, and docs.
- C++ CI images install Conan in `cpp/ci.Dockerfile` and `cpp/dev.Dockerfile`.
- CMake projects rely on `conanbuildinfo.cmake`, `conan_basic_setup()`, and
  `${CONAN_LIBS}` linking:
  - `cpp/test`, `cpp/bench`, `cpp/docs`, `cpp/examples`
  - `cpp/examples/jsonschema`, `cpp/examples/protobuf`
- Conan recipes define dependency resolution points:
  - `cpp/mcap/conanfile.py`: `lz4/1.9.4`, `zstd/1.5.2`
  - `cpp/test/conanfile.py`: `catch2/2.13.8`, `nlohmann_json/3.10.5`, `mcap`
  - `cpp/examples/conanfile.py`: `protobuf/3.21.1`, `nlohmann_json/3.10.5`,
    `catch2/2.13.8`, `mcap`
  - `cpp/bench/conanfile.py`: `benchmark/1.7.0`, `mcap`
  - `cpp/docs/conanfile.py`: `mcap`

## Notes from discussion (no decisions yet)
- Catch2 is header-only; possible approach is to vendor it into a `vendor/`
  directory and add the include path in CMake.
- nlohmann/json is header-only; possible approach is to vendor it into a
  `vendor/` directory and add the include path in CMake.
- zstd, lz4, protobuf, and benchmark could be treated as system dependencies:
  install them in `ci.Dockerfile`, then link using CMake `find_package` targets
  in the relevant binaries.
- Vendor directory location: `cpp/vendor`.
- Windows testing assumption: run tests in the Linux Docker container (no native
  Windows dependency strategy planned).
