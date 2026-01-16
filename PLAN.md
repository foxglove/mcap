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
