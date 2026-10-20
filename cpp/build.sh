#!/usr/bin/env bash

set -e

CONAN_SETTINGS=(-s compiler.cppstd=17)

# Create a default profile if one does not exist yet. We deliberately do not pass
# --force so an existing developer profile is left untouched; CI containers start
# without a profile, so detection still runs there. compiler.cppstd is forced via
# -s below, so the detected profile's default standard does not matter.
conan profile detect 2>/dev/null || true

# Builds one configuration in build/<build_type>. `conan build` installs
# dependencies, generates and builds; `&:` scopes the options to this recipe.
conan_build() {
  local build_type=$1
  shift
  conan build . "${CONAN_SETTINGS[@]}" -s build_type="$build_type" "$@" --build=missing
}

conan_build Debug -o "&:build_tests=True"

if [ "$1" != "--build-tests-only" ]; then
  conan_build Release -o "&:build_examples=True" -o "&:build_bench=True"

  # Packages into the Conan cache and builds test_package against it. This
  # checks the Conan package only; the cpp-cmake CI job covers the plain
  # CMake install.
  conan create . "${CONAN_SETTINGS[@]}" --build=missing
fi
