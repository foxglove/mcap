import os
import re

from conan import ConanFile
from conan.tools.build import check_min_cppstd
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain
from conan.tools.files import copy, load, rmdir

required_conan_version = ">=2.0"


class McapConan(ConanFile):
    name = "mcap"
    url = "https://github.com/foxglove/mcap"
    homepage = "https://github.com/foxglove/mcap"
    description = "A C++ implementation of the MCAP file format"
    license = "MIT"
    topics = ("mcap", "serialization", "deserialization", "recording")
    package_type = "header-library"
    settings = "os", "arch", "compiler", "build_type"

    # Developer switches for the CMake project; none affect the package.
    options = {
        "build_tests": [True, False],
        "build_examples": [True, False],
        "build_bench": [True, False],
        "build_docs": [True, False],
    }
    default_options = {
        "build_tests": False,
        "build_examples": False,
        "build_bench": False,
        "build_docs": False,
    }

    exports_sources = (
        "CMakeLists.txt",
        "cmake/*",
        "mcap/*",
        "test/*",
        "examples/*",
        "bench/*",
        "docs/*",
        "!*/build/*",
    )

    def set_version(self):
        # The version is defined once, in types.hpp; CMakeLists.txt reads it too.
        types_hpp = os.path.join(self.recipe_folder, "mcap", "include", "mcap", "types.hpp")
        match = re.search(r'^#define MCAP_LIBRARY_VERSION "([^"]+)"', load(self, types_hpp), re.M)
        if not match:
            raise RuntimeError(f"Could not read MCAP_LIBRARY_VERSION from {types_hpp}")
        self.version = match.group(1)

    def layout(self):
        self.folders.source = "."
        # One tree per configuration. Docs get their own so hdoc sees only the
        # docs translation unit in compile_commands.json.
        if self.options.build_docs:
            self.folders.build = os.path.join("build", "docs")
        else:
            self.folders.build = os.path.join("build", str(self.settings.build_type))
        self.folders.generators = os.path.join(self.folders.build, "generators")
        self.cpp.source.includedirs = ["mcap/include"]

    def requirements(self):
        self.requires("lz4/1.10.0")
        self.requires("zstd/1.5.2")

        test_requires = set()
        if self.options.build_tests:
            test_requires.update(["catch2/2.13.8", "nlohmann_json/3.10.5"])
        if self.options.build_examples:
            test_requires.update(["catch2/2.13.8", "nlohmann_json/3.10.5", "protobuf/3.21.12"])
        if self.options.build_bench:
            test_requires.add("benchmark/1.7.0")
        for ref in sorted(test_requires):
            self.test_requires(ref)

    def package_id(self):
        self.info.clear()

    def validate(self):
        check_min_cppstd(self, 17)

    def generate(self):
        tc = CMakeToolchain(self)
        tc.cache_variables["MCAP_BUILD_TESTS"] = bool(self.options.build_tests)
        tc.cache_variables["MCAP_BUILD_EXAMPLES"] = bool(self.options.build_examples)
        tc.cache_variables["MCAP_BUILD_BENCH"] = bool(self.options.build_bench)
        tc.cache_variables["MCAP_BUILD_DOCS"] = bool(self.options.build_docs)
        tc.cache_variables["MCAP_INSTALL"] = True
        tc.generate()
        CMakeDeps(self).generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        copy(self, "LICENSE", src=os.path.join(self.source_folder, "mcap"),
             dst=os.path.join(self.package_folder, "licenses"))
        cmake = CMake(self)
        cmake.install()
        # Conan consumers get a generated config; the installed one is for
        # non-Conan installs.
        rmdir(self, os.path.join(self.package_folder, "lib", "cmake"))

    def package_info(self):
        self.cpp_info.bindirs = []
        self.cpp_info.libdirs = []
        self.cpp_info.includedirs = ["include"]
        self.cpp_info.set_property("cmake_file_name", "mcap")
        self.cpp_info.set_property("cmake_target_name", "mcap::mcap")
        self.cpp_info.requires = ["lz4::lz4", "zstd::zstdlib"]
