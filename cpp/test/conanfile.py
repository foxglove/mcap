from conan import ConanFile
from conan.tools.build import can_run
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain, cmake_layout
import os

class McapTestConan(ConanFile):
    settings = "os", "compiler", "build_type", "arch"
    test_type = "explicit"

    def requirements(self):
        self.requires(self.tested_reference_str)
        self.requires("catch2/[>=2.13.8 <3.0]")
        self.requires("nlohmann_json/[>=3.10.5]")

    def layout(self):
        cmake_layout(self)

    def generate(self):
        tc = CMakeToolchain(self)
        tc.generate()
        deps = CMakeDeps(self)
        deps.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def test(self):
        if can_run(self):
            # Only the unit-tests executable can be run directly.
            # The conformance test executables are require additional input.
            bin_path = os.path.join(self.cpp.build.bindirs[0], "unit-tests")
            self.run(bin_path, env="conanrun")
