from conan import ConanFile
from conan.tools.build import can_run
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain, cmake_layout
import os

class McapExamplesConan(ConanFile):
    settings = "os", "compiler", "build_type", "arch"
    test_type = "explicit"

    def requirements(self):
        self.requires(self.tested_reference_str)
        self.requires("protobuf/[>=3.21.1]")
        self.requires("nlohmann_json/[>=3.10.5]")
        self.requires("catch2/[>=2.13.8 <3.0]")

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
            bin_path = os.path.join("protobuf", self.cpp.build.bindirs[0], "example_protobuf_unit_tests")
            self.run(bin_path, env="conanrun")
