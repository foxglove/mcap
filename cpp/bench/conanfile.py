from conan import ConanFile
from conan.tools.cmake import CMake, CMakeToolchain, CMakeDeps


class McapBenchmarksConan(ConanFile):
    settings = "os", "compiler", "build_type", "arch"
    requires = "benchmark/1.7.0", "mcap/2.1.2"

    def generate(self):
        tc = CMakeToolchain(self)
        tc.generate()
        deps = CMakeDeps(self)
        deps.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()
