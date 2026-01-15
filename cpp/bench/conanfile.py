from conan import ConanFile
from conan.tools.cmake import CMake, cmake_layout


class McapBenchmarksConan(ConanFile):
    settings = "os", "compiler", "build_type", "arch"
    generators = "CMakeDeps", "CMakeToolchain"
    requires = "benchmark/1.7.0", "mcap/2.1.2"

    def layout(self):
        cmake_layout(self)

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()
