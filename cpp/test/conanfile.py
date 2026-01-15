from conan import ConanFile
from conan.tools.cmake import CMake, cmake_layout


class McapTestConan(ConanFile):
    settings = "os", "compiler", "build_type", "arch"
    generators = "CMakeDeps", "CMakeToolchain"
    requires = "catch2/2.13.8", "mcap/2.1.2", "nlohmann_json/3.10.5"

    def layout(self):
        cmake_layout(self)

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()
