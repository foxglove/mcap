from conan import ConanFile
from conan.tools.cmake import CMake, cmake_layout


class McapExamplesConan(ConanFile):
    settings = "os", "compiler", "build_type", "arch"
    generators = "CMakeDeps", "CMakeToolchain"
    requires = [
        "mcap/2.1.2",
        "protobuf/3.21.1",
        "nlohmann_json/3.10.5",
        "catch2/2.13.8",
    ]

    def layout(self):
        cmake_layout(self)

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()
