from conan import ConanFile
from conan.tools.cmake import CMake, CMakeToolchain, CMakeDeps


class McapExamplesConan(ConanFile):
    settings = "os", "compiler", "build_type", "arch"
    requires = [
        "mcap/2.1.2",
        "protobuf/3.21.12",
        "nlohmann_json/3.10.5",
        "catch2/2.13.8",
    ]

    def generate(self):
        tc = CMakeToolchain(self)
        tc.generate()
        deps = CMakeDeps(self)
        deps.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()
