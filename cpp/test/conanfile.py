from conan import ConanFile
from conan.tools.cmake import CMake, CMakeToolchain, CMakeDeps


class McapTestConan(ConanFile):
    settings = "os", "compiler", "build_type", "arch"
    requires = "catch2/2.13.8", "mcap/2.1.2", "nlohmann_json/3.10.5"

    def generate(self):
        tc = CMakeToolchain(self)
        tc.generate()
        deps = CMakeDeps(self)
        deps.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()
