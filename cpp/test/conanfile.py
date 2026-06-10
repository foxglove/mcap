from conan import ConanFile
from conan.tools.cmake import CMake


class McapTestConan(ConanFile):
    settings = "os", "compiler", "build_type", "arch"
    generators = "CMakeDeps", "CMakeToolchain"
    requires = "catch2/2.13.8", "mcap/2.1.3", "nlohmann_json/3.10.5"

    def layout(self):
        self.folders.build = "."
        self.folders.generators = "generators"

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()
