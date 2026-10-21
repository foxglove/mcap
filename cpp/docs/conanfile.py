from conan import ConanFile
from conan.tools.cmake import CMake


class McapDocsConan(ConanFile):
    settings = "os", "compiler", "build_type", "arch"
    generators = "CMakeDeps", "CMakeToolchain"
    requires = "mcap/2.2.0"

    def layout(self):
        self.folders.build = "."
        self.folders.generators = "generators"

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()
