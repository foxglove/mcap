from conan import ConanFile
from conan.tools.cmake import CMake


class McapDocsConan(ConanFile):
    settings = "os", "compiler", "build_type", "arch"
    generators = "CMakeDeps", "CMakeToolchain"
    requires = "mcap/2.1.3"

    def layout(self):
        self.folders.build = "."
        self.folders.generators = "generators"

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()
