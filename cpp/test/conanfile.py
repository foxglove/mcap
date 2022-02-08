from conans import ConanFile, CMake


class McapTestConan(ConanFile):
    settings = "os", "compiler", "build_type", "arch"
    generators = "cmake"
    requires = "mcap/0.0.1"
    build_requires = "catch2/2.13.8"

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()
