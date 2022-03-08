from conans import ConanFile, CMake


class McapExamplesConan(ConanFile):
    settings = "os", "compiler", "build_type", "arch"
    generators = "cmake"
    requires = ("fmt/8.1.1", "mcap/0.0.1")

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()
