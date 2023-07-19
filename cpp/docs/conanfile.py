from conans import ConanFile, CMake


class McapDocsConan(ConanFile):
    settings = "os", "compiler", "build_type", "arch"
    generators = "cmake"
    requires = "mcap/1.2.0"

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()
