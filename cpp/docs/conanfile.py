from conans import ConanFile, CMake


class McapDocsConan(ConanFile):
    settings = "os", "compiler", "build_type", "arch"
    generators = "cmake"
    requires = "mcap/1.3.1"

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()
