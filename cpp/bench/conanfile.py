from conans import ConanFile, CMake


class McapBenchmarksConan(ConanFile):
    settings = "os", "compiler", "build_type", "arch"
    generators = "cmake"
    requires = "mcap/0.0.1"
    build_requires = "benchmark/1.6.0"

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()
