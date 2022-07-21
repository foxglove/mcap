from conans import ConanFile, CMake


class McapExamplesConan(ConanFile):
    settings = "os", "compiler", "build_type", "arch"
    generators = "cmake"
    requires = ["mcap/0.1.2", "protobuf/3.21.1", "nlohmann_json/3.10.5"]

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()
