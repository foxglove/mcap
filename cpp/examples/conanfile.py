from conans import ConanFile, CMake


class McapExamplesConan(ConanFile):
    settings = "os", "compiler", "build_type", "arch"
    generators = "cmake"
    requires = [
        "mcap/1.0.0",
        "protobuf/3.21.9",
        "nlohmann_json/3.11.2",
        "catch2/3.3.1",
        "zfp/1.0.0",
    ]

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()
