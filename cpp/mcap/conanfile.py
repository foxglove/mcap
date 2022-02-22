from conans import ConanFile, tools


class McapConan(ConanFile):
    name = "mcap"
    version = "0.0.1"
    url = "https://github.com/foxglove/mcap"
    homepage = "https://github.com/foxglove/mcap"
    description = "A C++ implementation of the MCAP file format"
    license = "Apache-2.0"
    topics = ("mcap", "serialization", "deserialization", "recording")

    settings = ("os", "compiler", "build_type", "arch")
    generators = "cmake"

    def validate(self):
        tools.check_min_cppstd(self, "17")

    def configure(self):
        pass

    def package(self):
        self.copy(pattern="LICENSE", dst="licenses")
        self.copy("include/*")
