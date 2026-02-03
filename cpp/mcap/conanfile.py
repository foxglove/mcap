from conan import ConanFile
from conan.tools.files import copy
from conan.tools.build import check_min_cppstd
from os import path


class McapConan(ConanFile):
    name = "mcap"
    version = "2.1.2"
    url = "https://github.com/foxglove/mcap"
    homepage = "https://github.com/foxglove/mcap"
    description = "A C++ implementation of the MCAP file format"
    license = "MIT"
    topics = ("mcap", "serialization", "deserialization", "recording")

    settings = ("os", "compiler", "build_type", "arch")
    requires = ("lz4/1.9.4", "zstd/1.5.2")
    generators = ("CMakeToolchain", "CMakeDeps")

    def validate(self):
        check_min_cppstd(self, "17")

    def configure(self):
        pass

    def package(self):
        copy(
            self,
            pattern="LICENSE",
            src=self.recipe_folder,
            dst=path.join(self.package_folder, "licenses"),
        )
        copy(
            self,
            "*",
            src=path.join(self.recipe_folder, "include"),
            dst=path.join(self.package_folder, "include"),
        )

    def package_id(self):
        self.info.clear()
