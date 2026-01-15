import os

from conan import ConanFile
from conan.tools.build import check_min_cppstd
from conan.tools.files import copy


class McapConan(ConanFile):
    name = "mcap"
    version = "2.1.2"
    url = "https://github.com/foxglove/mcap"
    homepage = "https://github.com/foxglove/mcap"
    description = "A C++ implementation of the MCAP file format"
    license = "MIT"
    topics = ("mcap", "serialization", "deserialization", "recording")
    package_type = "header-library"
    exports_sources = "include/*", "LICENSE"

    settings = ("os", "compiler", "build_type", "arch")
    requires = ("lz4/1.9.4", "zstd/1.5.2")

    def validate(self):
        check_min_cppstd(self, "17")

    def layout(self):
        self.folders.source = "."
        self.cpp.source.includedirs = ["include"]

    def configure(self):
        pass

    def package(self):
        copy(self, "LICENSE", dst=os.path.join(self.package_folder, "licenses"), src=self.source_folder)
        copy(self, "include/*", dst=os.path.join(self.package_folder, "include"), src=self.source_folder)

    def package_info(self):
        self.cpp_info.set_property("cmake_file_name", "mcap")
        self.cpp_info.set_property("cmake_target_name", "mcap::mcap")
        self.cpp_info.includedirs = ["include"]
        self.cpp_info.requires = ["lz4::lz4", "zstd::zstd"]
