import os

from conan import ConanFile
from conan.tools.build import check_min_cppstd
from conan.tools.files import copy


class McapConan(ConanFile):
    name = "mcap"
    version = "2.1.3"
    url = "https://github.com/foxglove/mcap"
    homepage = "https://github.com/foxglove/mcap"
    description = "A C++ implementation of the MCAP file format"
    license = "MIT"
    topics = ("mcap", "serialization", "deserialization", "recording")
    package_type = "header-library"
    settings = "os", "arch", "compiler", "build_type"

    def layout(self):
        self.folders.source = "."
        self.cpp.source.includedirs = ["include"]

    def requirements(self):
        self.requires("lz4/1.9.4")
        self.requires("zstd/1.5.2")

    def package_id(self):
        self.info.clear()

    def validate(self):
        check_min_cppstd(self, 17)

    def package(self):
        copy(self, "LICENSE", dst=os.path.join(self.package_folder, "licenses"))
        copy(self, "*", dst=os.path.join(self.package_folder, "include"), src="include")

    def package_info(self):
        self.cpp_info.bindirs = []
        self.cpp_info.libdirs = []
        self.cpp_info.includedirs = ["include"]
        self.cpp_info.set_property("cmake_file_name", "mcap")
        self.cpp_info.set_property("cmake_target_name", "mcap::mcap")
        self.cpp_info.requires = ["lz4::lz4", "zstd::libzstd_static"]
