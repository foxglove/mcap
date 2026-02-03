from conan import ConanFile
from conan.errors import ConanInvalidConfiguration
from conan.tools.build import check_min_cppstd
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain, cmake_layout
from conan.tools.files import copy, get, rmdir
from conan.tools.scm import Version
import os

required_conan_version = ">=1.52.0"

class McapConan(ConanFile):
    name = "mcap"
    description = (
        "MCAP is a modular, performant, and serialization-agnostic container file format for pub/sub messages, "
        "primarily intended for use in robotics applications."
    )
    license = "MIT"
    url = "https://github.com/conan-io/conan-center-index"
    homepage = "https://github.com/foxglove/mcap"
    topics = ("mcap", "serialization", "deserialization", "recording")
    package_type = "library"
    settings = "os", "arch", "compiler", "build_type"
    options = {
        "header_only": [True, False],
        "shared": [True, False],
        "fPIC": [True, False],
        "with_lz4": [True, False],
        "with_zstd": [True, False],
    }
    default_options = {
        "header_only": False,
        "shared": False,
        "fPIC": True,
        "with_lz4": True,
        "with_zstd": True,
    }
    implements = ["auto_shared_fpic"]

    @property
    def _min_cppstd(self):
        return 17

    @property
    def _compilers_minimum_version(self):
        return {
            "Visual Studio": "16",
            "msvc": "191",
            "gcc": "7",
            "clang": "9",
            "apple-clang": "12",
        }
        
    def validate(self):
        if self.options.header_only and self.options.shared:
            raise ConanInvalidConfiguration("Shared library and header-only builds are mutually exclusive.")
        check_min_cppstd(self, self._min_cppstd)
        minimum_version = self._compilers_minimum_version.get(str(self.settings.compiler), False)
        if minimum_version and Version(self.settings.compiler.version) < minimum_version:
            raise ConanInvalidConfiguration(
                f"{self.ref} requires C++{self._min_cppstd}, which your compiler does not support."
            )

    def configure(self):
        if Version(self.version) < "0.3.0":
            self.license = "Apache-2.0"
        if self.options.header_only:
            self.package_type = "header-library"
            
    def package_id(self):
        if self.options.header_only:
            self.info.clear()

    def export_sources(self):
        copy(self, "CMakeLists.txt", self.recipe_folder, self.export_sources_folder)
        copy(self, "*.hpp", self.recipe_folder, self.export_sources_folder)
        copy(self, "*.cpp", self.recipe_folder, self.export_sources_folder)
        copy(self, "*.inl", self.recipe_folder, self.export_sources_folder)
        copy(self, "LICENSE", self.recipe_folder, self.export_sources_folder)

    def layout(self):
        cmake_layout(self)

    def requirements(self):
        if self.options.with_lz4:
            self.requires("lz4/[>=1.9.4]")
        if self.options.with_zstd:
            self.requires("zstd/[>=1.5.2]")

    def generate(self):
        if self.options.header_only:
            return
        tc = CMakeToolchain(self)
        tc.variables["MCAP_COMPRESSION_LZ4"] = bool(self.options.with_lz4)
        tc.variables["MCAP_COMPRESSION_ZSTD"] = bool(self.options.with_zstd)
        tc.generate()

        deps = CMakeDeps(self)
        if self.options.with_zstd:
            deps.set_property("zstd", "cmake_target_name", "zstd::zstd")
        deps.generate()

    def build(self):
        if self.options.header_only:
            return
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        copy(self, "LICENSE", dst=os.path.join(self.package_folder, "licenses"), src=self.source_folder)
        if self.options.header_only:
            copy(self, "*.hpp", dst=self.package_folder, src=self.source_folder)
            copy(self, "*.inl", dst=self.package_folder, src=self.source_folder)
        else:
            cmake = CMake(self)
            cmake.install()
            # Not allowed in Conan package.
            rmdir(self, os.path.join(self.package_folder, "lib", "cmake"))

    def package_info(self):
        self.cpp_info.set_property("cmake_target_name", "mcap::mcap")
        self.cpp_info.set_property("pkg_config_name", "mcap")
        if self.options.header_only:
            self.cpp_info.defines.append("MCAP_INLINE_IMPLEMENTATION=1")
        else:
            self.cpp_info.libs = ["mcap"]
        if not self.options.shared:
            self.cpp_info.defines.append("MCAP_PUBLIC=")

        if self.options.with_lz4:
            self.cpp_info.requires.append("lz4::lz4")
        else:
            self.cpp_info.defines.append("MCAP_COMPRESSION_NO_LZ4=1")
        if self.options.with_zstd:
            self.cpp_info.requires.append("zstd::zstd")
        else:
            self.cpp_info.defines.append("MCAP_COMPRESSION_NO_ZSTD=1")
