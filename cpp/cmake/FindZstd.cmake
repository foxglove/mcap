#[=======================================================================[.rst:
FindZstd
--------

Finds the Zstandard compression library and provides the imported target
``Zstd::Zstd``.

Tries, in order: a CMake package config for ``zstd`` (Conan's CMakeDeps or
zstd's own install), pkg-config (``libzstd``), then a plain search for
``zstd.h`` and the library.

Result variables: ``Zstd_FOUND`` and, when known, ``Zstd_VERSION``.

Call with ``find_package(Zstd MODULE)``: under CMAKE_FIND_PACKAGE_PREFER_CONFIG
a ``zstd-config.cmake`` would otherwise satisfy the search without creating
``Zstd::Zstd``.
#]=======================================================================]

if(TARGET Zstd::Zstd)
  set(Zstd_FOUND TRUE)
  return()
endif()

include(FindPackageHandleStandardArgs)

# What Zstd::Zstd will link to.
set(_zstd_link "")
set(Zstd_VERSION "")

# Makes an imported target visible to consumers in parent directories, for
# the add_subdirectory() case. Only for targets the find_package() below
# created: CMake refuses to promote a target created elsewhere, and such a
# target is already visible to every consumer.
function(_zstd_make_global target)
  get_target_property(_aliased "${target}" ALIASED_TARGET)
  if(_aliased)
    set(target "${_aliased}")
  endif()
  get_target_property(_imported "${target}" IMPORTED)
  get_target_property(_global "${target}" IMPORTED_GLOBAL)
  if(_imported AND NOT _global)
    set_target_properties("${target}" PROPERTIES IMPORTED_GLOBAL TRUE)
  endif()
endfunction()

# 1. Package config.
set(_zstd_candidates zstd::libzstd zstd::libzstd_shared zstd::libzstd_static)
set(_zstd_preexisting "")
foreach(_candidate ${_zstd_candidates})
  if(TARGET ${_candidate})
    list(APPEND _zstd_preexisting ${_candidate})
  endif()
endforeach()
find_package(zstd CONFIG QUIET)
if(zstd_FOUND)
  foreach(_candidate ${_zstd_candidates})
    if(TARGET ${_candidate})
      set(_zstd_link ${_candidate})
      if(NOT _candidate IN_LIST _zstd_preexisting)
        _zstd_make_global(${_candidate})
      endif()
      break()
    endif()
  endforeach()
  if(_zstd_link AND zstd_VERSION)
    set(Zstd_VERSION "${zstd_VERSION}")
  endif()
endif()

# 2. pkg-config.
if(NOT _zstd_link)
  find_package(PkgConfig QUIET)
  if(PkgConfig_FOUND)
    pkg_check_modules(_zstd_pc QUIET IMPORTED_TARGET GLOBAL libzstd)
    if(_zstd_pc_FOUND)
      set(_zstd_link PkgConfig::_zstd_pc)
      set(Zstd_VERSION "${_zstd_pc_VERSION}")
    endif()
  endif()
endif()

# 3. Plain search.
if(NOT _zstd_link)
  find_path(Zstd_INCLUDE_DIR NAMES zstd.h)
  find_library(Zstd_LIBRARY NAMES zstd libzstd)
  mark_as_advanced(Zstd_INCLUDE_DIR Zstd_LIBRARY)
  if(Zstd_INCLUDE_DIR AND Zstd_LIBRARY)
    set(_zstd_link "${Zstd_LIBRARY}")
    if(EXISTS "${Zstd_INCLUDE_DIR}/zstd.h")
      file(STRINGS "${Zstd_INCLUDE_DIR}/zstd.h" _zstd_version_lines
           REGEX "^#define ZSTD_VERSION_(MAJOR|MINOR|RELEASE) +[0-9]+")
      string(REGEX REPLACE ".*ZSTD_VERSION_MAJOR +([0-9]+).*" "\\1" _zstd_major "${_zstd_version_lines}")
      string(REGEX REPLACE ".*ZSTD_VERSION_MINOR +([0-9]+).*" "\\1" _zstd_minor "${_zstd_version_lines}")
      string(REGEX REPLACE ".*ZSTD_VERSION_RELEASE +([0-9]+).*" "\\1" _zstd_release "${_zstd_version_lines}")
      if(_zstd_major MATCHES "^[0-9]+$" AND _zstd_minor MATCHES "^[0-9]+$" AND _zstd_release MATCHES "^[0-9]+$")
        set(Zstd_VERSION "${_zstd_major}.${_zstd_minor}.${_zstd_release}")
      endif()
    endif()
  endif()
endif()

find_package_handle_standard_args(Zstd
  REQUIRED_VARS _zstd_link
  VERSION_VAR Zstd_VERSION)

if(Zstd_FOUND)
  if(TARGET "${_zstd_link}")
    add_library(Zstd::Zstd INTERFACE IMPORTED GLOBAL)
    set_target_properties(Zstd::Zstd PROPERTIES INTERFACE_LINK_LIBRARIES "${_zstd_link}")
  else()
    add_library(Zstd::Zstd UNKNOWN IMPORTED GLOBAL)
    set_target_properties(Zstd::Zstd PROPERTIES
      IMPORTED_LOCATION "${Zstd_LIBRARY}"
      INTERFACE_INCLUDE_DIRECTORIES "${Zstd_INCLUDE_DIR}")
  endif()
endif()

unset(_zstd_link)
unset(_zstd_candidates)
unset(_zstd_preexisting)
