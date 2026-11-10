#[=======================================================================[.rst:
FindLZ4
-------

Finds the LZ4 compression library and provides the imported target
``LZ4::LZ4``.

Tries, in order: a CMake package config for ``lz4`` (Conan's CMakeDeps or
lz4's own install), pkg-config (``liblz4``), then a plain search for
``lz4.h`` and the library.

Result variables: ``LZ4_FOUND`` and, when known, ``LZ4_VERSION``.

Call with ``find_package(LZ4 MODULE)``: under CMAKE_FIND_PACKAGE_PREFER_CONFIG
a ``lz4-config.cmake`` would otherwise satisfy the search without creating
``LZ4::LZ4``.
#]=======================================================================]

if(TARGET LZ4::LZ4)
  set(LZ4_FOUND TRUE)
  return()
endif()

include(FindPackageHandleStandardArgs)

# What LZ4::LZ4 will link to.
set(_lz4_link "")
set(LZ4_VERSION "")

# Makes an imported target visible to consumers in parent directories, for
# the add_subdirectory() case. Only for targets the find_package() below
# created: CMake refuses to promote a target created elsewhere, and such a
# target is already visible to every consumer.
function(_lz4_make_global target)
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
set(_lz4_candidates lz4::lz4 LZ4::lz4_shared LZ4::lz4_static)
set(_lz4_preexisting "")
foreach(_candidate ${_lz4_candidates})
  if(TARGET ${_candidate})
    list(APPEND _lz4_preexisting ${_candidate})
  endif()
endforeach()
find_package(lz4 CONFIG QUIET)
if(lz4_FOUND)
  foreach(_candidate ${_lz4_candidates})
    if(TARGET ${_candidate})
      set(_lz4_link ${_candidate})
      if(NOT _candidate IN_LIST _lz4_preexisting)
        _lz4_make_global(${_candidate})
      endif()
      break()
    endif()
  endforeach()
  if(_lz4_link AND lz4_VERSION)
    set(LZ4_VERSION "${lz4_VERSION}")
  endif()
endif()

# 2. pkg-config.
if(NOT _lz4_link)
  find_package(PkgConfig QUIET)
  if(PkgConfig_FOUND)
    pkg_check_modules(_lz4_pc QUIET IMPORTED_TARGET GLOBAL liblz4)
    if(_lz4_pc_FOUND)
      set(_lz4_link PkgConfig::_lz4_pc)
      set(LZ4_VERSION "${_lz4_pc_VERSION}")
    endif()
  endif()
endif()

# 3. Plain search.
if(NOT _lz4_link)
  find_path(LZ4_INCLUDE_DIR NAMES lz4.h)
  find_library(LZ4_LIBRARY NAMES lz4 liblz4)
  mark_as_advanced(LZ4_INCLUDE_DIR LZ4_LIBRARY)
  if(LZ4_INCLUDE_DIR AND LZ4_LIBRARY)
    set(_lz4_link "${LZ4_LIBRARY}")
    if(EXISTS "${LZ4_INCLUDE_DIR}/lz4.h")
      file(STRINGS "${LZ4_INCLUDE_DIR}/lz4.h" _lz4_version_lines
           REGEX "^#define LZ4_VERSION_(MAJOR|MINOR|RELEASE) +[0-9]+")
      string(REGEX REPLACE ".*LZ4_VERSION_MAJOR +([0-9]+).*" "\\1" _lz4_major "${_lz4_version_lines}")
      string(REGEX REPLACE ".*LZ4_VERSION_MINOR +([0-9]+).*" "\\1" _lz4_minor "${_lz4_version_lines}")
      string(REGEX REPLACE ".*LZ4_VERSION_RELEASE +([0-9]+).*" "\\1" _lz4_release "${_lz4_version_lines}")
      if(_lz4_major MATCHES "^[0-9]+$" AND _lz4_minor MATCHES "^[0-9]+$" AND _lz4_release MATCHES "^[0-9]+$")
        set(LZ4_VERSION "${_lz4_major}.${_lz4_minor}.${_lz4_release}")
      endif()
    endif()
  endif()
endif()

find_package_handle_standard_args(LZ4
  REQUIRED_VARS _lz4_link
  VERSION_VAR LZ4_VERSION)

if(LZ4_FOUND)
  if(TARGET "${_lz4_link}")
    add_library(LZ4::LZ4 INTERFACE IMPORTED GLOBAL)
    set_target_properties(LZ4::LZ4 PROPERTIES INTERFACE_LINK_LIBRARIES "${_lz4_link}")
  else()
    add_library(LZ4::LZ4 UNKNOWN IMPORTED GLOBAL)
    set_target_properties(LZ4::LZ4 PROPERTIES
      IMPORTED_LOCATION "${LZ4_LIBRARY}"
      INTERFACE_INCLUDE_DIRECTORIES "${LZ4_INCLUDE_DIR}")
  endif()
endif()

unset(_lz4_link)
unset(_lz4_candidates)
unset(_lz4_preexisting)
