find_path(LZ4_INCLUDE_DIR
  NAMES lz4frame.h lz4.h
)

find_library(LZ4_LIBRARY
  NAMES lz4
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(LZ4
  REQUIRED_VARS LZ4_LIBRARY LZ4_INCLUDE_DIR
)

if(LZ4_FOUND AND NOT TARGET LZ4::LZ4)
  add_library(LZ4::LZ4 UNKNOWN IMPORTED)
  set_target_properties(LZ4::LZ4 PROPERTIES
    IMPORTED_LOCATION "${LZ4_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${LZ4_INCLUDE_DIR}"
  )
endif()
