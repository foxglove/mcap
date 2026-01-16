find_path(ZSTD_INCLUDE_DIR
  NAMES zstd.h
)

find_library(ZSTD_LIBRARY
  NAMES zstd
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(ZSTD
  REQUIRED_VARS ZSTD_LIBRARY ZSTD_INCLUDE_DIR
)

if(ZSTD_FOUND AND NOT TARGET ZSTD::ZSTD)
  add_library(ZSTD::ZSTD UNKNOWN IMPORTED)
  set_target_properties(ZSTD::ZSTD PROPERTIES
    IMPORTED_LOCATION "${ZSTD_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${ZSTD_INCLUDE_DIR}"
  )
endif()
