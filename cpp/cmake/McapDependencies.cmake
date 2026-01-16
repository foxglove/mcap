include_guard(GLOBAL)

get_filename_component(MCAP_ROOT "${CMAKE_CURRENT_LIST_DIR}/.." ABSOLUTE)
set(MCAP_VENDOR_DIR "${MCAP_ROOT}/vendor")
set(MCAP_INCLUDE_DIR "${MCAP_ROOT}/mcap/include")

find_package(LZ4 REQUIRED)
find_package(ZSTD REQUIRED)

if(NOT TARGET mcap::mcap)
  add_library(mcap INTERFACE)
  add_library(mcap::mcap ALIAS mcap)
  target_include_directories(mcap INTERFACE "${MCAP_INCLUDE_DIR}")
  target_link_libraries(mcap INTERFACE LZ4::LZ4 ZSTD::ZSTD)
endif()

if(NOT TARGET mcap::vendor_catch2)
  add_library(mcap_vendor_catch2 INTERFACE)
  add_library(mcap::vendor_catch2 ALIAS mcap_vendor_catch2)
  target_include_directories(mcap_vendor_catch2 SYSTEM INTERFACE
    "${MCAP_VENDOR_DIR}/catch2/single_include"
  )
endif()

if(NOT TARGET mcap::vendor_nlohmann)
  add_library(mcap_vendor_nlohmann INTERFACE)
  add_library(mcap::vendor_nlohmann ALIAS mcap_vendor_nlohmann)
  target_include_directories(mcap_vendor_nlohmann SYSTEM INTERFACE
    "${MCAP_VENDOR_DIR}/nlohmann_json/include"
  )
endif()
