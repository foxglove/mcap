set(SOURCES
    include/mcap/crc32.hpp
    include/mcap/errors.hpp
    include/mcap/internal.hpp
    include/mcap/intervaltree.hpp
    include/mcap/mcap.hpp
    include/mcap/read_job_queue.hpp
    include/mcap/reader.hpp
    include/mcap/reader.inl
    include/mcap/types.hpp
    include/mcap/types.inl
    include/mcap/visibility.hpp
    include/mcap/writer.hpp
    include/mcap/writer.inl
)

add_library(mcap INTERFACE ${SOURCES})
add_library(mcap::mcap ALIAS mcap)
target_include_directories(
    mcap
    INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

include(CMakePackageConfigHelpers)
write_basic_package_version_file(
    "${PROJECT_BINARY_DIR}/mcap-config-version.cmake"
    VERSION ${CMAKE_PROJECT_VERSION}
    COMPATIBILITY AnyNewerVersion
)

install(TARGETS mcap
    EXPORT mcap-targets
    RUNTIME DESTINATION bin
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    INCLUDES
    DESTINATION include
)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/ DESTINATION include)

include(CMakePackageConfigHelpers)
configure_package_config_file(
    "${PROJECT_SOURCE_DIR}/cmake/mcap-config.cmake.in"
    "${PROJECT_BINARY_DIR}/mcap-config.cmake"
    INSTALL_DESTINATION lib/cmake/mcap
)

install(EXPORT mcap-targets DESTINATION lib/cmake/mcap)
install(FILES "${PROJECT_BINARY_DIR}/mcap-config-version.cmake"
              "${PROJECT_BINARY_DIR}/mcap-config.cmake"
        DESTINATION lib/cmake/mcap)
