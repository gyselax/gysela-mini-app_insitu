option(BUILD_COMPRESSION_APP "Build the Compression mini app" ON)

if(BUILD_COMPRESSION_APP)
    add_subdirectory(compression)
endif()
