option(BUILD_DEISA_BENCH_APP "Build the deisa benchmark mini app" ON)

if(BUILD_DEISA_BENCH_APP)
    add_subdirectory(deisa_bench)
endif()
