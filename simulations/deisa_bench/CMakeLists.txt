add_executable(gys_deisa_bench
    ${PROJECT_SOURCE_DIR}/src/deisa_bench/gys_deisa_bench.cpp
    ${PROJECT_SOURCE_DIR}/src/deisa_bench/predcorr_deisa_bench.cpp
)

target_include_directories(gys_deisa_bench PRIVATE
    ${PROJECT_SOURCE_DIR}/src
    ${PROJECT_SOURCE_DIR}/src/deisa_bench
)

target_link_libraries(gys_deisa_bench
    PUBLIC
        gysela_mini_app_lib
        DDC::core
        PDI::PDI_C
        paraconf::paraconf
        MPI::MPI_CXX
        gyselalibxx::initialisation_xyvxvy_double
        gyselalibxx::vlasov_xyvxvy_double
        gyselalibxx::poisson_xy_double
        gyselalibxx::advection
        gyselalibxx::speciesinfo
        gyselalibxx::time_integration_xyvxvy_double
        gyselalibxx::io
        gyselalibxx::utils
)

install(TARGETS gys_deisa_bench)
