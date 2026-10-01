add_executable(gys_compress
    ${PROJECT_SOURCE_DIR}/src/compression/gys_compress.cpp
    ${PROJECT_SOURCE_DIR}/src/compression/predcorr_compress.cpp
)

target_include_directories(gys_compress PRIVATE
    ${PROJECT_SOURCE_DIR}/src
    ${PROJECT_SOURCE_DIR}/src/compression
)

target_link_libraries(gys_compress
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

install(TARGETS gys_compress)
