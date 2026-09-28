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

get_filename_component(PROJECT_ROOT_DIR ${CMAKE_SOURCE_DIR} ABSOLUTE)
set(PYTHON_MODULE_PATH "${PROJECT_ROOT_DIR}")
set_target_properties(gys_compress PROPERTIES
    ENVIRONMENT "PYTHONPATH=${PYTHON_MODULE_PATH}:$ENV{PYTHONPATH}"
)

install(TARGETS gys_compress)
