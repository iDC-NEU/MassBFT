cmake_minimum_required(VERSION 3.12)
project(ycsb_client)

# After source files is built.
set(SOURCE_FILES
        measurements/measurements.cpp
        property.cpp
)

add_library(${LIB_YCSB} SHARED STATIC ${SOURCE_FILES})

add_executable(${PROJECT_NAME}
        ycsb.cpp
        )

add_dependencies(ycsb_client build_external_projects)

target_link_libraries(${PROJECT_NAME}
        ${LIB_PTHREADS}
        ${LIB_GLOG}
        ${LIB_YAML}
        ${LIB_YCSB}
        )

# install to ${CMAKE_INSTALL_PREFIX}/bin folder.
install(TARGETS ${PROJECT_NAME} DESTINATION ${CMAKE_INSTALL_PREFIX}/bin)
