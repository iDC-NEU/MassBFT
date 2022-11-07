cmake_minimum_required(VERSION 3.12)
project(ycsb_client)

include(FindThreads)
find_package(glog REQUIRED)

# After source files is built.
set(SOURCE_FILES
        measurements/measurements.cpp
        property.cpp
        ycsb.cpp
)

add_executable(${PROJECT_NAME} ${SOURCE_FILES})
target_link_libraries(${PROJECT_NAME}
        pthread
        glog
        yaml-cpp
        )

# install to ${CMAKE_INSTALL_PREFIX}/bin folder.
install(TARGETS ycsb_client DESTINATION ${CMAKE_INSTALL_PREFIX}/bin)
