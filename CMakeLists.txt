cmake_minimum_required(VERSION 3.14...3.22)

# ---- Project ----

project(
        NBP
        VERSION 1.0
        LANGUAGES CXX
)

# Set debug and release flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-parameter")

### CHECK g++ version
if(CMAKE_COMPILER_IS_GNUCXX)
  execute_process(COMMAND ${CMAKE_C_COMPILER} -dumpversion OUTPUT_VARIABLE GCC_VERSION)
  if (GCC_VERSION VERSION_GREATER 11.0 OR GCC_VERSION VERSION_EQUAL 11.0)
    message(STATUS "Support C++20.")
    set(CMAKE_CXX_STANDARD 20)
  else ()
    message(FATAL_ERROR "C++20 needed. Therefore a gcc compiler with a version higher than 11.0 is needed.")
  endif()
endif(CMAKE_COMPILER_IS_GNUCXX)

# ---- Include guards ----

if(PROJECT_SOURCE_DIR STREQUAL PROJECT_BINARY_DIR)
  message(
          FATAL_ERROR
          "In-source builds not allowed. Please make a new directory (called a build directory) and run CMake from there."
  )
endif()

# ---- Add dependencies via CPM ----
# see https://github.com/TheLartians/CPM.cmake for more info

include(vendor/CPM.cmake)

### Thread support
include(FindThreads)

# PackageProject.cmake will be used to make our target installable
CPMAddPackage("gh:TheLartians/PackageProject.cmake@1.8.0")

CPMAddPackage(
        NAME fmt
        GITHUB_REPOSITORY fmtlib/fmt
        GIT_TAG 8.1.1
        OPTIONS "FMT_INSTALL YES" # create an installable target
)

CPMAddPackage(
        NAME yaml-cpp
        GITHUB_REPOSITORY jbeder/yaml-cpp
        VERSION 0.7.0
        GIT_TAG yaml-cpp-0.7.0
        OPTIONS "YAML_CPP_FORMAT_SOURCE OFF"
)

CPMAddPackage(
        NAME glog
        GITHUB_REPOSITORY google/glog
        VERSION 0.6.0
        GIT_TAG v0.6.0
        OPTIONS "WITH_GFLAGS OFF" "WITH_GTEST OFF"
)

### Add zmq support (only link cppzmq is enough)
CPMAddPackage(
        NAME zmq
        GITHUB_REPOSITORY zeromq/libzmq
        VERSION 22.11.20
        GIT_TAG 8d4f4efef0b45a242c0b0f927d715ac79d863729
        OPTIONS "ZMQ_BUILD_TESTS OFF" "ENABLE_CURVE OFF" "WITH_TLS OFF"
)

CPMAddPackage(
        NAME cppzmq
        GITHUB_REPOSITORY zeromq/cppzmq
        VERSION v4.9.0
        GIT_TAG c66fc6094b2a03439dea8469719e221e80e2e8e7
)

### concurrent map header only
CPMAddPackage(
        NAME unordered_dense  ## include the project name
        GITHUB_REPOSITORY martinus/unordered_dense
        VERSION v2.0.0
        GIT_TAG bf7336568fce3993d0fdb136a9b8599f46d8fb40
)

### SPMC queue header only
CPMAddPackage(
        NAME RiftenDeque  ## include the project name
        GITHUB_REPOSITORY ConorWilliams/ConcurrentDeque
        GIT_TAG 1552c895c60b2ca8986abd3f3e4fd38847687d25
)

### MPMC queue header only
CPMAddPackage(
        NAME MPMCQueue  ## include the project name
        GITHUB_REPOSITORY rigtorp/MPMCQueue
        GIT_TAG 28d05c021d68fc5280b593329d1982ed02f9d7b3
)

### A thread pool
CPMAddPackage(
        NAME ThreadPool
        GITHUB_REPOSITORY DeveloperPaul123/thread-pool
        GIT_TAG 80b3c8b40e881a3b4f88599e873321a46d66f232
        OPTIONS "TP_BUILD_TESTS OFF" "TP_BUILD_EXAMPLES OFF" "TP_BUILD_BENCHMARKS OFF"
)

### Crypto support
find_package(openssl
        CONFIG
        REQUIRED
        PATHS ${PROJECT_SOURCE_DIR}/vendor
        )

### Protobuf support
find_package(pbwrapper
        CONFIG
        REQUIRED
        PATHS ${PROJECT_SOURCE_DIR}/vendor
        )

### zpp-bits support
find_package(zppbits
        CONFIG
        REQUIRED
        PATHS ${PROJECT_SOURCE_DIR}/vendor
        )

SET(COMMON_DEPENDENCIES glog yaml-cpp fmt pthread cppzmq unordered_dense MPMCQueue RiftenDeque ThreadPool crypto protobuf)

### compile protobuf
include_directories(${CMAKE_BINARY_DIR}/include)
LINK_DIRECTORIES(${CMAKE_BINARY_DIR}/lib)

# ---- Add source files ----

# Note: globbing sources is considered bad practice as CMake's generators may not detect new files
# automatically. Keep that in mind when changing files, or explicitly mention them here.
file(GLOB_RECURSE headers CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/include/*.h")
file(GLOB_RECURSE sources CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")

# ---- Create library ----

# Note: for header-only libraries change all PUBLIC flags to INTERFACE and create an interface
# target: add_library(${PROJECT_NAME} INTERFACE)
add_library(${PROJECT_NAME} ${headers} ${sources})
set_target_properties(${PROJECT_NAME} PROPERTIES CXX_STANDARD 20)

# being a cross-platform target, we enforce standards conformance on MSVC
target_compile_options(${PROJECT_NAME} PUBLIC "$<$<COMPILE_LANG_AND_ID:CXX,MSVC>:/permissive->")

# Link dependencies
target_link_libraries(${PROJECT_NAME} PRIVATE ${COMMON_DEPENDENCIES})

target_include_directories(
        ${PROJECT_NAME} PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/${PROJECT_NAME}-${PROJECT_VERSION}>
)

# ---- Create an installable target ----
# this allows users to install and find the library via `find_package()`.

# the location where the project's version header will be placed should match the project's regular
# header paths
string(TOLOWER ${PROJECT_NAME}/version.h VERSION_HEADER_LOCATION)

add_subdirectory(standalone)
add_subdirectory(tests)
add_subdirectory(doc)