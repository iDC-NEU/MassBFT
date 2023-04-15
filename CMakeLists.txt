cmake_minimum_required(VERSION 3.14...3.22)

# ---- Project ----

project(
        NBP
        VERSION 1.0
        LANGUAGES CXX
)

# For fresh ubuntu 22.04 startup
# zlib1g-dev is for gzip compress
# libgoogle-perftools-dev for linking tcmalloc
# apt install libtool make autoconf g++-11 zlib1g-dev libgoogle-perftools-dev
# apt install g++

# Set debug and release flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wno-unused-parameter -Wno-unused-variable")
SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} ${CMAKE_CXX_FLAGS_COMMON}  -g -O0 -fprofile-arcs -ftest-coverage")
SET(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} ${CMAKE_CXX_FLAGS_COMMON} -O2")

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
        GIT_SHALLOW TRUE
)

CPMAddPackage(
        NAME yaml-cpp
        GITHUB_REPOSITORY jbeder/yaml-cpp
        VERSION 0.7.0
        GIT_TAG yaml-cpp-0.7.0
        OPTIONS "YAML_CPP_FORMAT_SOURCE OFF"
        GIT_SHALLOW TRUE
)

### Gflags support
find_package(gflags_install
        CONFIG
        REQUIRED
        PATHS ${PROJECT_SOURCE_DIR}/vendor
        )

### Glog support
find_package(glog_install
        CONFIG
        REQUIRED
        PATHS ${PROJECT_SOURCE_DIR}/vendor
        )

### Add zmq support
find_package(zeromq_install
        CONFIG
        REQUIRED
        PATHS ${PROJECT_SOURCE_DIR}/vendor)

### fast hashmap header only
CPMAddPackage(
        NAME unordered_dense  ## include the project name
        GITHUB_REPOSITORY martinus/unordered_dense
        VERSION v2.0.0
        GIT_TAG bf7336568fce3993d0fdb136a9b8599f46d8fb40
)

### greg7mdp/gtl gel template repo
CPMAddPackage(
        NAME gtl  ## include the project name
        GITHUB_REPOSITORY greg7mdp/gtl
        VERSION v1.1.5
        GIT_TAG 0f8cecf52c91bb8f265d6ff2fc32cd8144c987a6
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

### Another MPMC queue header only (not linearizable)
CPMAddPackage(
        NAME concurrentqueue  ## include the project name
        GITHUB_REPOSITORY sydxsty/concurrentqueue
        GIT_TAG 590201524f6bb47dbbd00fe9dd2838c4adbef070
)

### Crypto support
find_package(openssl_install
        CONFIG
        REQUIRED
        PATHS ${PROJECT_SOURCE_DIR}/vendor
        )

find_package(libssh_install
        CONFIG
        REQUIRED
        PATHS ${PROJECT_SOURCE_DIR}/vendor
        )

### Protobuf support
find_package(protobuf_install
        CONFIG
        REQUIRED
        PATHS ${PROJECT_SOURCE_DIR}/vendor
        )

### rocksdb support
find_package(rocksdb_install
        CONFIG
        REQUIRED
        PATHS ${PROJECT_SOURCE_DIR}/vendor
        )

### Leveldb support, needed for brpc
find_package(leveldb_install
        CONFIG
        REQUIRED
        PATHS ${PROJECT_SOURCE_DIR}/vendor
        )

### BRPC support
find_package(brpc_install
        CONFIG
        REQUIRED
        PATHS ${PROJECT_SOURCE_DIR}/vendor
        )

### BRAFT support
find_package(braft_install
        CONFIG
        REQUIRED
        PATHS ${PROJECT_SOURCE_DIR}/vendor
        )


### zpp-bits support
find_package(zppbits_install
        CONFIG
        REQUIRED
        PATHS ${PROJECT_SOURCE_DIR}/vendor
        )

### Erasure code support, must link erasurecode dl
find_package(erasurecode_install
        CONFIG
        REQUIRED
        PATHS ${PROJECT_SOURCE_DIR}/vendor
        )

### Gtest support
find_package(gtest_install
        CONFIG
        REQUIRED
        PATHS ${PROJECT_SOURCE_DIR}/vendor
        )

include(${CMAKE_CURRENT_SOURCE_DIR}/vendor/tools.cmake)
### compile protobuf
file(GLOB PROTO_FILES "${CMAKE_CURRENT_SOURCE_DIR}/include/proto/*.proto")
# https://stackoverflow.com/questions/26147454/how-to-automatically-move-pb-h-and-pb-h-under-include-and-src-directories
my_protobuf_generate_cpp(include/proto PROTO_SRCS PROTO_HDRS ${PROTO_FILES})
ADD_LIBRARY(${PROJECT_NAME}_proto ${PROTO_HDRS} ${PROTO_SRCS})

find_library(gflags gflags PATHS ${PROJECT_BINARY_DIR}/lib)
# NOTE: DO NOT LINK BRPC.SO AND BRAFT.SO TOGETHER
SET(COMMON_DEPENDENCIES glog yaml-cpp fmt pthread zmq braft unordered_dense gtl MPMCQueue concurrentqueue RiftenDeque crypto protobuf erasurecode dl grc tcmalloc ${PROJECT_NAME}_proto rocksdb ssh)

include_directories(SYSTEM ${CMAKE_BINARY_DIR}/include)
LINK_DIRECTORIES(${CMAKE_BINARY_DIR}/lib)

# ---- Add source files ----

# Note: globbing sources is considered bad practice as CMake's generators may not detect new files
# automatically. Keep that in mind when changing files, or explicitly mention them here.
file(GLOB_RECURSE headers CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/include/*.h")
file(GLOB_RECURSE sources CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")

# ---- Create library ----

# Note: for header-only libraries change all PUBLIC flags to INTERFACE and create an interface
# target: add_library(${PROJECT_NAME} INTERFACE)
add_library(${PROJECT_NAME} ${headers} ${sources} include/common/sftp.h src/common/sftp.cpp)
set_target_properties(${PROJECT_NAME} PROPERTIES CXX_STANDARD 20)

# being a cross-platform target, we enforce standards conformance on MSVC
target_compile_options(${PROJECT_NAME} PUBLIC "$<$<COMPILE_LANG_AND_ID:CXX,MSVC>:/permissive->")

# Link dependencies
# https://stackoverflow.com/questions/58997230/cmake-project-fails-to-find-shared-library
target_link_libraries(${PROJECT_NAME}
        PRIVATE
        ${COMMON_DEPENDENCIES}
        "-Wl,--disable-new-dtags")

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