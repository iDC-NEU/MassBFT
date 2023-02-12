cmake_minimum_required(VERSION 3.14...3.22)

CPMAddPackage(
        NAME zppbits
        GITHUB_REPOSITORY eyalz800/zpp_bits
        GIT_TAG 4b88b0cdce67242d44c0e203f313e40973dbdc07
        DOWNLOAD_ONLY True
)

execute_process(COMMAND cp -f ${zppbits_SOURCE_DIR}/zpp_bits.h ${zppbits_BINARY_DIR}
        RESULT_VARIABLE result
        WORKING_DIRECTORY ${zppbits_SOURCE_DIR}
        OUTPUT_QUIET)
if(result)
    message(FATAL_ERROR "Configure for zppbits failed: ${result}")
endif()

include_directories(${zppbits_BINARY_DIR})