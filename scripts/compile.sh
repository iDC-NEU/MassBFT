#!/usr/bin/env bash

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y zip unzip git cmake libtool make autoconf g++-11 zlib1g-dev libgoogle-perftools-dev g++

# build the binary
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=/usr/bin/gcc-11 -DCMAKE_CXX_COMPILER=/usr/bin/g++-11 -B build
cmake --build build --target NBPStandalone_peer NBPStandalone_ycsb NBPStandalone_ca -j
