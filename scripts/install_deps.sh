#!/usr/bin/env bash
# make sure the nc-bft package is under ../

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y zip unzip git cmake libtool make autoconf g++-11 zlib1g-dev libgoogle-perftools-dev g++ openssh-server

# the zip is already decompressed by ca
# unzip -q ../nc_bft.zip ../

chmod +x ../nc_bft/clash-linux-amd64-v3
nohup ../nc_bft/clash-linux-amd64-v3 -f ../nc_bft/proxy.yaml &

export https_proxy=http://127.0.0.1:7890;
export http_proxy=http://127.0.0.1:7890;
export all_proxy=socks5://127.0.0.1:7890;

# wait for clash ready
sleep 5

# build the binary
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=/usr/bin/gcc-11 -DCMAKE_CXX_COMPILER=/usr/bin/g++-11 -B build
cmake --build build --target NBPStandalone_peer NBPStandalone_ycsb NBPStandalone_ca -j
