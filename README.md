# MassBFT: Fast and Scalable Geo-Distributed Byzantine Fault-Tolerant Consensus

## Installation Instructions

### Prerequisites
* Ubuntu 22.04
* GCC 11.4.0
* CMake >=3.14.0

### Dependencies
```sh
sudo apt-get update
sudo apt-get install -y zip unzip git cmake libtool make autoconf g++-11 zlib1g-dev libgoogle-perftools-dev g++
```

### Building

#### MassBFT
Please ensure that your internet connection is active and reliable during building process.

```sh
git clone https://github.com/iDC-NEU/mass_bft.git
cd mass_bft
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=/usr/bin/gcc-11 -DCMAKE_CXX_COMPILER=/usr/bin/g++-11 -B build
cmake --build build -j
```

#### [BFT-SMART](https://github.com/bft-smart/library)

The modified version of the BFT-SMART source code is available [here](scripts/nc-bft-src.zip), and the precompiled binary is [here](https://github.com/iDC-NEU/mass_bft/releases/download/dep/nc_bft.zip).

Please place BFT-SMART and MassBFT in the same directory

```sh
cd ..
wget https://github.com/iDC-NEU/mass_bft/releases/download/dep/nc_bft.zip
unzip -q -o nc_bft.zip
```

```
root@iZ6weg2bv7ohyev6mlnzdsZ:~# tree -L 1
.
├── mass_bft
├── nc_bft
└── nc_bft.zip
```

### Preparing Config Files

[Stand-alone deployment (1 group with 3 nodes)](doc/en/test_network.md)

[Distributed deployment](doc/en/distributed_deployment.md)

### Troubleshooting
