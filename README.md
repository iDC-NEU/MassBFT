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

### Switch Execution Engine
- Uncomment line 46 in `include/peer/core/module_coordinator.h` to enable the `SerialCoordinator`, which uses the serial execution engine.

- Uncomment line 44 in `include/peer/core/module_coordinator.h` to activate the `CoordinatorImpl`, which implements the Aria concurrency control engine.

### Switch Consensus Mechanism

- Uncomment lines 61 and 66 in `include/peer/core/module_factory.h` to switch to the `GeoBFT` consensus.

- Uncomment lines 61 and 68 in `include/peer/core/module_factory.h` to use the `Steward` consensus.

- Uncomment lines 61 and 69 in `include/peer/core/module_factory.h` to enable the modified Hierarchical `ISS` consensus.

- Uncomment lines 61 and 65 in `include/peer/core/module_factory.h` to use the `Baseline` consensus.

- Uncomment lines 62 in `include/peer/core/module_factory.h` to use the `bijective sending approach`.

### [BFT-SMART](https://github.com/bft-smart/library)

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

### [HotStuff Engine](https://github.com/hot-stuff/libhotstuff)
The HotStuff engine has been designed to provide efficient consensus through a modular architecture.
This engine is compiled into a shared object (SO) file.
You can find the modifications related to the HotStuff engine in the hotstuff branch of our repository.

The modified HotStuff source code can be downloaded from the release page:
```sh
wget https://github.com/iDC-NEU/mass_bft/releases/download/dep/hotstuff.zip
unzip -q -o hotstuff.zip
```

### Preparing Config Files

[Stand-alone deployment (1 group with 3 nodes)](doc/en/test_network.md)

[Distributed deployment](doc/en/distributed_deployment.md)

### Troubleshooting
