//
// Created by peng on 11/6/22.
//

# include "ycsb/core/measurements/measurements.h"

namespace ycsb::core {
    Measurements *Measurements::singleton;

    std::mutex Measurements::mutex;
}
