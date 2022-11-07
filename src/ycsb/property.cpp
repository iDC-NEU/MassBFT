//
// Created by peng on 11/6/22.
//

#include "ycsb/core/property.h"
namespace ycsb::utils {
    std::unique_ptr<Properties> Properties::p;
    std::mutex Properties::mutex;
}