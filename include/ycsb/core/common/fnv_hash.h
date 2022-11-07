//
// Created by peng on 10/17/22.
//

#pragma once

#include <cstdint>

namespace ycsb::utils {
    template<uint64_t kFNVOffsetBasis64 = 0xCBF29CE484222325, uint64_t kFNVPrime64 = 1099511628211>
    inline uint64_t FNVHash64(uint64_t val) {
        uint64_t hash = kFNVOffsetBasis64;
        for (int i = 0; i < 8; i++) {
            uint64_t octet = val & 0x00ff;
            val = val >> 8;

            hash = hash ^ octet;
            hash = hash * kFNVPrime64;
        }
        return hash;
    }
}
