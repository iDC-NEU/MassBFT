//
// Created by peng on 10/18/22.
//

#pragma once

#include "client/core/generator/uniform_long_generator.h"
#include "common/phmap.h"
#include <string>

namespace client::utils {
    using ByteIterator = std::string;
    using ByteIteratorMap = util::MyFlatHashMap<std::string, ByteIterator>;

    inline std::string RandomString(auto length) {
        static const auto alpha = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        static const auto len = 63;
        auto ul = client::core::UniformLongGenerator(0, len - 1);
        std::string result;
        result.resize(length);
        for (auto& it: result) {
            it = alpha[ul.nextValue()];
        }
        return result;
    };
}
