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

    inline void RandomString(auto& container, int length) {
        if ((int)container.size() < length) {
            LOG(ERROR) << "RandomString container is too small!";
            length = container.size();
        }
        static const auto alpha = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        static const auto len = 63;
        auto ul = client::core::UniformLongGenerator(0, len - 1);
        for (int i=0; i<length; i++) {
            container[i] = alpha[ul.nextValue()];
        }
    };

    inline std::string RandomString(int length) {
        std::string result;
        result.resize(length);
        RandomString(result, length);
        return result;
    };
}
