//
// Created by peng on 2/20/23.
//

#ifndef NBP_PHMAP_H
#define NBP_PHMAP_H

#include "gtl/phmap.hpp"
#include "common/crypto.h"

namespace util {
    template<class K, class V, class Mutex=gtl::NullMutex, size_t N=4>
    using MyFlatHashMap = gtl::parallel_flat_hash_map<K, V,
            gtl::priv::hash_default_hash<K>,
            gtl::priv::hash_default_eq<K>,
            gtl::priv::Allocator<gtl::priv::Pair<const K, V>>,
            N, Mutex>;

    template<class K, class V, class Mutex=gtl::NullMutex, size_t N=4>
    using MyNodeHashMap = gtl::parallel_node_hash_map<K, V,
            gtl::priv::hash_default_hash<K>,
            gtl::priv::hash_default_eq<K>,
            gtl::priv::Allocator<gtl::priv::Pair<const K, V>>,
            N, Mutex>;
}

namespace std {
    template <>
    struct hash<util::OpenSSLSHA256::digestType> {
        size_t operator()(const util::OpenSSLSHA256::digestType& k) const {
            return *reinterpret_cast<const size_t*>(k.data());
        }
    };
}

#endif //NBP_PHMAP_H
