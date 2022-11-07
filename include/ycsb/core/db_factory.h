//
// Created by peng on 11/6/22.
//

#ifndef NEUCHAIN_PLUS_DB_FACTORY_H
#define NEUCHAIN_PLUS_DB_FACTORY_H

#include <string>
#include <memory>
#include "yaml-cpp/yaml.h"
#include "ycsb/neuchain_db.h"

namespace ycsb::core {
/**
 * Creates a DB layer by dynamically classloading the specified DB class.
 */
    class DBFactory {
    private:
        DBFactory() = default;

    public:
        static auto NewDB(const std::string &dbname, const YAML::Node &properties) {
            auto ret = std::make_unique<ycsb::client::NeuChainDB>();
            ret->properties = &properties;
            return ret;
        }
    };
}

#endif //NEUCHAIN_PLUS_DB_FACTORY_H
