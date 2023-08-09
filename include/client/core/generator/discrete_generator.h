//
// Created by peng on 10/18/22.
//

#pragma once

#include "client/core/common/random_double.h"
#include "glog/logging.h"

namespace client::core {
    enum class Operation {
        INSERT,
        READ,
        UPDATE,
        SCAN,
        READ_MODIFY_WRITE,
    };

    class DiscreteGenerator : public Generator<Operation> {
    public:
        void addValue(double weight, Operation value) {
            if (values.empty()) {
                setLastValue(value);
            }
            values.emplace_back(value, weight);
            sum += weight;
        }

        Operation nextValue() override {
            double chooser = randomDouble.nextValue();
            for (auto p : values) {
                if (chooser < p.second / sum) {
                    setLastValue(p.first);
                    return p.first;
                }
                chooser -= p.second / sum;
            }
            CHECK(false);
            return lastValue();
        }

        double mean() override {
            CHECK(false) << "@todo implement ZipfianGenerator.mean()";
            return -1;
        }

    private:
        utils::RandomDouble randomDouble;
        std::vector<std::pair<Operation, double>> values{};
        double sum{};
    };

}
