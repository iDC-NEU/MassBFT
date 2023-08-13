//
// Created by peng on 10/18/22.
//

#pragma once

#include "client/core/common/random_double.h"
#include "glog/logging.h"

namespace client::core {
    template <class Operation>
    class DiscreteGenerator : public Generator<Operation> {
    public:
        void addValue(double weight, Operation value) {
            if (values.empty()) {
                Generator<Operation>::setLastValue(value);
            }
            values.emplace_back(value, weight);
            sum += weight;
        }

        Operation nextValue() override {
            double chooser = randomDouble.nextValue();
            for (auto p : values) {
                if (chooser < p.second / sum) {
                    Generator<Operation>::setLastValue(p.first);
                    return p.first;
                }
                chooser -= p.second / sum;
            }
            CHECK(false) << "The sum of the proportions of each transaction type may be less than 1";
            return Generator<Operation>::lastValue();
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
