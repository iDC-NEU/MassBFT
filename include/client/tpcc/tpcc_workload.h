//
// Created by user on 23-8-10.
//

#pragma once

#include "client/core/workload.h"
#include "client/core/generator/discrete_generator.h"
#include "client/tpcc/tpcc_helper.h"
#include "tpcc_property.h"

namespace client::tpcc {

    using namespace ::client::core;

    enum class Operation {
        NEW_ORDER,
        PAYMENT,
    };
    using TPCCDiscreteGenerator = DiscreteGenerator<Operation>;

    class TPCCWorkload: public Workload {
    public:
        struct InvokeRequestType {
            constexpr static const auto TPCC = "tpcc";
            constexpr static const auto NEW_ORDER = "n";
            constexpr static const auto PAYMENT = "p";
        };

        void init(const ::util::Properties& prop) override;

        bool doTransaction(DB* db) const override;

    protected:
        void initOperationGenerator(const TPCCProperties::Proportion& p);

        bool doNewOrderRand(DB* db, int warehouseId) const;

        bool doPaymentRand(DB* db, int warehouseId) const;

    private:
        int warehouseCount;
        std::unique_ptr<NumberGenerator> warehouseChooser;
        std::unique_ptr<NumberGenerator> districtIdChooser;
        std::unique_ptr<NumberGenerator> orderLineCountChooser;
        std::unique_ptr<DoubleGenerator> percentChooser;
        std::unique_ptr<DoubleGenerator> amountChooser;
        std::unique_ptr<TPCCDiscreteGenerator> operationChooser;
        std::unique_ptr<TPCCHelper> helper;
    };
}
