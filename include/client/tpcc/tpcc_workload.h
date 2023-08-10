//
// Created by user on 23-8-10.
//

#pragma once

#include "client/core/workload.h"
#include "client/core/generator/discrete_generator.h"
#include "client/tpcc/tpcc_helper.h"
#include "tpcc_property.h"

namespace client::tpcc {
    enum class Operation {
        NEW_ORDER,
        PAYMENT,
    };
    using TPCCDiscreteGenerator = core::DiscreteGenerator<Operation>;

    class TPCCWorkload: public core::Workload {
    public:
        struct InvokeRequestType {
            constexpr static const auto TPCC = "tpcc";
            constexpr static const auto NEW_ORDER = "n";
            constexpr static const auto PAYMENT = "p";
        };

        void init(const ::util::Properties& prop) override;

        bool doTransaction(core::DB* db) const override;

    protected:
        void initOperationGenerator(const TPCCProperties::Proportion& p);

        bool doNewOrderRand(core::DB* db, int warehouseId) const;

        bool doPaymentRand(core::DB* db, int warehouseId) const;

    private:
        int warehouseCount;
        std::unique_ptr<core::NumberGenerator> warehouseChooser;
        std::unique_ptr<core::NumberGenerator> districtIdChooser;
        std::unique_ptr<core::NumberGenerator> orderLineCountChooser;
        std::unique_ptr<core::DoubleGenerator> percentChooser;
        std::unique_ptr<core::DoubleGenerator> amountChooser;
        std::unique_ptr<TPCCDiscreteGenerator> operationChooser;
        std::unique_ptr<TPCCHelper> helper;
    };
}
