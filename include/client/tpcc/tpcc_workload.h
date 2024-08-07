//
// Created by user on 23-8-10.
//

#pragma once

#include "client/core/workload.h"
#include "client/core/generator/discrete_generator.h"
#include "client/tpcc/tpcc_helper.h"
#include "client/tpcc/tpcc_property.h"

namespace client::tpcc {
    enum class Operation {
        NEW_ORDER,
        PAYMENT,
    };

    using TPCCDiscreteGenerator = core::DiscreteGenerator<Operation>;

    class TPCCWorkload: public core::Workload {
    public:
        TPCCWorkload() = default;

        void init(const ::util::Properties& prop) override;

        bool doTransaction(core::DB* db) const override;

        bool doInsert(core::DB*) const override { return false; }

    protected:
        void initOperationGenerator(const TPCCProperties::Proportion& p) {
            auto op = std::make_unique<TPCCDiscreteGenerator>();
            if (p.newOrderProportion > 0) {
                op->addValue(p.newOrderProportion, Operation::NEW_ORDER);
            }
            if (p.paymentProportion > 0) {
                op->addValue(p.paymentProportion, Operation::PAYMENT);
            }
            this->operationChooser = std::move(op);
        }

        bool doNewOrderRand(core::DB* db, int warehouseId) const;

        bool doPaymentRand(core::DB* db, int warehouseId) const;

    private:
        int warehouseCount{};
        // look up C_ID on secondary index.
        bool paymentLookup{};
        std::unique_ptr<core::NumberGenerator> warehouseChooser{};
        std::unique_ptr<core::NumberGenerator> districtIdChooser{};
        std::unique_ptr<core::NumberGenerator> orderLineCountChooser{};
        std::unique_ptr<core::DoubleGenerator> percentChooser{};
        std::unique_ptr<core::DoubleGenerator> amountChooser{};
        std::unique_ptr<TPCCDiscreteGenerator> operationChooser{};
        std::unique_ptr<TPCCHelper> helper{};
    };
}
