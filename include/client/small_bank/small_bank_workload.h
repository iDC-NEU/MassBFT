//
// Created by user on 23-8-13.
//

#pragma once

#include "client/core/workload.h"
#include "client/core/generator/discrete_generator.h"
#include "client/core/common/random_uint64.h"
#include "client/core/common/random_double.h"
#include "client/small_bank/small_bank_helper.h"
#include "client/small_bank/small_bank_property.h"

namespace client::small_bank {
    enum class Operation {
        BALANCE,
        DEPOSIT_CHECKING,
        TRANSACT_SAVING,
        AMALGAMATE,
        WRITE_CHECK,
    };

    using SmallBankDiscreteGenerator = core::DiscreteGenerator<Operation>;

    class SmallBankWorkload: public core::Workload {
    public:
        SmallBankWorkload() = default;

        void init(const ::util::Properties& prop) override;

        bool doTransaction(core::DB* db) const override;

        bool doInsert(core::DB*) const override { return false; }

    protected:
        void initOperationGenerator(const SmallBankProperties::Proportion& p);

        bool doBalance(core::DB* db) const;

        bool doDepositChecking(core::DB* db) const;

        bool doTransactSaving(core::DB* db) const;

        bool doAmalgamate(core::DB* db) const;

        bool doWriteCheck(core::DB* db) const;

        [[nodiscard]] inline AccountIDType generateOneAccount() const {
            bool isHotSpot = zeroToOneChooser->nextValue() < hotspotTxnPercentage;
            if (isHotSpot) {
                return (AccountIDType)hotSpotTxnChooser->nextValue();
            }
            return (AccountIDType)nonHotSpotTxnChooser->nextValue();
        }

        [[nodiscard]] inline std::pair<AccountIDType, AccountIDType> generateTwoAccount() const {
            bool isHotSpot = zeroToOneChooser->nextValue() < hotspotTxnPercentage;
            utils::RandomUINT64* chooser;
            if (isHotSpot) {
                chooser = hotSpotTxnChooser.get();
            } else {
                chooser = nonHotSpotTxnChooser.get();
            }
            auto acc1 = (AccountIDType)chooser->nextValue();
            auto acc2 = (AccountIDType)chooser->nextValue();
            while (acc1 == acc2) {
                acc2 = (AccountIDType)chooser->nextValue();
            }
            return std::make_pair(acc1, acc2);
        }

    private:
        int accountsCount{};
        double hotspotTxnPercentage{};
        std::unique_ptr<utils::RandomDouble> zeroToOneChooser;
        std::unique_ptr<SmallBankDiscreteGenerator> operationChooser;
        std::unique_ptr<utils::RandomUINT64> nonHotSpotTxnChooser;
        std::unique_ptr<utils::RandomUINT64> hotSpotTxnChooser;
    };
}