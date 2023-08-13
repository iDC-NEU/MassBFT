//
// Created by user on 23-8-13.
//

#include "client/small_bank/small_bank_workload.h"
#include "client/core/db.h"
#include "client/core/status.h"

namespace client::small_bank {

    void SmallBankWorkload::init(const ::util::Properties &prop) {
        zeroToOneChooser = std::make_unique<utils::RandomDouble>(0, 1);
        auto n = SmallBankProperties::NewFromProperty(prop);
        initOperationGenerator(n->getProportion());
        accountsCount = n->getAccountsCount();
        hotspotTxnPercentage = n->getProbAccountHotspot();
        int hotSpotSize = n->getHotspotFixedSize();
        if (!n->hotspotUseFixedSize()) {
            hotSpotSize = static_cast<int>(n->getHotspotPercentage() * accountsCount);
        }
        auto ub = std::min(hotSpotSize, accountsCount);
        CHECK(ub > 0) << "hotSpotTxnChooser can not generate two different account value!";
        hotSpotTxnChooser = std::make_unique<utils::RandomUINT64>(0, ub);
        auto lb = std::min(hotSpotSize+1, accountsCount);
        CHECK(lb < accountsCount) << "nonHotSpotTxnChooser can not generate two different account value!";
        nonHotSpotTxnChooser = std::make_unique<utils::RandomUINT64>(std::min(hotSpotSize+1, accountsCount), accountsCount);
    }

    void SmallBankWorkload::initOperationGenerator(const SmallBankProperties::Proportion &p) {
        operationChooser = std::make_unique<SmallBankDiscreteGenerator>();
        if (p.balProportion > 0) {
            operationChooser->addValue(p.balProportion, Operation::BALANCE);
        }
        if (p.dcProportion > 0) {
            operationChooser->addValue(p.dcProportion, Operation::DEPOSIT_CHECKING);
        }
        if (p.tsProportion > 0) {
            operationChooser->addValue(p.tsProportion, Operation::TRANSACT_SAVING);
        }
        if (p.amgProportion > 0) {
            operationChooser->addValue(p.amgProportion, Operation::AMALGAMATE);
        }
        if (p.wcProportion > 0) {
            operationChooser->addValue(p.wcProportion, Operation::WRITE_CHECK);
        }
    }

    bool SmallBankWorkload::doTransaction(core::DB *db) const {
        auto operation = operationChooser->nextValue();
        switch (operation) {
            case Operation::BALANCE :
                return doBalance(db);
            case Operation::DEPOSIT_CHECKING:
                return doDepositChecking(db);
            case Operation::TRANSACT_SAVING:
                return doTransactSaving(db);
            case Operation::AMALGAMATE:
                return doAmalgamate(db);
            case Operation::WRITE_CHECK:
                return doWriteCheck(db);
        }
        return false;
    }

    bool SmallBankWorkload::doBalance(core::DB *db) const {
        AccountIDType acc = generateOneAccount();
        std::string data;
        zpp::bits::out out(data);
        if (failure(out(acc))) {
            return false;
        }
        auto status = db->sendInvokeRequest(InvokeRequestType::SMALL_BANK, InvokeRequestType::BALANCE, data);
        measurements->beginTransaction(status.getDigest(), status.getGenTimeMs());
        return true;
    }

    bool SmallBankWorkload::doDepositChecking(core::DB *db) const {
        AccountIDType acc = generateOneAccount();
        // https://github.com/apavlo/h-store/blob/e49885293bf32dad701cb08a3394719d4f844a64/src/benchmarks/edu/brown/benchmark/smallbank/SmallBankClient.java#L95C40-L95C58
        auto amount = 1.3;

        std::string data;
        zpp::bits::out out(data);
        if (failure(out(acc, amount))) {
            return false;
        }
        auto status = db->sendInvokeRequest(InvokeRequestType::SMALL_BANK, InvokeRequestType::DEPOSIT_CHECKING, data);
        measurements->beginTransaction(status.getDigest(), status.getGenTimeMs());
        return true;
    }

    bool SmallBankWorkload::doTransactSaving(core::DB *db) const {
        AccountIDType acc = generateOneAccount();
        auto amount = 20.20;

        std::string data;
        zpp::bits::out out(data);
        if (failure(out(acc, amount))) {
            return false;
        }
        auto status = db->sendInvokeRequest(InvokeRequestType::SMALL_BANK, InvokeRequestType::TRANSACT_SAVING, data);
        measurements->beginTransaction(status.getDigest(), status.getGenTimeMs());
        return true;
    }

    bool SmallBankWorkload::doAmalgamate(core::DB *db) const {
        auto [acc1, acc2] = generateTwoAccount();
        std::string data;
        zpp::bits::out out(data);
        if (failure(out(acc1, acc2))) {
            return false;
        }
        auto status = db->sendInvokeRequest(InvokeRequestType::SMALL_BANK, InvokeRequestType::AMALGAMATE, data);
        measurements->beginTransaction(status.getDigest(), status.getGenTimeMs());
        return true;
    }

    bool SmallBankWorkload::doWriteCheck(core::DB *db) const {
        AccountIDType acc = generateOneAccount();
        auto amount = 5.0;

        std::string data;
        zpp::bits::out out(data);
        if (failure(out(acc, amount))) {
            return false;
        }
        auto status = db->sendInvokeRequest(InvokeRequestType::SMALL_BANK, InvokeRequestType::WRITE_CHECK, data);
        measurements->beginTransaction(status.getDigest(), status.getGenTimeMs());
        return true;
    }
}