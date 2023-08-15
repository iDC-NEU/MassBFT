//
// Created by user on 23-8-10.
//

#include "client/tpcc/tpcc_workload.h"
#include "client/tpcc/tpcc_proto.h"
#include "client/core/generator/uniform_long_except_generator.h"
#include "client/core/db.h"
#include "client/core/status.h"
#include "common/timer.h"

namespace client::tpcc {
    using namespace ::client::core;

    void TPCCWorkload::init(const ::util::Properties &prop) {
        helper = std::make_unique<TPCCHelper>();
        auto n = TPCCProperties::NewFromProperty(prop);
        initOperationGenerator(n->getProportion());
        warehouseCount = n->getWarehouseCount();
        paymentLookup = n->enablePaymentLookup();
        if (n->getWarehouseLocality()) {
            auto [beginPartition, endPartition] = TPCCHelper::CalculatePartition(0, 1, warehouseCount);
            // partitionId+1 = warehouseId
            warehouseChooser = std::make_unique<utils::RandomUINT64>(beginPartition+1, endPartition);
        } else {
            warehouseChooser = std::make_unique<utils::RandomUINT64>(0+1, warehouseCount);
        }
        districtIdChooser = std::make_unique<utils::RandomUINT64>(1, TPCCHelper::DISTRICT_COUNT);
        orderLineCountChooser = std::make_unique<utils::RandomUINT64>(5, 15);
        percentChooser = std::make_unique<utils::RandomDouble>(0, 100);
        amountChooser = std::make_unique<utils::RandomDouble>(1, 5000);
    }

    bool TPCCWorkload::doTransaction(DB *db) const {
        auto wareHouseId = warehouseChooser->nextValue();
        auto operation = operationChooser->nextValue();
        switch (operation) {
            case Operation::NEW_ORDER:
                return doNewOrderRand(db, (int)wareHouseId);
            case Operation::PAYMENT:
                return doPaymentRand(db, (int)wareHouseId);
        }
        return false;
    }

    bool TPCCWorkload::doNewOrderRand(DB *db, int warehouseId) const {
        // The number of items in the order (ol_cnt) is randomly selected within [5 ... 15] (an average of 10)
        proto::NewOrder newOrderProto(orderLineCountChooser->nextValue());
        // For any given terminal, the home warehouse number (W_ID) is constant over the whole measurement interval (see Clause 5.5).
        newOrderProto.warehouseId = (Integer)warehouseId;
        // The district number (D_ID) is randomly selected within [1 ... 10]
        newOrderProto.districtId = (Integer)districtIdChooser->nextValue();
        // The non-uniform random customer number (C_ID) is selected using the NURand (1023,1,3000) function
        newOrderProto.customerId = (Integer)helper->getCustomerID();

        // init generators
        UniformLongExceptGenerator remoteWarehouseChooser(1, warehouseCount, warehouseId);
        utils::RandomUINT64 quantityChooser(1, 10);
        for (Integer i = 0; i < newOrderProto.orderLineCount; i++) {
            newOrderProto.orderLineNumbers[i] = i + 1;
            // 1% remote warehouse transaction
            if (percentChooser->nextValue() < 1) {
                newOrderProto.supplierWarehouse[i] = (Integer)remoteWarehouseChooser.nextValue();
            } else {
                newOrderProto.supplierWarehouse[i] = (Integer)warehouseId;
            }
            while(true) {
                auto& itemList = newOrderProto.itemIds;
                auto nextItemId = helper->getItemID();
                if (std::find(itemList.begin(), itemList.begin() + i, nextItemId) == itemList.begin() + i) {
                    newOrderProto.itemIds[i] = nextItemId;
                    break;
                }
                // item already exist in itemList
            }
            newOrderProto.quantities[i] = (Integer)quantityChooser.nextValue();
        }
        newOrderProto.timestamp = util::Timer::time_now_ns();

        std::string data;
        zpp::bits::out out(data);
        if (failure(out(newOrderProto))) {
            return false;
        }
        auto status = db->sendInvokeRequest(InvokeRequestType::TPCC, InvokeRequestType::NEW_ORDER, data);
        measurements->beginTransaction(status.getDigest(), status.getGenTimeMs());
        return true;
    }

    bool TPCCWorkload::doPaymentRand(DB *db, int warehouseId) const {
        proto::Payment paymentProto{};
        paymentProto.warehouseId = (Integer)warehouseId;
        paymentProto.districtId = (Integer)districtIdChooser->nextValue();
        if (percentChooser->nextValue() > 85) {
            UniformLongExceptGenerator customerWarehouseChooser(1, warehouseCount, warehouseId);
            paymentProto.customerWarehouseId = (Integer)customerWarehouseChooser.nextValue();
            paymentProto.customerDistrictId = (Integer)districtIdChooser->nextValue();
        } else {
            paymentProto.customerWarehouseId = (Integer)warehouseId;
            paymentProto.customerDistrictId = paymentProto.districtId;
        }
        paymentProto.homeOrderTotalAmount = amountChooser->nextValue();
        paymentProto.homeOrderTotalDate = util::Timer::time_now_ns();
        paymentProto.timestamp = util::Timer::time_now_ns();

        // The customer is randomly selected 60% of the time by last name (C_W_ID ,
        // C_D_ID, C_LAST) and 40% of the time by number (C_W_ID , C_D_ID , C_ID).
        if (paymentLookup && percentChooser->nextValue() < 60) {
            paymentProto.isPaymentById = false;
            paymentProto.customerLastName = TPCCHelper::GenerateLastName(helper->getNonUniformRandomLastNameForRun());
        } else {
            paymentProto.isPaymentById = true;
            paymentProto.customerId = helper->getCustomerID();
        }
        std::string data;
        zpp::bits::out out(data);
        if (failure(out(paymentProto))) {
            return false;
        }
        auto status = db->sendInvokeRequest(InvokeRequestType::TPCC, InvokeRequestType::PAYMENT, data);
        measurements->beginTransaction(status.getDigest(), status.getGenTimeMs());
        return true;
    }
}

