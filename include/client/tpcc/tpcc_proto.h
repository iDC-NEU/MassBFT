//
// Created by user on 23-8-10.
//

#include "zpp_bits.h"
#include "client/tpcc/tpcc_types.h"

namespace client::tpcc::proto {
    struct NewOrder {
        explicit NewOrder(size_t size)
                : orderLineCount(static_cast<Integer>(size))
                , orderLineNumbers(size)
                , supplierWarehouse(size)
                , itemIds(size)
                , quantities(size) { }

        Integer warehouseId{};
        Integer districtId{};
        Integer customerId{};
        // O_OL_CNT
        Integer orderLineCount;
        std::vector<Integer> orderLineNumbers;  // OL_I_ID
        std::vector<Integer> supplierWarehouse; // OL_SUPPLY_W_ID
        std::vector<Integer> itemIds;           // I_ID
        // Quantities, indicating the quantity of each item in the order
        std::vector<Integer> quantities;    // OL_QUANTITY
        Timestamp timestamp{};

        friend zpp::bits::access;

        constexpr static auto serialize(auto &archive, auto &b) {
            return archive(b.warehouseId, b.districtId, b.customerId, b.orderLineCount,
                           b.orderLineNumbers, b.supplierWarehouse, b.itemIds, b.quantities, b.timestamp);
        }
    };

    struct Payment {
        Integer warehouseId{};
        Integer districtId{};
        // c_w_id
        Integer customerWarehouseId{};
        // c_d_id
        Integer customerDistrictId{};
        // h_amount
        Numeric homeOrderTotalAmount{};
        // h_date
        Timestamp homeOrderTotalDate{};
        Timestamp timestamp{};
        bool isPaymentById{};
        Varchar<16> customerLastName;
        Integer customerId{};

        friend zpp::bits::access;

        constexpr static auto serialize(auto &archive, auto &b) {
            return archive(b.warehouseId, b.districtId, b.customerWarehouseId, b.customerDistrictId,
                           b.homeOrderTotalAmount, b.homeOrderTotalDate, b.timestamp, b.isPaymentById,
                           b.customerLastName, b.customerId);
        }
    };
}