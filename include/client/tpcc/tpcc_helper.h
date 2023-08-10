//
// Created by user on 23-8-9.
//

#include "client/tpcc/tpcc_types.h"
#include "client/core/generator/uniform_long_generator.h"
#include "client/core/generator/non_uniform_generator.h"

namespace client::tpcc {
    struct TableNamesPrefix {
        inline static constexpr std::string_view WAREHOUSE = "w_";
        inline static const std::string DISTRICT = "d_";
        inline static const std::string CUSTOMER = "c_";
        inline static const std::string CUSTOMER_WDL = "c-wdl_";
        inline static const std::string HISTORY = "h_";
        inline static const std::string NEW_ORDER = "n_";
        inline static const std::string ORDER = "o_";
        inline static const std::string ORDER_WDC = "o-wdc_";
        inline static const std::string ORDER_LINE = "ol_";
        inline static const std::string ITEM = "i_";
        inline static const std::string STOCK = "s_";
    };

    class TPCCHelper {
    public:
        TPCCHelper()
                : zipCodeUL(0, 9999)
                , itemIDNU(8191, 1, ITEMS_COUNT, OL_I_ID_C)
                , customerIDNU(1023, 1, 3000, C_ID_C)
                , cLastRunNU(255, 0, 999, C_LAST_RUN_C)
                , cLastLoadNU(255, 0, 999, C_LAST_LOAD_C) { }

        Varchar<9> randomZipCode() {
            auto id = zipCodeUL.nextValue();
            Varchar<9> result;
            result.append(static_cast<char>('0' + (id / 1000)));
            result.append(static_cast<char>('0' + (id / 100) % 10));
            result.append(static_cast<char>('0' + (id / 10) % 10));
            result.append(static_cast<char>('0' + (id % 10)));
            return result + std::string_view("11111");
        }

        Integer getItemID() { return (Integer)itemIDNU.nextValue(); }

        Integer getCustomerID() { return (Integer)customerIDNU.nextValue(); }

        static Varchar<16> GenerateLastName(int n) {
            const auto &s1 = lastNames[n / 100];
            const auto &s2 = lastNames[n / 10 % 10];
            const auto &s3 = lastNames[n % 10];
            auto name = s1 + s2 + s3;
            if (name.size() > 16) {
                name.resize(16);
            }
            return Varchar<16>(name);
        }

        Integer getNonUniformRandomLastNameForRun() { return (Integer)cLastRunNU.nextValue(); }

        Integer getNonUniformRandomLastNameForLoad() { return (Integer)cLastLoadNU.nextValue(); }

    public:
        static constexpr Integer OL_I_ID_C = 7911;  // in range [0, 8191]
        static constexpr Integer C_ID_C = 259;      // in range [0, 1023]

        static constexpr Integer ITEMS_COUNT = 100000;  // 100K

        static constexpr auto DISTRICT_COUNT = 10;  // 10

        inline static const std::vector<std::string> lastNames = {
                "BAR", "OUGHT", "ABLE",  "PRI",   "PRES",
                "ESE", "ANTI",  "CALLY", "ATION", "EING"
        };

        // NOTE: TPC-C 2.1.6.1 specifies that abs(C_LAST_LOAD_C - C_LAST_RUN_C) must be within [65, 119]
        static constexpr Integer C_LAST_LOAD_C = 157;  // in range [0, 255]
        static constexpr Integer C_LAST_RUN_C = 223;   // in range [0, 255]

        inline static constexpr std::string_view ORIGINAL_STR = "ORIGINAL";

        // nodeId start from 0, return warehouseId-1 = partitionId [begin, end)
        static std::pair<int, int> CalculatePartition(int myId, int nodesCount, int warehouseCount) {
            const uint64_t blockSize = warehouseCount / nodesCount;
            auto begin = myId * blockSize;
            auto end = begin + blockSize;
            if (myId == nodesCount - 1) {
                end = warehouseCount;
            }
            return {begin, end};
        }

    private:
        client::core::UniformLongGenerator zipCodeUL;
        client::core::NonUniformGenerator itemIDNU;
        client::core::NonUniformGenerator customerIDNU;
        client::core::NonUniformGenerator cLastRunNU;
        client::core::NonUniformGenerator cLastLoadNU;
    };
}