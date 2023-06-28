//
// Created by user on 23-5-24.
//

#pragma once

#include "common/property.h"

#include <thread>

#include "yaml-cpp/yaml.h"
#include "glog/logging.h"

namespace ycsb::utils {
    class YCSBProperties : std::enable_shared_from_this<YCSBProperties> {
    public:
        constexpr static const auto YCSB_PROPERTIES = "ycsb";

        /// ----- For clients -----
        // The target number of operations to perform.
        constexpr static const auto OPERATION_COUNT_PROPERTY = "operationcount";
        /**
         * Indicates how many inserts to do if less than recordcount.
         * Useful for partitioning the load among multiple servers if the client is the bottleneck.
         * Additional workloads should support the "insertstart" property which tells them which record to start at.
         */
        constexpr static const auto INSERT_COUNT_PROPERTY = "insertcount";

        // The number of records to load into the database initially.
        constexpr static const auto RECORD_COUNT_PROPERTY = "recordcount";

        constexpr static const auto THREAD_COUNT_PROPERTY = "threadcount";

        // used to specify the target throughput.
        constexpr static const auto TARGET_PROPERTY = "target_throughput";

        constexpr static const auto LABEL_PROPERTY = "label";
        /// ----- For clients -----

        // The name of the database table to run queries against.
        constexpr static const auto TABLENAME_PROPERTY = "table";

        // The name of the property for the number of fields in a record.
        constexpr static const auto FIELD_COUNT_PROPERTY = "fieldcount";

        /**
         * The name of the property for the field length distribution. Options are "uniform", "zipfian"
         * (favouring short records), "constant", and "histogram".
         * If "uniform", "zipfian" or "constant", the maximum field length will be that specified by the
         * fieldlength property. If "histogram", then the histogram will be read from the filename
         * specified in the "fieldlengthhistogram" property.
         */
        constexpr static const auto FIELD_LENGTH_DISTRIBUTION_PROPERTY = "fieldlengthdistribution";

        // The name of the property for the length of a field in bytes.
        constexpr static const auto FIELD_LENGTH_PROPERTY = "fieldlength";

        // The name of the property for the minimum length of a field in bytes.
        constexpr static const auto MIN_FIELD_LENGTH_PROPERTY = "minfieldlength";

        // The name of the property for deciding whether to read one field (false) or all fields (true) of a record.
        constexpr static const auto READ_ALL_FIELDS_PROPERTY = "readallfields";

        /**
         * The name of the property for determining how to read all the fields when readallfields is true.
         * If set to true, all the field names will be passed into the underlying client. If set to false,
         * null will be passed into the underlying client. When passed a null, some clients may retrieve
         * the entire row with a wildcard, which may be slower than naming all the fields.
         */
        constexpr static const auto READ_ALL_FIELDS_BY_NAME_PROPERTY = "readallfieldsbyname";

        // The name of the property for deciding whether to write one field (false) or all fields (true) of a record.
        constexpr static const auto WRITE_ALL_FIELDS_PROPERTY = "writeallfields";

        /**
         * The name of the property for deciding whether to check all returned
         * data against the formation template to ensure data integrity.
         */
        constexpr static const auto DATA_INTEGRITY_PROPERTY = "dataintegrity";

        // The name of the property for the proportion of transactions that are reads.
        constexpr static const auto READ_PROPORTION_PROPERTY = "readproportion";

        // The name of the property for the proportion of transactions that are updates.
        constexpr static const auto UPDATE_PROPORTION_PROPERTY = "updateproportion";

        // The name of the property for the proportion of transactions that are inserts.
        constexpr static const auto INSERT_PROPORTION_PROPERTY = "insertproportion";

        // The name of the property for the proportion of transactions that are scans.
        constexpr static const auto SCAN_PROPORTION_PROPERTY = "scanproportion";

        // The name of the property for the proportion of transactions that are read-modify-write.
        constexpr static const auto READMODIFYWRITE_PROPORTION_PROPERTY = "readmodifywriteproportion";

        /**
         * The name of the property for the the distribution of requests across the keyspace. Options are
         * "uniform", "zipfian" and "latest"
         */
        constexpr static const auto REQUEST_DISTRIBUTION_PROPERTY = "requestdistribution";

        /**
         * The name of the property for adding zero padding to record numbers in order to match
         * string sort order. Controls the number of 0s to left pad with.
         */
        constexpr static const auto ZERO_PADDING_PROPERTY = "zeropadding";

        // The name of the property for the min scan length (number of records).
        constexpr static const auto MIN_SCAN_LENGTH_PROPERTY = "minscanlength";

        // The name of the property for the max scan length (number of records).
        constexpr static const auto MAX_SCAN_LENGTH_PROPERTY = "maxscanlength";

        // The name of the property for the scan length distribution. Options are "uniform" and "zipfian" (favoring short scans)
        constexpr static const auto SCAN_LENGTH_DISTRIBUTION_PROPERTY = "scanlengthdistribution";

        // The name of the property for the order to insert records. Options are "ordered" or "hashed"
        constexpr static const auto INSERT_ORDER_PROPERTY = "insertorder";

        // Percentage data items that constitute the hot set.
        constexpr static const auto HOTSPOT_DATA_FRACTION = "hotspotdatafraction";
        constexpr static const auto HOTSPOT_DATA_FRACTION_DEFAULT = 0.2;

        // Percentage operations that access the hot set.
        constexpr static const auto HOTSPOT_OPN_FRACTION = "hotspotopnfraction";
        constexpr static const auto HOTSPOT_OPN_FRACTION_DEFAULT = 0.8;

        // How many times to retry when insertion of a single item to a DB fails.
        constexpr static const auto INSERTION_RETRY_LIMIT = "core_workload_insertion_retry_limit";

        // On average, how long to wait between the retries, in seconds.
        constexpr static const auto INSERTION_RETRY_INTERVAL = "core_workload_insertion_retry_interval";

        constexpr static const auto INSERT_START_PROPERTY = "insertstart";

        constexpr static const auto EXPONENTIAL_PERCENTILE_PROPERTY = "exponential.percentile";
        constexpr static const double EXPONENTIAL_PERCENTILE_DEFAULT = 95;
        // What fraction of the dataset should be accessed exponential.percentile of the time?
        constexpr static const auto EXPONENTIAL_FRAC_PROPERTY = "exponential.frac";
        constexpr static const auto EXPONENTIAL_FRAC_DEFAULT = 0.8571428571;  // 1/7

        static std::unique_ptr<YCSBProperties> NewFromProperty(const util::Properties &n) {
            auto ret = std::unique_ptr<YCSBProperties>(new YCSBProperties(n.getCustomPropertiesOrPanic(YCSB_PROPERTIES)));
            return ret;
        }

    public:
        YCSBProperties(const YCSBProperties& rhs) = default;

        YCSBProperties(YCSBProperties&& rhs) noexcept : n(rhs.n) { }

        auto getSharedFromThis() const {
            return shared_from_this();
        }

    public:
        auto getThreadCount() const {
            return n[THREAD_COUNT_PROPERTY].as<int>((int)std::thread::hardware_concurrency());
        }

        double getTargetTPSPerThread() const {
            auto target = n[TARGET_PROPERTY].as<int>(1000);  // testing targetTPS=1000
            auto threadCount = getThreadCount();
            return ((double) target) / threadCount;
        }

        inline auto getTableName() const {
            return n[TABLENAME_PROPERTY].as<std::string>("user_table");
        }

        inline auto getFieldCount() const {
            return n[FIELD_COUNT_PROPERTY].as<uint64_t>(10);
        }

        inline auto getFieldNames() const {
            std::vector<std::string> fieldNames;
            for (uint64_t i = 0; i < getFieldCount(); i++) {
                fieldNames.push_back("field" + std::to_string(i));
            }
            return fieldNames;
        }

        inline auto getFieldLengthDistribution() const {
            return n[FIELD_LENGTH_DISTRIBUTION_PROPERTY].as<std::string>("constant");
        }

        inline auto getFieldLength() const {
            return n[FIELD_LENGTH_PROPERTY].as<int>(100);
        }

        inline auto getMinFieldLength() const {
            return n[MIN_FIELD_LENGTH_PROPERTY].as<int>(1);
        }

        inline auto getRecordCount() const {
            return n[RECORD_COUNT_PROPERTY].as<int>(10000); // 10k for testing
        }

        inline auto getRequestDistrib() const {
            return n[REQUEST_DISTRIBUTION_PROPERTY].as<std::string>("uniform");
        }

        inline auto getScanLengthDistrib() const {
            return n[SCAN_LENGTH_DISTRIBUTION_PROPERTY].as<std::string>("uniform");
        }

        // return [min, max] scan length
        inline auto getScanLength() const {
            auto minScanLength = n[MIN_SCAN_LENGTH_PROPERTY].as<int>(1);
            auto maxScanLength = n[MAX_SCAN_LENGTH_PROPERTY].as<int>(1000);
            return std::make_pair(minScanLength, maxScanLength);
        }

        inline auto getInsertStart() const {
            return n[INSERT_START_PROPERTY].as<int>(0);
        }

        inline auto getInsertCount() const {
            auto insertStart = getInsertStart();
            auto recordCount = getRecordCount();
            auto insertCount = n[INSERT_COUNT_PROPERTY].as<int>(recordCount - insertStart);
            return insertCount;
        }

        inline auto getZeroPadding() const {
            return n[ZERO_PADDING_PROPERTY].as<int>(1);
        }

        inline std::pair<bool, bool> getReadAllFields() const {
            bool readAllFields = n[READ_ALL_FIELDS_PROPERTY].as<bool>(true);
            if (!readAllFields) {
                return {readAllFields, false};
            }
            bool readAllFieldsByName = n[READ_ALL_FIELDS_BY_NAME_PROPERTY].as<bool>(false);
            return {readAllFields, readAllFieldsByName};
        }

        inline auto getWriteAllFields() const {
            return n[WRITE_ALL_FIELDS_PROPERTY].as<bool>(false);
        }

        // check integrity of reads
        inline auto getDataIntegrity() const {
            auto dataIntegrity = n[DATA_INTEGRITY_PROPERTY].as<bool>(false);
            // Confirm that getFieldLengthDistribution returns a constant if data integrity check requested.
            if (dataIntegrity) {
                LOG(INFO) << "Data integrity is enabled.";
                if (!(getFieldLengthDistribution() == "constant")) {
                    CHECK(false) << "Must have constant field size to check data integrity.";
                }
            }
            return dataIntegrity;
        }

        inline auto getInsertOrder() const {
            return n[INSERT_ORDER_PROPERTY].as<std::string>("hashed");
        }

        inline bool getOrderedInserts() const {
            if (getInsertOrder() == "hashed") {
                return false;
            }
            return true;
        }

        struct Proportion {
            double readProportion;
            double updateProportion;
            double insertProportion;
            double scanProportion;
            double readModifyWriteProportion;
        };

        inline Proportion getProportion() const {
            Proportion p{};
            p.readProportion = n[READ_PROPORTION_PROPERTY].as<double>(0.95);
            p.updateProportion =  n[UPDATE_PROPORTION_PROPERTY].as<double>(0.05);
            p.insertProportion =  n[INSERT_PROPORTION_PROPERTY].as<double>(0);
            p.scanProportion =  n[SCAN_PROPORTION_PROPERTY].as<double>(0);
            p.readModifyWriteProportion =  n[READMODIFYWRITE_PROPORTION_PROPERTY].as<double>(0);
            return p;
        }

        inline auto getExponentialArgs() const {
            auto percentile = n[EXPONENTIAL_PERCENTILE_PROPERTY].as<double>(EXPONENTIAL_PERCENTILE_DEFAULT);
            auto frac = n[EXPONENTIAL_FRAC_PROPERTY].as<double>(EXPONENTIAL_FRAC_DEFAULT);
            return std::make_pair(percentile, frac);
        }

        inline auto getOperationCount() const {
            return n[OPERATION_COUNT_PROPERTY].as<uint64_t>(10000); // 10k for default
        }

        inline auto getHotspotArgs() const {
            auto hotSetFraction = n[HOTSPOT_DATA_FRACTION].as<double>(HOTSPOT_DATA_FRACTION_DEFAULT);
            auto hotOpnFraction = n[HOTSPOT_OPN_FRACTION].as<double>(HOTSPOT_OPN_FRACTION_DEFAULT);
            return std::make_pair(hotSetFraction, hotOpnFraction);
        }

        inline auto getInsertRetryLimit() const {
            return n[INSERTION_RETRY_LIMIT].as<int>(0);
        }

        inline auto getInsertRetryInterval() const {
            return n[INSERTION_RETRY_INTERVAL].as<int>(3);  // ms
        }

    protected:
        explicit YCSBProperties(const YAML::Node& node) :n(node) { }

    private:
        YAML::Node n;
    };

}