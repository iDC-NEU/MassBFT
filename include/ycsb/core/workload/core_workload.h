//
// Created by peng on 10/18/22.
//

#pragma once

#include <string>
#include <unordered_map>
#include <vector>
#include "ycsb/core/workload/workload.h"
#include "ycsb/core/common/fnv_hash.h"
#include "ycsb/core/common/byte_iterator.h"
#include "ycsb/core/generator/constant_integer_generator.h"
#include "ycsb/core/generator/uniform_long_generator.h"
#include "ycsb/core/generator/counter_generator.h"
#include "ycsb/core/generator/discrete_generator.h"
#include "ycsb/core/generator/exponential_generator.h"
#include "ycsb/core/generator/sequential_generator.h"
#include "ycsb/core/generator/skewed_latest_generator.h"
#include "ycsb/core/generator/hot_spot_inyerger_generator.h"
#include "ycsb/core/generator/acknowledge_counter_generator.h"
#include "ycsb/core/measurements/measurements.h"
#include "ycsb/core/client.h"
#include "ycsb/core/status.h"
#include "ycsb/core/db.h"
#include "ycsb/core/generator/scrambled_zipfian_generator.h"

namespace ycsb::core::workload {
    /**
     * The core benchmark scenario. Represents a set of clients doing simple CRUD operations. The
     * relative proportion of different kinds of operations, and other properties of the workload,
     * are controlled by parameters specified at runtime.
     * <p>
     * Properties to control the client:
     * <UL>
     * <LI><b>fieldcount</b>: the number of fields in a record (default: 10)
     * <LI><b>fieldlength</b>: the size of each field (default: 100)
     * <LI><b>minfieldlength</b>: the minimum size of each field (default: 1)
     * <LI><b>readallfields</b>: should reads read all fields (true) or just one (false) (default: true)
     * <LI><b>writeallfields</b>: should updates and read/modify/writes update all fields (true) or just
     * one (false) (default: false)
     * <LI><b>readproportion</b>: what proportion of operations should be reads (default: 0.95)
     * <LI><b>updateproportion</b>: what proportion of operations should be updates (default: 0.05)
     * <LI><b>insertproportion</b>: what proportion of operations should be inserts (default: 0)
     * <LI><b>scanproportion</b>: what proportion of operations should be scans (default: 0)
     * <LI><b>readmodifywriteproportion</b>: what proportion of operations should be read a record,
     * modify it, write it back (default: 0)
     * <LI><b>requestdistribution</b>: what distribution should be used to select the records to operate
     * on - uniform, zipfian, hotspot, sequential, exponential or latest (default: uniform)
     * <LI><b>minscanlength</b>: for scans, what is the minimum number of records to scan (default: 1)
     * <LI><b>maxscanlength</b>: for scans, what is the maximum number of records to scan (default: 1000)
     * <LI><b>scanlengthdistribution</b>: for scans, what distribution should be used to choose the
     * number of records to scan, for each scan, between 1 and maxscanlength (default: uniform)
     * <LI><b>insertstart</b>: for parallel loads and runs, defines the starting record for this
     * YCSB instance (default: 0)
     * <LI><b>insertcount</b>: for parallel loads and runs, defines the number of records for this
     * YCSB instance (default: recordcount)
     * <LI><b>zeropadding</b>: for generating a record sequence compatible with string sort order by
     * 0 padding the record number. Controls the number of 0s to use for padding. (default: 1)
     * For example for row 5, with zeropadding=1 you get 'user5' key and with zeropading=8 you get
     * 'user00000005' key. In order to see its impact, zeropadding needs to be bigger than number of
     * digits in the record number.
     * <LI><b>insertorder</b>: should records be inserted in order by key ("ordered"), or in hashed
     * order ("hashed") (default: hashed)
     * <LI><b>fieldnameprefix</b>: what should be a prefix for field names, the shorter may decrease the
     * required storage size (default: "field")
     * </ul>
     */
    class CoreWorkload: public Workload {
    public:
        // The name of the database table to run queries against.
        constexpr static const auto TABLENAME_PROPERTY = "table";
        // The default name of the database table to run queries against.
        constexpr static const auto TABLENAME_PROPERTY_DEFAULT = "usertable";

    protected:
        std::string table;

    public:
        // The name of the property for the number of fields in a record.
        constexpr static const auto FIELD_COUNT_PROPERTY = "fieldcount";

        // Default number of fields in a record.
        constexpr static const auto FIELD_COUNT_PROPERTY_DEFAULT = 10;

    private:
        std::vector<std::string> fieldnames;

    public:
        /**
         * The name of the property for the field length distribution. Options are "uniform", "zipfian"
         * (favouring short records), "constant", and "histogram".
         * If "uniform", "zipfian" or "constant", the maximum field length will be that specified by the
         * fieldlength property. If "histogram", then the histogram will be read from the filename
         * specified in the "fieldlengthhistogram" property.
         */
        constexpr static const auto FIELD_LENGTH_DISTRIBUTION_PROPERTY = "fieldlengthdistribution";

        // The default field length distribution.
        constexpr static const auto FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = "constant";

        // The name of the property for the length of a field in bytes.
        constexpr static const auto FIELD_LENGTH_PROPERTY = "fieldlength";

        // The default maximum length of a field in bytes.
        constexpr static const auto FIELD_LENGTH_PROPERTY_DEFAULT = 100;

        // The name of the property for the minimum length of a field in bytes.
        constexpr static const auto MIN_FIELD_LENGTH_PROPERTY = "minfieldlength";

        // The default minimum length of a field in bytes.
        constexpr static const auto MIN_FIELD_LENGTH_PROPERTY_DEFAULT = 1;

        /**
         * The name of a property that specifies the filename containing the field length histogram (only
         * used if fieldlengthdistribution is "histogram").
         */
        constexpr static const auto FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY = "fieldlengthhistogram";

        // The default filename containing a field length histogram.
        constexpr static const auto FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT = "hist.txt";

        /**
         * Generator object that produces field lengths.  The value of this depends on the properties that
         * start with "FIELD_LENGTH_".
         */
    protected:
        std::unique_ptr<NumberGenerator> fieldLengthGenerator;

    public:
        // The name of the property for deciding whether to read one field (false) or all fields (true) of a record.
        constexpr static const auto READ_ALL_FIELDS_PROPERTY = "readallfields";

        // The default value for the readallfields property.
        constexpr static const auto READ_ALL_FIELDS_PROPERTY_DEFAULT = "true";

    protected:
        bool readAllFields;

    public:
        /**
         * The name of the property for determining how to read all the fields when readallfields is true.
         * If set to true, all the field names will be passed into the underlying client. If set to false,
         * null will be passed into the underlying client. When passed a null, some clients may retrieve
         * the entire row with a wildcard, which may be slower than naming all the fields.
         */
        constexpr static const auto READ_ALL_FIELDS_BY_NAME_PROPERTY = "readallfieldsbyname";

        /**
         * The default value for the readallfieldsbyname property.
         */
        constexpr static const auto READ_ALL_FIELDS_BY_NAME_PROPERTY_DEFAULT = false;

    protected:
        bool readAllFieldsByName;

    public:
        // The name of the property for deciding whether to write one field (false) or all fields (true) of a record.
        constexpr static const auto WRITE_ALL_FIELDS_PROPERTY = "writeallfields";

        // The default value for the writeallfields property.
        constexpr static const auto WRITE_ALL_FIELDS_PROPERTY_DEFAULT = "false";

    protected:
        bool writeAllFields;

    public:
        /**
         * The name of the property for deciding whether to check all returned
         * data against the formation template to ensure data integrity.
         */
        constexpr static const auto DATA_INTEGRITY_PROPERTY = "dataintegrity";

        // The default value for the dataintegrity property.
        constexpr static const auto DATA_INTEGRITY_PROPERTY_DEFAULT = "false";

    private:
        /**
         * Set to true if want to check correctness of reads. Must also
         * be set to true during loading phase to function.
         */
        bool dataIntegrity;

    public:
        // The name of the property for the proportion of transactions that are reads.
        constexpr static const auto READ_PROPORTION_PROPERTY = "readproportion";

        // The default proportion of transactions that are reads.
        constexpr static const auto READ_PROPORTION_PROPERTY_DEFAULT = 0.95;

        // The name of the property for the proportion of transactions that are updates.
        constexpr static const auto UPDATE_PROPORTION_PROPERTY = "updateproportion";

        // The default proportion of transactions that are updates.
        constexpr static const auto UPDATE_PROPORTION_PROPERTY_DEFAULT = 0.05;

        // The name of the property for the proportion of transactions that are inserts.
        constexpr static const auto INSERT_PROPORTION_PROPERTY = "insertproportion";

        // The default proportion of transactions that are inserts.
        constexpr static const auto INSERT_PROPORTION_PROPERTY_DEFAULT = 0.0;

        // The name of the property for the proportion of transactions that are scans.
        constexpr static const auto SCAN_PROPORTION_PROPERTY = "scanproportion";

        // The default proportion of transactions that are scans.
        constexpr static const auto SCAN_PROPORTION_PROPERTY_DEFAULT = 0.0;

        // The name of the property for the proportion of transactions that are read-modify-write.
        constexpr static const auto READMODIFYWRITE_PROPORTION_PROPERTY = "readmodifywriteproportion";

        // The default proportion of transactions that are scans.
        constexpr static const auto READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT = 0.0;

        /**
         * The name of the property for the the distribution of requests across the keyspace. Options are
         * "uniform", "zipfian" and "latest"
         */
        constexpr static const auto REQUEST_DISTRIBUTION_PROPERTY = "requestdistribution";

        // The default distribution of requests across the keyspace.
        constexpr static const auto REQUEST_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";

        /**
         * The name of the property for adding zero padding to record numbers in order to match
         * string sort order. Controls the number of 0s to left pad with.
         */
        constexpr static const auto ZERO_PADDING_PROPERTY = "zeropadding";

        // The default zero padding value. Matches integer sort order
        constexpr static const auto ZERO_PADDING_PROPERTY_DEFAULT = 1;

        // The name of the property for the min scan length (number of records).
        constexpr static const auto MIN_SCAN_LENGTH_PROPERTY = "minscanlength";

        // The default min scan length.
        constexpr static const auto MIN_SCAN_LENGTH_PROPERTY_DEFAULT = 1;

        // The name of the property for the max scan length (number of records).
        constexpr static const auto MAX_SCAN_LENGTH_PROPERTY = "maxscanlength";

        // The default max scan length.
        constexpr static const auto MAX_SCAN_LENGTH_PROPERTY_DEFAULT = 1000;

        // The name of the property for the scan length distribution. Options are "uniform" and "zipfian" (favoring short scans)
        constexpr static const auto SCAN_LENGTH_DISTRIBUTION_PROPERTY = "scanlengthdistribution";

        // The default max scan length.
        constexpr static const auto SCAN_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";

        // The name of the property for the order to insert records. Options are "ordered" or "hashed"
        constexpr static const auto INSERT_ORDER_PROPERTY = "insertorder";

        // Default insert order.
        constexpr static const auto INSERT_ORDER_PROPERTY_DEFAULT = "hashed";

        // Percentage data items that constitute the hot set.
        constexpr static const auto HOTSPOT_DATA_FRACTION = "hotspotdatafraction";

        // Default value of the size of the hot set.
        constexpr static const auto HOTSPOT_DATA_FRACTION_DEFAULT = 0.2;

        // Percentage operations that access the hot set.
        constexpr static const auto HOTSPOT_OPN_FRACTION = "hotspotopnfraction";

        // Default value of the percentage operations accessing the hot set.
        constexpr static const auto HOTSPOT_OPN_FRACTION_DEFAULT = 0.8;

        // How many times to retry when insertion of a single item to a DB fails.
        constexpr static const auto INSERTION_RETRY_LIMIT = "core_workload_insertion_retry_limit";
        constexpr static const auto INSERTION_RETRY_LIMIT_DEFAULT = 0;

        // On average, how long to wait between the retries, in seconds.
        constexpr static const auto INSERTION_RETRY_INTERVAL = "core_workload_insertion_retry_interval";
        constexpr static const auto INSERTION_RETRY_INTERVAL_DEFAULT = 3;

        // Field name prefix.
        constexpr static const auto FIELD_NAME_PREFIX = "fieldnameprefix";

        // Default value of the field name prefix.
        constexpr static const auto FIELD_NAME_PREFIX_DEFAULT = "field";

    protected:
        std::unique_ptr<NumberGenerator> keySequence;
        std::unique_ptr<DiscreteGenerator> operationChooser;
        std::unique_ptr<NumberGenerator> keyChooser;
        std::unique_ptr<NumberGenerator> fieldChooser;
        std::unique_ptr<AcknowledgedCounterGenerator> transactionInsertKeySequence;
        std::unique_ptr<NumberGenerator> scanLength;
        bool orderedInserts;
        uint64_t fieldCount, recordCount;
        int zeroPadding, insertionRetryLimit, insertionRetryInterval;

    private:
        // a singleton reference
        Measurements* measurements;

    public:
        static inline std::string buildKeyName(uint64_t keyNum, int zeroPadding, bool orderedInserts) {
            if (!orderedInserts) {
                keyNum = utils::FNVHash64(keyNum);
            }
            auto value = std::to_string(keyNum);
            int fill = zeroPadding - (int)value.size();
            std::string keyPrefix = "user";
            keyPrefix.reserve(fill + keyPrefix.size() + value.size() + 1);
            for (int i = 0; i < fill; i++) {
                keyPrefix.push_back('0');
            }
            return keyPrefix + value;
        }
    protected:
        static std::unique_ptr<NumberGenerator> getFieldLengthGenerator(const YAML::Node& n) noexcept(false) {
            std::unique_ptr<NumberGenerator> fieldLengthGenerator;
            auto fieldLengthDistribution = n[FIELD_LENGTH_DISTRIBUTION_PROPERTY].as<std::string>(FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);
            auto fieldLength = n[FIELD_LENGTH_PROPERTY].as<int>(FIELD_LENGTH_PROPERTY_DEFAULT);
            auto minFieldLength = n[MIN_FIELD_LENGTH_PROPERTY].as<int>(MIN_FIELD_LENGTH_PROPERTY_DEFAULT);
            auto fieldLengthHistogram = n[FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY].as<std::string>(FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT);
            if (fieldLengthDistribution == "constant") {
                fieldLengthGenerator = ConstantIntegerGenerator::NewConstantIntegerGenerator(fieldLength);
            } else if (fieldLengthDistribution == "constant") {
                fieldLengthGenerator = UniformLongGenerator::NewUniformLongGenerator(minFieldLength, fieldLength);
            } else if (fieldLengthDistribution == "zipfian") {
                fieldLengthGenerator = ZipfianGenerator::NewZipfianGenerator(minFieldLength, fieldLength);
            } else if (fieldLengthDistribution == "histogram") {
                // TODO: HistogramGenerator
            } else {
                throw utils::WorkloadException("Unknown field length distribution \"" + fieldLengthDistribution + "\"");
            }
            return fieldLengthGenerator;
        }
    public:
        // A single thread init the workload.
        void init(const YAML::Node& n) override {
            table = n[TABLENAME_PROPERTY].as<std::string>(TABLENAME_PROPERTY_DEFAULT);
            fieldCount = n[FIELD_COUNT_PROPERTY].as<uint64_t>(FIELD_COUNT_PROPERTY_DEFAULT);
            const auto fieldNamePrefix = n[FIELD_NAME_PREFIX].as<std::string>(FIELD_NAME_PREFIX_DEFAULT);
            fieldnames.clear();
            fieldnames.reserve(fieldCount);
            for (uint64_t i = 0; i < fieldCount; i++) {
                fieldnames.push_back(fieldNamePrefix + std::to_string(i));
            }
            fieldLengthGenerator = CoreWorkload::getFieldLengthGenerator(n);
            recordCount = n[Client::RECORD_COUNT_PROPERTY].as<uint64_t>(Client::DEFAULT_RECORD_COUNT);
            if (recordCount == 0) {
                recordCount = UINT64_MAX;
            }
            auto requestDistrib = n[REQUEST_DISTRIBUTION_PROPERTY].as<std::string>(REQUEST_DISTRIBUTION_PROPERTY_DEFAULT);
            auto minScanLength = n[MIN_SCAN_LENGTH_PROPERTY].as<uint64_t>(MIN_SCAN_LENGTH_PROPERTY_DEFAULT);
            auto maxScanLength = n[MAX_SCAN_LENGTH_PROPERTY].as<uint64_t>(MAX_SCAN_LENGTH_PROPERTY_DEFAULT);
            auto scanLengthDistrib = n[SCAN_LENGTH_DISTRIBUTION_PROPERTY].as<std::string>(SCAN_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);
            auto insertStart = n[INSERT_START_PROPERTY].as<uint64_t>(INSERT_START_PROPERTY_DEFAULT);
            auto insertCount = n[INSERT_COUNT_PROPERTY].as<uint64_t>(recordCount - insertStart);
            // Confirm valid values for insertStart and recordCount in relation to recordCount
            CHECK(recordCount >= (insertStart + insertCount))
                            << "Invalid combination of insertstart, insertcount and recordcount."
                            << "recordcount must be bigger than insertstart + insertcount.";
            zeroPadding = n[ZERO_PADDING_PROPERTY].as<int>(ZERO_PADDING_PROPERTY_DEFAULT);
            readAllFields = n[READ_ALL_FIELDS_PROPERTY].as<bool>(READ_ALL_FIELDS_PROPERTY_DEFAULT);
            readAllFieldsByName = n[READ_ALL_FIELDS_BY_NAME_PROPERTY].as<bool>(READ_ALL_FIELDS_BY_NAME_PROPERTY_DEFAULT);
            writeAllFields = n[WRITE_ALL_FIELDS_PROPERTY].as<bool>(WRITE_ALL_FIELDS_PROPERTY_DEFAULT);
            dataIntegrity = n[DATA_INTEGRITY_PROPERTY].as<bool>(DATA_INTEGRITY_PROPERTY_DEFAULT);
            // Confirm that fieldlengthgenerator returns a constant if data integrity check requested.
            if (dataIntegrity && !(n[FIELD_LENGTH_DISTRIBUTION_PROPERTY].as<std::string>(FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT) == "constant")) {
                CHECK(false) << "Must have constant field size to check data integrity.";
            }
            if (dataIntegrity) {
                LOG(INFO) << "Data integrity is enabled.";
            }
            if (n[INSERT_ORDER_PROPERTY].as<std::string>(INSERT_ORDER_PROPERTY_DEFAULT) == "hashed") {
                orderedInserts = false;
            } else {
                orderedInserts = true;
            }
            keySequence = CounterGenerator::NewCounterGenerator(insertStart);
            operationChooser = createOperationGenerator(n);

            transactionInsertKeySequence = AcknowledgedCounterGenerator::NewAcknowledgedCounterGenerator(recordCount);
            if (requestDistrib == "uniform") {
                keyChooser = UniformLongGenerator::NewUniformLongGenerator(insertStart, insertStart + insertCount - 1);
            } else if (requestDistrib == "exponential") {
                auto percentile = n[ExponentialGenerator::EXPONENTIAL_PERCENTILE_PROPERTY].as<double>(ExponentialGenerator::EXPONENTIAL_PERCENTILE_DEFAULT);
                auto frac = n[ExponentialGenerator::EXPONENTIAL_FRAC_PROPERTY].as<double>(ExponentialGenerator::EXPONENTIAL_FRAC_DEFAULT);
                keyChooser = ExponentialGenerator::NewExponentialGenerator(percentile, (double)recordCount * frac);
            } else if (requestDistrib == "sequential") {
                keyChooser = SequentialGenerator::NewSequentialGenerator(insertStart, insertStart + insertCount - 1);
            } else if (requestDistrib == "zipfian") {
                // it does this by generating a random "next key" in part by taking the modulus over the number of keys.
                // If the number of keys changes, this would shift the modulus, and we don't want that to
                // change which keys are popular, so we'll actually construct the scrambled zipfian generator
                // with a keyspace that is larger than exists at the beginning of the test. that is, we'll predict
                // the number of inserts, and tell the scrambled zipfian generator the number of existing keys
                // plus the number of predicted keys as the total keyspace. then, if the generator picks a key
                // that hasn't been inserted yet, will just ignore it and pick another key. this way, the size of
                // the keyspace doesn't change from the perspective of the scrambled zipfian generator
                const auto insertProportion = n[INSERT_PROPORTION_PROPERTY].as<double>(INSERT_PROPORTION_PROPERTY_DEFAULT);
                auto opCount = n[Client::OPERATION_COUNT_PROPERTY].as<int>();
                auto expectedNewKeys = (int) ((opCount) * insertProportion * 2.0); // 2 is fudge factor
                keyChooser = ScrambledZipfianGenerator::NewScrambledZipfianGenerator(insertStart, insertStart + insertCount + expectedNewKeys);
            } else if (requestDistrib == "latest") {
                keyChooser = SkewedLatestGenerator::NewSkewedLatestGenerator(transactionInsertKeySequence.get());
            } else if (requestDistrib == "hotspot") {
                auto hotSetFraction = n[HOTSPOT_DATA_FRACTION].as<double>(HOTSPOT_DATA_FRACTION_DEFAULT);
                auto hotOpnFraction = n[HOTSPOT_OPN_FRACTION].as<double>(HOTSPOT_OPN_FRACTION_DEFAULT);
                keyChooser = HotspotIntegerGenerator::NewHotspotIntegerGenerator(insertStart, insertStart + insertCount - 1,
                                                        hotSetFraction, hotOpnFraction);
            } else {
                throw utils::WorkloadException("Unknown request distribution \"" + requestDistrib + "\"");
            }
            fieldChooser = UniformLongGenerator::NewUniformLongGenerator(0, fieldCount - 1);
            if (scanLengthDistrib == "uniform") {
                scanLength = UniformLongGenerator::NewUniformLongGenerator(minScanLength, maxScanLength);
            } else if (scanLengthDistrib == "zipfian") {
                scanLength = ZipfianGenerator::NewZipfianGenerator(minScanLength, maxScanLength);
            } else {
                throw utils::WorkloadException("Distribution \"" + scanLengthDistrib + "\" not allowed for scan length");
            }
            insertionRetryLimit = n[INSERTION_RETRY_LIMIT].as<int>(INSERTION_RETRY_LIMIT_DEFAULT);
            insertionRetryInterval = n[INSERTION_RETRY_INTERVAL].as<int>(INSERTION_RETRY_INTERVAL_DEFAULT);
        }
    private:
        void buildSingleValue(utils::ByteIteratorMap& value, const std::string& key) {
            const auto& fieldKey = fieldnames[fieldChooser->nextValue()];
            std::unique_ptr<utils::ByteIterator> data;
            if (dataIntegrity) {
                data = std::make_unique<utils::StringByteIterator>(buildDeterministicValue(key, fieldKey));
            } else {
                // fill with random data
                data = std::make_unique<utils::RandomByteIterator>(fieldLengthGenerator->nextValue());
            }
            value[fieldKey].swap(data);
        }

        /**
         * Builds values for all fields.
         */
        void buildValues(utils::ByteIteratorMap& values, const std::string& key) {
            for (const auto& fieldKey : fieldnames) {
                std::unique_ptr<utils::ByteIterator> data;
                if (dataIntegrity) {
                    data = std::make_unique<utils::StringByteIterator>(buildDeterministicValue(key, fieldKey));
                } else {
                    // fill with random data
                    data =  std::make_unique<utils::RandomByteIterator>(fieldLengthGenerator->nextValue());
                }
                values[fieldKey].swap(data);
            }
        }

    private:
        std::string buildDeterministicValue(const std::string& key, const std::string& fieldKey) {
            auto size = fieldLengthGenerator->nextValue();
            std::string sb;
            sb.reserve(size);
            sb.append(key);
            sb.append(":");
            sb.append(fieldKey);
            while (sb.length() < size) {
                sb.append(":");
                sb.append(std::to_string(std::hash<std::string>{}(sb)));
            }
            sb.resize(size);
            return sb;
        }
    public:
        /**
         * Do one insert operation. Because it will be called concurrently from multiple client threads,
         * this function must be thread safe. However, avoid synchronized, or the threads will block waiting
         * for each other, and it will be difficult to reach the target throughput. Ideally, this function would
         * have no side effects other than DB operations.
         */
        bool doInsert(DB* db, void* threadState) override {
            auto keyNum = keySequence->nextValue();
            auto dbKey = CoreWorkload::buildKeyName(keyNum, zeroPadding, orderedInserts);
            utils::ByteIteratorMap values;
            buildValues(values, dbKey);

            int numOfRetries = 0;
            while (true) {
                auto status = db->insert(table, dbKey, values);
                if (status.isOk()) {
                    return true;
                }
                // Retry if configured. Without retrying, the load process will fail
                // even if one single insertion fails. User can optionally configure
                // an insertion retry limit (default is 0) to enable retry.
                if (++numOfRetries <= insertionRetryLimit) {
                    LOG(ERROR) << "Retrying insertion, retry count: " << numOfRetries;
                    // Sleep for a random number between [0.8, 1.2)*insertionRetryInterval.
                    int sleepTime = (int) (1000 * insertionRetryInterval * (0.8 + 0.4));
                    sleep(sleepTime);

                } else {
                    LOG(ERROR) << "Error inserting, not retrying any more. number of attempts: " << numOfRetries << "Insertion Retry Limit: " << insertionRetryLimit;
                    return false;
                }
            }
        }

        /**
         * Do one transaction operation. Because it will be called concurrently from multiple client
         * threads, this function must be thread safe. However, avoid synchronized, or the threads will block waiting
         * for each other, and it will be difficult to reach the target throughput. Ideally, this function would
         * have no side effects other than DB operations.
         */
        bool doTransaction(DB* db, void* threadState) override {
            auto operation = operationChooser->nextValue();
            switch (operation) {
                case Operation::READ:
                    doTransactionRead(db);
                    break;
                case Operation::UPDATE:
                    doTransactionUpdate(db);
                    break;
                case Operation::INSERT:
                    doTransactionInsert(db);
                    break;
                case Operation::SCAN:
                    doTransactionScan(db);
                    break;
                default:
                    doTransactionReadModifyWrite(db);
            }
            return true;
        }

    protected:
        /**
         * Results are reported in the first three buckets of the histogram under
         * the label "VERIFY".
         * Bucket 0 means the expected data was returned.
         * Bucket 1 means incorrect data was returned.
         * Bucket 2 means null data was returned when some data was expected.
         */
        void verifyRow(const std::string& key, utils::ByteIteratorMap& cells) {
            Status verifyStatus = STATUS_OK;
            auto startTime = util::Timer::time_now_ns();
            if (!cells.empty()) {
                for (auto& entry : cells) {
                    if (entry.second->toString() != buildDeterministicValue(key, entry.first)) {
                        verifyStatus = UNEXPECTED_STATE;
                        break;
                    }
                }
            } else {
                // This assumes that null data is never valid
                verifyStatus = ERROR;
            }
            auto endTime = util::Timer::time_now_ns();
            measurements->measure("VERIFY", (int) (endTime - startTime) / 1000);
            measurements->reportStatus("VERIFY", verifyStatus);
        }
        uint64_t nextKeyNum() {
            uint64_t keyNum;
            if (dynamic_cast<ExponentialGenerator*>(keyChooser.get())!= nullptr) {
                uint64_t a, b;
                do {
                    a = transactionInsertKeySequence->lastValue();
                    b = keyChooser->nextValue();
                } while (a < b);
                keyNum = a - b;
            } else {
                do {
                    keyNum = keyChooser->nextValue();
                } while (keyNum > transactionInsertKeySequence->lastValue());
            }
            return keyNum;
        }
    public:

        void doTransactionRead(DB* db) {
            // choose a random key
            auto keyNum = nextKeyNum();

            auto keyName = CoreWorkload::buildKeyName(keyNum, zeroPadding, orderedInserts);

            std::vector<std::string> fields;

            if (!readAllFields) {
                // read a random field
                const auto& fieldName = fieldnames[fieldChooser->nextValue()];
                fields.push_back(fieldName);
            } else if (dataIntegrity || readAllFieldsByName) {
                // pass the full field list if dataIntegrity is on for verification
                fields = fieldnames;
            }

            utils::ByteIteratorMap cells;
            db->read(table, keyName, fields, cells);

            if (dataIntegrity) {
                verifyRow(keyName, cells);
            }
        }

        void doTransactionReadModifyWrite(DB* db) {
            // choose a random key
            auto keyNum = nextKeyNum();

            auto keyName = CoreWorkload::buildKeyName(keyNum, zeroPadding, orderedInserts);

            std::vector<std::string> fields;

            if (!readAllFields) {
                // read a random field
                const auto& fieldName = fieldnames[fieldChooser->nextValue()];
                fields.push_back(fieldName);
            }

            utils::ByteIteratorMap values;

            if (writeAllFields) {
                // new data for all the fields
                buildValues(values, keyName);
            } else {
                // update a random field
                buildSingleValue(values, keyName);
            }

            // do the transaction
            utils::ByteIteratorMap cells;

            auto ist = measurements->getIntendedStartTimeNs();
            auto st  = util::Timer::time_now_ns();
            db->read(table, keyName, fields, cells);

            db->update(table, keyName, values);

            auto en  = util::Timer::time_now_ns();

            if (dataIntegrity) {
                verifyRow(keyName, cells);
            }

            measurements->measure("READ-MODIFY-WRITE", (int) ((en - st) / 1000));
            measurements->measureIntended("READ-MODIFY-WRITE", (int) ((en - ist) / 1000));
        }

        void doTransactionScan(DB* db) {
            // choose a random key
            auto keyNum = nextKeyNum();
            auto startKeyName = CoreWorkload::buildKeyName(keyNum, zeroPadding, orderedInserts);

            // choose a random scan length
            auto len = scanLength->nextValue();

            std::vector<std::string> fields;

            if (!readAllFields) {
                // read a random field
                const auto& fieldName = fieldnames[fieldChooser->nextValue()];
                fields.push_back(fieldName);
            }
            std::vector<utils::ByteIteratorMap> result;
            db->scan(table, startKeyName, len, fields, result);
        }

        void doTransactionUpdate(DB* db) {
            // choose a random key
            auto keyNum = nextKeyNum();
            auto keyName = CoreWorkload::buildKeyName(keyNum, zeroPadding, orderedInserts);
            utils::ByteIteratorMap values;

            if (writeAllFields) {
                // new data for all the fields
                buildValues(values, keyName);
            } else {
                // update a random field
                buildSingleValue(values, keyName);
            }
            db->update(table, keyName, values);
        }


        void doTransactionInsert(DB* db) {
            // choose the next key
            auto keyNum = transactionInsertKeySequence->nextValue();

            try {
                auto dbKey = CoreWorkload::buildKeyName(keyNum, zeroPadding, orderedInserts);
                utils::ByteIteratorMap values;
                buildValues(values, dbKey);
                db->insert(table, dbKey, values);
            } catch (const std::exception& e) {
                LOG(ERROR) << e.what();
            }
            transactionInsertKeySequence->acknowledge((int)keyNum);
        }


    protected:
        /**
         * Creates a weighted discrete values with database operations for a workload to perform.
         * Weights/proportions are read from the properties list and defaults are used
         * when values are not configured.
         * Current operations are "READ", "UPDATE", "INSERT", "SCAN" and "READMODIFYWRITE".
         *
         * @param p The properties list to pull weights from.
         * @return A generator that can be used to determine the next operation to perform.
         * @throws IllegalArgumentException if the properties object was null.
         */
        static std::unique_ptr<DiscreteGenerator> createOperationGenerator(const YAML::Node& n) {
            auto readProportion = n[READ_PROPORTION_PROPERTY].as<double>(READ_PROPORTION_PROPERTY_DEFAULT);
            auto updateProportion =  n[UPDATE_PROPORTION_PROPERTY].as<double>(UPDATE_PROPORTION_PROPERTY_DEFAULT);
            auto insertProportion =  n[INSERT_PROPORTION_PROPERTY].as<double>(INSERT_PROPORTION_PROPERTY_DEFAULT);
            auto scanProportion =  n[SCAN_PROPORTION_PROPERTY].as<double>(SCAN_PROPORTION_PROPERTY_DEFAULT);
            auto readModifyWriteProportion =  n[READMODIFYWRITE_PROPORTION_PROPERTY].as<double>(READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT);

            auto operationChooser = std::make_unique<DiscreteGenerator>();
            if (readProportion > 0) {
                operationChooser->addValue(readProportion, Operation::READ);
            }

            if (updateProportion > 0) {
                operationChooser->addValue(updateProportion, Operation::UPDATE);
            }

            if (insertProportion > 0) {
                operationChooser->addValue(insertProportion, Operation::INSERT);
            }

            if (scanProportion > 0) {
                operationChooser->addValue(scanProportion, Operation::SCAN);
            }

            if (readModifyWriteProportion > 0) {
                operationChooser->addValue(readModifyWriteProportion, Operation::READ_MODIFY_WRITE);
            }
            return operationChooser;
        }
    };
}
