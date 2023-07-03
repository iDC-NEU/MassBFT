//
// Created by peng on 10/18/22.
//

#pragma once

#include "ycsb/core/workload/workload.h"
#include "ycsb/core/client_thread.h"
#include "ycsb/core/measurements.h"
#include "ycsb/core/db.h"
#include "ycsb/core/status.h"
#include "ycsb/core/generator/constant_integer_generator.h"
#include "ycsb/core/generator/uniform_long_generator.h"
#include "ycsb/core/generator/counter_generator.h"
#include "ycsb/core/generator/discrete_generator.h"
#include "ycsb/core/generator/exponential_generator.h"
#include "ycsb/core/generator/sequential_generator.h"
#include "ycsb/core/generator/skewed_latest_generator.h"
#include "ycsb/core/generator/hot_spot_interger_generator.h"
#include "ycsb/core/generator/acknowledge_counter_generator.h"
#include "ycsb/core/generator/scrambled_zipfian_generator.h"
#include "ycsb/core/common/fnv_hash.h"
#include "ycsb/core/common/byte_iterator.h"

namespace ycsb::core::workload {
    class CoreWorkload: public Workload {
        std::unique_ptr<NumberGenerator> keySequence;
        std::unique_ptr<DiscreteGenerator> operationChooser;
        std::unique_ptr<NumberGenerator> keyChooser;
        std::unique_ptr<NumberGenerator> fieldChooser;
        std::unique_ptr<AcknowledgedCounterGenerator> transactionInsertKeySequence;
        std::unique_ptr<NumberGenerator> scanLength;
        int insertStart, insertCount;
        bool orderedInserts, dataIntegrity;
        uint64_t fieldCount, recordCount;
        int zeroPadding, insertionRetryLimit, insertionRetryInterval;
        bool readAllFields, readAllFieldsByName, writeAllFields;

        // Generator object that produces field lengths.  The value of this depends on the properties that start with "FIELD_LENGTH_".
        std::unique_ptr<NumberGenerator> fieldLengthGenerator;

        std::string tableName;
        std::vector<std::string> fieldnames;

        mutable std::shared_ptr<Measurements> measurements;

    public:
        static inline std::string buildKeyName(uint64_t keyNum, int zeroPadding, bool orderedInserts) {
            if (!orderedInserts) {
                keyNum = utils::FNVHash64(keyNum);
            }
            auto value = std::to_string(keyNum);
            int fill = zeroPadding - (int)value.size();
            fill = fill < 0 ? 0 : fill;
            std::string keyPrefix = "user";
            keyPrefix.reserve(keyPrefix.size() + fill + value.size());
            for (int i = 0; i < fill; i++) {
                keyPrefix.push_back('0');
            }
            return keyPrefix + value;
        }

        static std::unique_ptr<NumberGenerator> getFieldLengthGenerator(const utils::YCSBProperties& n) {
            auto disName = n.getFieldLengthDistribution();
            auto fl = n.getFieldLength();
            auto mfl = n.getMinFieldLength();
            if (disName == "constant") {
                return ConstantIntegerGenerator::NewConstantIntegerGenerator(fl);
            } else if (disName == "uniform") {
                return UniformLongGenerator::NewUniformLongGenerator(mfl, fl);
            } else if (disName == "zipfian") {
                return ZipfianGenerator::NewZipfianGenerator(mfl, fl);
            }
            return nullptr;
        }

        std::unique_ptr<Generator<uint64_t>> initKeyChooser(const utils::YCSBProperties& n) const {
            auto requestDistrib = n.getRequestDistrib();
            if (requestDistrib == "uniform") {
                return UniformLongGenerator::NewUniformLongGenerator(insertStart, insertStart + insertCount - 1);
            }
            if (requestDistrib == "exponential") {
                auto [percentile, frac] = n.getExponentialArgs();
                return ExponentialGenerator::NewExponentialGenerator(percentile, (double)recordCount * frac);
            }
            if (requestDistrib == "sequential") {
                return SequentialGenerator::NewSequentialGenerator(insertStart, insertStart + insertCount - 1);
            }
            if (requestDistrib == "zipfian") {
                // it does this by generating a random "next key" in part by taking the modulus over the number of keys.
                // If the number of keys changes, this would shift the modulus, and we don't want that to
                // change which keys are popular, so we'll actually construct the scrambled zipfian generator
                // with a keyspace that is larger than exists at the beginning of the test. that is, we'll predict
                // the number of inserts, and tell the scrambled zipfian generator the number of existing keys
                // plus the number of predicted keys as the total keyspace. then, if the generator picks a key
                // that hasn't been inserted yet, will just ignore it and pick another key. this way, the size of
                // the keyspace doesn't change from the perspective of the scrambled zipfian generator
                const auto insertProportion = n.getProportion().insertProportion;
                auto opCount = n.getOperationCount();
                auto expectedNewKeys =  (int)((double)opCount * insertProportion * 2.0); // 2 is fudge factor
                return ScrambledZipfianGenerator::NewScrambledZipfianGenerator(insertStart, insertStart + insertCount + expectedNewKeys);
            }
            if (requestDistrib == "latest") {
                return SkewedLatestGenerator::NewSkewedLatestGenerator(transactionInsertKeySequence.get());
            }
            if (requestDistrib == "hotspot") {
                auto [hotSetFraction, hotOpnFraction] = n.getHotspotArgs();
                return HotspotIntegerGenerator::NewHotspotIntegerGenerator(insertStart, insertStart + insertCount - 1, hotSetFraction, hotOpnFraction);
            }
            return nullptr;
        }

        static std::unique_ptr<NumberGenerator> initScanLength(const utils::YCSBProperties& n) {
            auto distrib = n.getScanLengthDistrib();
            auto [min, max] = n.getScanLength();
            if (distrib == "uniform") {
                return UniformLongGenerator::NewUniformLongGenerator(min, max);
            }
            if (distrib == "zipfian") {
                return ZipfianGenerator::NewZipfianGenerator(min, max);
            }
            return nullptr;
        }

        static std::unique_ptr<DiscreteGenerator> createOperationGenerator(const utils::YCSBProperties::Proportion& p) {
            auto operationChooser = std::make_unique<DiscreteGenerator>();
            if (p.readProportion > 0) {
                operationChooser->addValue(p.readProportion, Operation::READ);
            }
            if (p.updateProportion > 0) {
                operationChooser->addValue(p.updateProportion, Operation::UPDATE);
            }
            if (p.insertProportion > 0) {
                operationChooser->addValue(p.insertProportion, Operation::INSERT);
            }
            if (p.scanProportion > 0) {
                operationChooser->addValue(p.scanProportion, Operation::SCAN);
            }
            if (p.readModifyWriteProportion > 0) {
                operationChooser->addValue(p.readModifyWriteProportion, Operation::READ_MODIFY_WRITE);
            }
            return operationChooser;
        }

        // A single thread init the workload.
        void init(const utils::YCSBProperties& n) override {
            fieldLengthGenerator = CoreWorkload::getFieldLengthGenerator(n);
            if (fieldLengthGenerator == nullptr) {
                CHECK(false) << "create fieldLengthGenerator failed!";
            }
            tableName = n.getTableName();
            fieldnames = n.getFieldNames();
            fieldCount = n.getFieldCount();
            recordCount = n.getRecordCount();
            insertStart = n.getInsertStart();
            insertCount = n.getInsertCount();
            orderedInserts = n.getOrderedInserts();
            dataIntegrity = n.getDataIntegrity();
            keySequence = CounterGenerator::NewCounterGenerator(insertStart);
            operationChooser = createOperationGenerator(n.getProportion());
            transactionInsertKeySequence = AcknowledgedCounterGenerator::NewAcknowledgedCounterGenerator(recordCount);
            keyChooser = initKeyChooser(n);
            if (keyChooser == nullptr) {
                CHECK(false) << "create keyChooser failed!";
            }
            fieldChooser = UniformLongGenerator::NewUniformLongGenerator(0, fieldCount - 1);
            scanLength = initScanLength(n);
            if (scanLength == nullptr) {
                CHECK(false) << "create scanLength failed!";
            }
            zeroPadding = n.getZeroPadding();
            insertionRetryLimit = n.getInsertRetryLimit();
            insertionRetryInterval = n.getInsertRetryInterval();
            auto ret = n.getReadAllFields();
            readAllFields = ret.first;
            readAllFieldsByName = ret.second;
            writeAllFields = n.getWriteAllFields();
        }

        void setMeasurements(auto&& rhs) { measurements = std::forward<decltype(rhs)>(rhs); }

    private:
        void buildSingleValue(utils::ByteIteratorMap& value, const std::string& key) const {
            const auto& fieldKey = fieldnames[fieldChooser->nextValue()];
            if (dataIntegrity) {
                value[fieldKey] = buildDeterministicValue(key, fieldKey);
            } else {
                // fill with random data
                value[fieldKey] = utils::RandomString(fieldLengthGenerator->nextValue());
                // LOG(INFO) << value[fieldKey];
            }
        }

        void buildValues(utils::ByteIteratorMap& values, const std::string& key) const {
            for (const auto& fieldKey : fieldnames) {
                if (dataIntegrity) {
                    values[fieldKey] = buildDeterministicValue(key, fieldKey);
                } else {
                    // fill with random data
                    values[fieldKey] = utils::RandomString(fieldLengthGenerator->nextValue());
                }
            }
        }

        std::string buildDeterministicValue(const std::string& key, const std::string& fieldKey) const {
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
        bool doInsert(DB* db) const override {
            auto keyNum = keySequence->nextValue();
            auto dbKey = CoreWorkload::buildKeyName(keyNum, zeroPadding, orderedInserts);
            utils::ByteIteratorMap values;
            buildValues(values, dbKey);

            int numOfRetries = 0;
            while (true) {
                auto status = db->insert(tableName, dbKey, values);
                if (status.isOk()) {
                    measurements->beginTransaction(status.getDigest(), status.getGenTimeMs());
                    return true;
                }
                // Retry if configured. Without retrying, the load process will fail
                // even if one single insertion fails. User can optionally configure
                // an insertion retry limit (default is 0) to enable retry.
                if (++numOfRetries <= insertionRetryLimit) {
                    LOG(ERROR) << "Retrying insertion, retry count: " << numOfRetries;
                    // TODO: Sleep for a random number between [0.8, 1.2)*insertionRetryInterval.
                    std::this_thread::sleep_for(std::chrono::milliseconds((1000 * insertionRetryInterval)));
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
        bool doTransaction(DB* db) const override {
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
        Status verifyRow(const std::string& key, utils::ByteIteratorMap& cells) const {
            Status verifyStatus = STATUS_OK;
            if (!cells.empty()) {
                for (auto& entry : cells) {
                    if (entry.second != buildDeterministicValue(key, entry.first)) {
                        verifyStatus = UNEXPECTED_STATE;
                        break;
                    }
                }
            } else {
                // This assumes that null data is never valid
                verifyStatus = ERROR;
                LOG(INFO) << "Verify failed!";
            }
            return verifyStatus;
        }

        uint64_t nextKeyNum() const {
            uint64_t keyNum;
            if (dynamic_cast<ExponentialGenerator*>(keyChooser.get()) != nullptr) {
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

        void doTransactionRead(DB* db) const {
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

            auto status = db->read(tableName, keyName, fields);
            measurements->beginTransaction(status.getDigest(), status.getGenTimeMs());
        }

        void doTransactionReadModifyWrite(DB* db) const {
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

            auto status = db->readModifyWrite(tableName, keyName, fields, values);
            measurements->beginTransaction(status.getDigest(), status.getGenTimeMs());
        }

        void doTransactionScan(DB* db) const {
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
            auto status = db->scan(tableName, startKeyName, len, fields);
            measurements->beginTransaction(status.getDigest(), status.getGenTimeMs());
        }

        void doTransactionUpdate(DB* db) const {
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
            auto status = db->update(tableName, keyName, values);
            measurements->beginTransaction(status.getDigest(), status.getGenTimeMs());
        }


        void doTransactionInsert(DB* db) const {
            // choose the next key
            auto keyNum = transactionInsertKeySequence->nextValue();

            try {
                auto dbKey = CoreWorkload::buildKeyName(keyNum, zeroPadding, orderedInserts);
                utils::ByteIteratorMap values;
                buildValues(values, dbKey);
                auto status = db->insert(tableName, dbKey, values);
                measurements->beginTransaction(status.getDigest(), status.getGenTimeMs());
            } catch (const std::exception& e) {
                LOG(ERROR) << e.what();
            }
            transactionInsertKeySequence->acknowledge((int)keyNum);
        }
    };
}
