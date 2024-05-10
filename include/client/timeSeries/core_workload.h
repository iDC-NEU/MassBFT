//
// Created by peng on 10/18/22.
//

#pragma once

#include "client/core/workload.h"
#include "client/core/client_thread.h"
#include "client/core/status.h"
#include "client/core/generator/constant_integer_generator.h"
#include "client/core/generator/discrete_generator.h"
#include "client/core/generator/exponential_generator.h"
#include "client/core/generator/sequential_generator.h"
#include "client/core/generator/skewed_latest_generator.h"
#include "client/core/generator/hot_spot_interger_generator.h"
#include "client/core/generator/acknowledge_counter_generator.h"
#include "client/core/generator/scrambled_zipfian_generator.h"
#include "client/timeSeries/timeSeries_property.h"
#include "client/timeSeries/timeSeries_db_wrapper.h"

namespace client::timeSeries {
    enum class Operation {
        INSERT,
        READ,
        UPDATE,
        SCAN,
        READ_MODIFY_WRITE,
    };
    using TimeSeriesDiscreteGenerator = core::DiscreteGenerator<Operation>;

    class CoreWorkload: public core::Workload {
        std::unique_ptr<core::NumberGenerator> keySequence;
        std::unique_ptr<TimeSeriesDiscreteGenerator> operationChooser;
        std::unique_ptr<core::NumberGenerator> keyChooser;
        std::unique_ptr<core::NumberGenerator> fieldChooser;
        std::unique_ptr<core::AcknowledgedCounterGenerator> transactionInsertKeySequence;
        std::unique_ptr<core::NumberGenerator> scanLength;
        int insertStart, insertCount;
        bool orderedInserts, dataIntegrity;
        uint64_t fieldCount, recordCount;
        int zeroPadding, insertionRetryLimit, insertionRetryInterval;
        bool readAllFields, readAllFieldsByName, writeAllFields;

        // Generator object that produces field lengths.  The value of this depends on the properties that start with "FIELD_LENGTH_".
        std::unique_ptr<core::NumberGenerator> fieldLengthGenerator;

        std::string tableName;
        std::vector<std::string> fieldnames;

    public:
        static std::string buildKeyName(uint64_t keyNum, int zeroPadding, bool orderedInserts);

        static std::unique_ptr<core::NumberGenerator> getFieldLengthGenerator(const TimeSeriesProperties& n);

        std::unique_ptr<core::Generator<uint64_t>> initKeyChooser(const TimeSeriesProperties& n) const;

        static std::unique_ptr<core::NumberGenerator> initScanLength(const TimeSeriesProperties& n);

        static std::unique_ptr<TimeSeriesDiscreteGenerator> createOperationGenerator(const TimeSeriesProperties::Proportion& p) {
            auto operationChooser = std::make_unique<TimeSeriesDiscreteGenerator>();
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
        void init(const ::util::Properties& prop) override;

    private:
        void buildSingleValue(utils::ByteIteratorMap& value, const std::string& key) const {
            const auto& fieldKey = fieldnames[fieldChooser->nextValue()];
            if (dataIntegrity) {
                value[fieldKey] = buildDeterministicValue(key, fieldKey);
            } else {
                // fill with random data
                value[fieldKey] = utils::RandomString((int)fieldLengthGenerator->nextValue());
            }
        }

        void buildValues(utils::ByteIteratorMap& values, const std::string& key) const {
            for (const auto& fieldKey : fieldnames) {
                if (dataIntegrity) {
                    values[fieldKey] = buildDeterministicValue(key, fieldKey);
                } else {
                    // fill with random data
                    values[fieldKey] = utils::RandomString((int)fieldLengthGenerator->nextValue());
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
        bool doInsert(core::DB* db) const override {
            auto keyNum = keySequence->nextValue();
            auto dbKey = CoreWorkload::buildKeyName(keyNum, zeroPadding, orderedInserts);
            utils::ByteIteratorMap values;
            buildValues(values, dbKey);

            int numOfRetries = 0;
            while (true) {
                auto status = DBWrapper(db).insert(tableName, dbKey, values);
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
        bool doTransaction(core::DB* db) const override {
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
        core::Status verifyRow(const std::string& key, utils::ByteIteratorMap& cells) const;

        uint64_t nextKeyNum() const;

    public:
        void doTransactionRead(core::DB* db) const {
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

            auto status = DBWrapper(db).read(tableName, keyName, fields);
            measurements->beginTransaction(status.getDigest(), status.getGenTimeMs());
        }

        void doTransactionReadModifyWrite(core::DB* db) const {
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

            auto status = DBWrapper(db).readModifyWrite(tableName, keyName, fields, values);
            measurements->beginTransaction(status.getDigest(), status.getGenTimeMs());
        }

        void doTransactionScan(core::DB* db) const {
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
            auto status = DBWrapper(db).scan(tableName, startKeyName, len, fields);
            measurements->beginTransaction(status.getDigest(), status.getGenTimeMs());
        }

        void doTransactionUpdate(core::DB* db) const {
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
            auto status = DBWrapper(db).update(tableName, keyName, values);
            measurements->beginTransaction(status.getDigest(), status.getGenTimeMs());
        }

        void doTransactionInsert(core::DB* db) const {
            // choose the next key
            auto keyNum = transactionInsertKeySequence->nextValue();

            try {
                auto dbKey = CoreWorkload::buildKeyName(keyNum, zeroPadding, orderedInserts);
                utils::ByteIteratorMap values;
                buildValues(values, dbKey);
                auto status = DBWrapper(db).insert(tableName, dbKey, values);
                measurements->beginTransaction(status.getDigest(), status.getGenTimeMs());
            } catch (const std::exception& e) {
                LOG(ERROR) << e.what();
            }
            transactionInsertKeySequence->acknowledge((int)keyNum);
        }
    };
}
