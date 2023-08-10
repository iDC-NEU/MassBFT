//
// Created by user on 23-8-10.
//

#include "client/ycsb/core_workload.h"

namespace client::ycsb {
    using namespace ::client::core;

    std::string CoreWorkload::buildKeyName(uint64_t keyNum, int zeroPadding, bool orderedInserts) {
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

    std::unique_ptr<core::NumberGenerator> CoreWorkload::getFieldLengthGenerator(const YCSBProperties &n) {
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

    std::unique_ptr<core::Generator<uint64_t>> CoreWorkload::initKeyChooser(const YCSBProperties &n) const {
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

    std::unique_ptr<core::NumberGenerator> CoreWorkload::initScanLength(const YCSBProperties &n) {
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

    void CoreWorkload::init(const util::Properties &prop) {
        auto n = YCSBProperties::NewFromProperty(prop);
        fieldLengthGenerator = CoreWorkload::getFieldLengthGenerator(*n);
        if (fieldLengthGenerator == nullptr) {
            CHECK(false) << "create fieldLengthGenerator failed!";
        }
        tableName = n->getTableName();
        fieldnames = n->getFieldNames();
        fieldCount = n->getFieldCount();
        recordCount = n->getRecordCount();
        insertStart = n->getInsertStart();
        insertCount = n->getInsertCount();
        orderedInserts = n->getOrderedInserts();
        dataIntegrity = n->getDataIntegrity();
        keySequence = CounterGenerator::NewCounterGenerator(insertStart);
        operationChooser = createOperationGenerator(n->getProportion());
        transactionInsertKeySequence = AcknowledgedCounterGenerator::NewAcknowledgedCounterGenerator(recordCount);
        keyChooser = initKeyChooser(*n);
        if (keyChooser == nullptr) {
            CHECK(false) << "create keyChooser failed!";
        }
        fieldChooser = UniformLongGenerator::NewUniformLongGenerator(0, fieldCount - 1);
        scanLength = initScanLength(*n);
        if (scanLength == nullptr) {
            CHECK(false) << "create scanLength failed!";
        }
        zeroPadding = n->getZeroPadding();
        insertionRetryLimit = n->getInsertRetryLimit();
        insertionRetryInterval = n->getInsertRetryInterval();
        auto ret = n->getReadAllFields();
        readAllFields = ret.first;
        readAllFieldsByName = ret.second;
        writeAllFields = n->getWriteAllFields();
    }

    core::Status CoreWorkload::verifyRow(const std::string &key, utils::ByteIteratorMap &cells) const {
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

    uint64_t CoreWorkload::nextKeyNum() const {
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
}
